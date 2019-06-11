package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingEveResultRecord;
import org.bgi.flexlab.metas.data.structure.reference.ReferenceInfoMatrix;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordInsertSizeFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelBase;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelFactory;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;

import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;


/**
 * ClassName: COMGProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public final class COMGProfilingMethod extends ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(COMGProfilingMethod.class);

    private MetasSamRecordInsertSizeFilter insertSizeFilter;

    private ReferenceInfoMatrix referenceInfoMatrix;

    private boolean doInsRecalibration;
    private boolean doGCRecalibration;

    private GCBiasModelBase gcBiasRecaliModel;

    public COMGProfilingMethod(MetasOptions options){

        super(options);

        this.profilingAnalysisMode = options.getProfilingAnalysisMode();

        this.doInsRecalibration = options.isDoInsRecalibration();
        this.doGCRecalibration = options.isDoGcBiasRecalibration();

        if (this.doGCRecalibration) {
            LOG.debug("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Do GC recalibration.");
            this.gcBiasRecaliModel = new GCBiasModelFactory(options.getGcBiasRecaliModelType(),
                    options.getGcBiasModelInput()).getGCBiasRecaliModel();
            //this.gcBiasRecaliModel.outputCoefficients(options.getProfilingOutputHdfsDir() + "/builtin_model.json");
        } else {
            LOG.debug("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Skip GC recalibration.");

        }
        this.referenceInfoMatrix = new ReferenceInfoMatrix(options.getReferenceMatrixFilePath(),
                    options.getSpeciesGenomeGCFilePath());

        this.insertSizeFilter = new MetasSamRecordInsertSizeFilter(options.getInsertSize(), this.referenceInfoMatrix);

    }


    /**
     * TODO: 提供内容控制参数，控制输出结果所包含的内容。
     *
     * @param readMetasSamPairRDD 键是对应的 SamPair 的 "sampleID\tread name"，值 SamPairRecord 聚合了该 read name
     *                            对应的所有有效的 SAMRecord 信息。
     * @return ProfilingResultRecord RDD that stores reads count results.
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(
            JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD, Partitioner partitioner){

        if (partitioner == null){
            partitioner = new HashPartitioner(200);
        }

        if (this.sequencingMode.equals(SequencingMode.PAIREDEND)){
                if (this.doInsRecalibration){
                this.insertSizeFilter.training(readMetasSamPairRDD);
            }

            /*
            Input:
             key: sampleID\treadName
             value: MetasSamPairRecord
             partition: sampleID + readname

            After flatMapToPair:
             key: sampleID\tclusterName
             value: Tuple4<SMTag, raw read count (unrecalibrated), recalibrated read count, merged read>
             partition: sampleID + readname

            After reduceByKey:
             key: sampleID\tclusterName
             value: Tuple4<SMTag, raw read count (unrecalibrated), recalibrated read count, merged read>
             partition: sampleID + clusterName

            After mapToPair:
             key: sampleID
             value: ProfilingResultRecord
             partition: sampleID + clusterName
             */
            return readMetasSamPairRDD.flatMapToPair(tupleKeyValue -> this.computePEReadCount(tupleKeyValue))
                    .filter(tup -> tup._1 != null)
                    .reduceByKey(partitioner, (a, b) -> new Tuple4<>(a._1(), a._2()+b._2(), a._3()+b._3(), a._4()+b._4()))
                    .mapToPair(tuple ->{
                        String[] keyStr = StringUtils.split(tuple._1, '\t');
                        return new Tuple2<>(
                                keyStr[0],
                                this.profilingResultGenerator(keyStr[1], tuple._2));
                    });

        } else {
            return readMetasSamPairRDD.flatMapToPair(tupleKeyValue -> this.computeSEReadCount(tupleKeyValue))
                    .filter(tup -> tup._1 != null)
                    .reduceByKey(partitioner, (a, b) -> new Tuple4<>(a._1(), a._2()+b._2(), a._3()+b._3(), a._4()+b._4()))
                    .mapToPair(tuple -> {
                        String[] keyStr = StringUtils.split(tuple._1, '\t');
                        return new Tuple2<>(
                                keyStr[0],
                                this.profilingResultGenerator(keyStr[1], tuple._2));
                    });
        }

    }

    /**
     * Generate ProfilingResultRecord instance.
     *
     * @param clusterName String of cluster name. "Cluster" is related to analysis level.
     * @param result Tuple4 of result list. Tuple4< SMTag, raw read count (unrecalibrated), recalibrated read count, merged read>
     * @return
     */
    private ProfilingResultRecord profilingResultGenerator(String clusterName, Tuple4<String, Integer, Double, String> result){

        ProfilingResultRecord resultRecord;

        if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
            resultRecord = new ProfilingEveResultRecord();
        } else {
            resultRecord = new ProfilingResultRecord();
        }

        resultRecord.setClusterName(clusterName);

        resultRecord.setSmTag(result._1());

        resultRecord.setRawReadCount(result._2());
        resultRecord.setrecaliReadCount(result._3());
        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.MARKERS)){
            int geneLen = this.referenceInfoMatrix.getGeneLength(clusterName);
            if (geneLen > 0) {
                resultRecord.setAbundance(result._3() / geneLen);
            } else {
                resultRecord.setAbundance(0.0);
            }
        } else {
            int genoLen = this.referenceInfoMatrix.getSpeciesGenoLen(clusterName);
            if (genoLen > 0) {
                resultRecord.setAbundance(result._3() / genoLen);
            } else {
                resultRecord.setAbundance(0.0);
            }
        }

        if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
            resultRecord.setReadNameString(result._4());
        }
        return resultRecord;
    }

    /**
     * The method is designed for the call function of the lambda expression in flatMapToPair operation of
     * readMetasSamPairRDDD.
     *
     * Note: Assume that paired-end read A-B are mapped to two different reference gene, in
     *     ProfilingAnalysisLevel.MARKER mode, A and B will be treated as two unrelated read, but in
     *     ProfilingAnalysisLevel.SPECIES mode, the two reference genes may belong to the same species.
     *     In later situation, the read count of the species should add 1.
     *
     * @param tupleKeyValue The object that store the sampleID_readName (key) and properly mapped
     *                      SamRecords (value) for both single-end and paired-end sequencing mode.
     * @return Iterator of scala.Tuple2<> where the 1st element is reference name, the 2nd element is a
     *     scala.Tuple4<> containing read SMTag, raw read count (unrecalibrated), recalibrated read count and merged read
     *     names (form: "read1/1|read1/2|read2/1|read3/2|...|").
     */
    private Iterator<Tuple2<String, Tuple4<String, Integer, Double, String>>> computePEReadCount (Tuple2<String, MetasSamPairRecord> tupleKeyValue){

        ArrayList<Tuple2<String, Tuple4<String, Integer, Double, String>>> readCountTupleList = new ArrayList<>(2);

        String sampleID = StringUtils.split(tupleKeyValue._1, '\t')[0];
        SAMRecord samRecord1 = tupleKeyValue._2.getFirstRecord();
        SAMRecord samRecord2 = tupleKeyValue._2.getSecondRecord();

        if (tupleKeyValue._2.isProperPaired()) {
            //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Proper paired records. rec1: " +
            //        samRecord1.getReadName() + " || rec2: " + samRecord2.getReadName());
            readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, samRecord1, samRecord2));
        } else if (tupleKeyValue._2.isPaired()){
            if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES) &&
                    this.isPairedAtSpeciesLevel(samRecord1, samRecord2)) {
                //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Proper paired records in Species " +
                //        "level. rec1: " + samRecord1.getReadName() + " || rec2: " + samRecord2.getReadName());
                readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, samRecord1, samRecord2));
            } else {
                if (this.insertSizeFilter.filter(samRecord1)) {
                    //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Discordant paired records1. " +
                    //        samRecord1.getReadName());
                    readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord1));
                }
                if (this.insertSizeFilter.filter(samRecord2)) {
                    //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Discordant paired records2. " +
                    //        samRecord2.getReadName());
                    readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord2));
                }
            }
        } else {
            if (this.insertSizeFilter.filter(samRecord1)) {
                //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Unpaired record in PE mode. " +
                //        samRecord1.getReadName());
                readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord1));
            }
        }

        return readCountTupleList.iterator();
    }

    private Iterator<Tuple2<String, Tuple4<String, Integer, Double, String>>> computeSEReadCount (Tuple2<String, MetasSamPairRecord> tupleKeyValue){

        ArrayList<Tuple2<String, Tuple4<String, Integer, Double, String>>> readCountTupleList = new ArrayList<>(2);

        String sampleID = StringUtils.split(tupleKeyValue._1, '\t')[0];
        SAMRecord samRecord = tupleKeyValue._2.getFirstRecord();

        //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Record in SE mode. " +
        //        samRecord.getReadName());
        readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord));

        return readCountTupleList.iterator();
    }


    private Boolean isPairedAtSpeciesLevel(SAMRecord record1, SAMRecord record2){
        String name1 = this.referenceInfoMatrix.getGeneSpeciesName(record1.getReferenceName());
        String name2 = this.referenceInfoMatrix.getGeneSpeciesName(record2.getReferenceName());
        if (name1 == null || name2 == null){
            return false;
        }
        return name1.equals(name2);
    }

    private Tuple2<String, Tuple4<String, Integer, Double, String>> singleCountTupleGenerator(String sampleID,
                                                                                              SAMRecord record){
        String clusterName;
        Integer rawReadCount = 1;
        Double recaliReadCount;
        String readNameLine;

        String geneName = record.getReferenceName();

        //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Count Single Record: " +
        //        record.toString() + " || Reference Gene name: " + geneName);

        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)) {
            clusterName = this.referenceInfoMatrix.getGeneSpeciesName(geneName);
        } else {
            clusterName = geneName;
        }

        if (clusterName == null){
            return new Tuple2<>(null, null);
        }

        if (this.doGCRecalibration) {
            recaliReadCount = this.gcBiasRecaliModel.recalibrateForSingle(
                    SequenceUtil.calculateGc(record.getReadBases()),
                    this.referenceInfoMatrix.getSpeciesGenoGC(this.referenceInfoMatrix.getGeneSpeciesName(geneName))
            );
        } else {
            recaliReadCount = (double) rawReadCount;
        }

        readNameLine = record.getReadName() + "|";

        return new Tuple2<>(sampleID + '\t' + clusterName, new Tuple4<>(getSampleTag(record),
                rawReadCount, recaliReadCount, readNameLine));
    }

    private Tuple2<String, Tuple4<String, Integer, Double, String>> pairedCountTupleGenerator(String sampleID,
                                                                                              SAMRecord record1,
                                                                                              SAMRecord record2){
        final String clusterName;
        final Integer rawReadCount = 1;
        final Double recaliReadCount;

        String geneName = record1.getReferenceName();

        //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Count Paired Record : rec1: " +
        //        record1.toString() + " || rec2: " + record2.toString() + " || Reference Gene name: " + geneName);


        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)){
            clusterName = this.referenceInfoMatrix.getGeneSpeciesName(geneName);
        } else {
            clusterName = geneName;
        }

        if (clusterName == null){
            return new Tuple2<>(null, null);
        }

        if (this.doGCRecalibration) {
            recaliReadCount = this.gcBiasRecaliModel.recalibrateForPair(
                    SequenceUtil.calculateGc(record1.getReadBases()),
                    SequenceUtil.calculateGc(record2.getReadBases()),
                    this.referenceInfoMatrix.getSpeciesGenoGC(this.referenceInfoMatrix.getGeneSpeciesName(geneName))
            );
        } else {
            recaliReadCount = (double) rawReadCount;
        }

        String sampleTag = getSampleTag(record1);

        String readNameLine;

        readNameLine = record1.getReadName() + "," + record2.getReadName() + ",";

        return new Tuple2<>(sampleID + '\t' + clusterName,
                new Tuple4<>(sampleTag, rawReadCount, recaliReadCount, readNameLine)
        );
    }

    private String getSampleTag(SAMRecord record){

        String smTag;

        SAMReadGroupRecord groupRecord = record.getReadGroup();

        smTag = groupRecord.getSample();

        if (smTag == null){
            smTag = "NOSMTAG";
        }

        return smTag;
    }

}
