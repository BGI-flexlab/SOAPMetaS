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
import org.bgi.flexlab.metas.data.structure.reference.ReferenceInfoMatrix;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordInsertSizeFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasCorrectionModelBase;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasCorrectionModelFactory;
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

    private GCBiasCorrectionModelBase gcBiasCorrectionModel;

    COMGProfilingMethod(MetasOptions options){

        super(options);

        this.insertSizeFilter = new MetasSamRecordInsertSizeFilter(options.getInsertSize(), this.referenceInfoMatrix);

        this.profilingAnalysisMode = options.getProfilingAnalysisMode();

        this.doInsRecalibration = options.isDoInsRecalibration();
        this.doGCRecalibration = options.isDoGcBiasRecalibration();

        if (this.doGCRecalibration) {
            LOG.debug("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Do GC recalibration.");
            this.gcBiasCorrectionModel = new GCBiasCorrectionModelFactory(options.getGcBiasCorrectionModelType(),
                    options.getGcBiasCoefficientsFilePath()).getGCBiasCorrectionModel();
        } else {
            LOG.debug("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Skip GC recalibration.");

        }

        this.referenceInfoMatrix = new ReferenceInfoMatrix(options.getReferenceMatrixFilePath(),
                    options.getSpeciesGenomeGCFilePath());
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
             partition: sampleID + clusterName

            After flatMapToPair:
             key: sampleID\tclusterName
             value: Tuple3<raw read count (uncorrected), corrected read count, merged read>
             partition: sampleID + clusterName

            After reduceByKey:
             key: sampleID\tclusterName
             value: Tuple3<raw read count (uncorrected), corrected read count, merged read>
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

    private ProfilingResultRecord profilingResultGenerator(String clusterName, Tuple4<String, Integer, Double, String> result){
        ProfilingResultRecord resultRecord = new ProfilingResultRecord();

        resultRecord.setClusterName(clusterName);
        resultRecord.setReadGroupID(result._1());
        resultRecord.setRawReadCount(result._2());
        resultRecord.setCorrectedReadCount(result._3());
        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.MARKERS)){
            resultRecord.setAbundance(result._3() / this.referenceInfoMatrix.getGeneLength(clusterName));
        } else {
            resultRecord.setAbundance(result._3() / this.referenceInfoMatrix.getSpeciesGenoLen(clusterName));
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
     *                      MetasSamRecords (value) for both single-end and paired-end sequencing mode.
     * @return Iterator of scala.Tuple2<> where the 1st element is reference name, the 2nd element is a
     *     scala.Tuple3<> containing raw read count (uncorrected), corrected read count and merged read
     *     names (form: "read1/1|read1/2|read2/1|read3/2|...|").
     */
    private Iterator<Tuple2<String, Tuple4<String, Integer, Double, String>>> computePEReadCount (Tuple2<String, MetasSamPairRecord> tupleKeyValue){

        ArrayList<Tuple2<String, Tuple4<String, Integer, Double, String>>> readCountTupleList = new ArrayList<>(2);

        String sampleID = StringUtils.split(tupleKeyValue._1, '\t')[0];
        SAMRecord samRecord1 = tupleKeyValue._2.getFirstRecord();
        SAMRecord samRecord2 = tupleKeyValue._2.getSecondRecord();


        if (tupleKeyValue._2.isProperPaired()) {
            LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Proper paired records. rec1: " +
                    samRecord1.getReadName() + " || rec2: " + samRecord2.getReadName());
            readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, samRecord1, samRecord2));
        } else if (tupleKeyValue._2.isPaired()){
            if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES) &&
                    this.isPairedAtSpeciesLevel(samRecord1, samRecord2)) {
                LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Proper paired records in Species " +
                        "level. rec1: " + samRecord1.getReadName() + " || rec2: " + samRecord2.getReadName());
                readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, samRecord1, samRecord2));
            } else {
                if (this.insertSizeFilter.filter(samRecord1)) {
                    LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Discordant paired records1. " +
                            samRecord1.getReadName());
                    readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord1));
                }
                if (this.insertSizeFilter.filter(samRecord2)) {
                    LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Discordant paired records2. " +
                            samRecord2.getReadName());
                    readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord2));
                }
            }
        } else {
            if (this.insertSizeFilter.filter(samRecord1)) {
                LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Unpaired record in PE mode. " +
                        samRecord1.getReadName());
                readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord1));
            }
        }

        return readCountTupleList.iterator();
    }

    private Iterator<Tuple2<String, Tuple4<String, Integer, Double, String>>> computeSEReadCount (Tuple2<String, MetasSamPairRecord> tupleKeyValue){

        ArrayList<Tuple2<String, Tuple4<String, Integer, Double, String>>> readCountTupleList = new ArrayList<>(2);

        String sampleID = StringUtils.split(tupleKeyValue._1, '\t')[0];
        SAMRecord samRecord = tupleKeyValue._2.getFirstRecord();

        LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Record in SE mode. " +
                samRecord.getReadName());
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
        Double correctedReadCount;
        String readNameLine;

        String geneName = record.getReferenceName();

        LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Count Single Record: " +
                record.toString() + " || Reference Gene name: " + geneName);

        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)) {
            clusterName = this.referenceInfoMatrix.getGeneSpeciesName(geneName);
        } else {
            clusterName = geneName;
        }

        if (clusterName == null){
            return new Tuple2<>(null, null);
        }

        if (this.doGCRecalibration) {
            correctedReadCount = this.gcBiasCorrectionModel.correctedCountForSingle(
                    SequenceUtil.calculateGc(record.getReadBases()),
                    this.referenceInfoMatrix.getSpeciesGenoGC(this.referenceInfoMatrix.getGeneSpeciesName(geneName))
            );
        } else {
            correctedReadCount = (double) rawReadCount;
        }

        readNameLine = record.getReadName() + "|";

        return new Tuple2<>(sampleID + "\t" + clusterName, new Tuple4<>(getReadGroupID(record),
                rawReadCount, correctedReadCount, readNameLine));
    }

    private Tuple2<String, Tuple4<String, Integer, Double, String>> pairedCountTupleGenerator(String sampleID,
                                                                                              SAMRecord record1,
                                                                                              SAMRecord record2){
        final String clusterName;
        final Integer rawReadCount = 1;
        final Double correctedReadCount;

        String geneName = record1.getReferenceName();

        LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Count Paired Record : rec1: " +
                record1.toString() + " || rec2: " + record2.toString() + " || Reference Gene name: " + geneName);


        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)){
            clusterName = this.referenceInfoMatrix.getGeneSpeciesName(geneName);
        } else {
            clusterName = geneName;
        }

        if (clusterName == null){
            return new Tuple2<>(null, null);
        }

        if (this.doGCRecalibration) {
            correctedReadCount = this.gcBiasCorrectionModel.correctedCountForPair(
                    SequenceUtil.calculateGc(record1.getReadBases()),
                    SequenceUtil.calculateGc(record2.getReadBases()),
                    this.referenceInfoMatrix.getSpeciesGenoGC(this.referenceInfoMatrix.getGeneSpeciesName(geneName))
            );
        } else {
            correctedReadCount = (double) rawReadCount;
        }

        String readGroupID = getReadGroupID(record1);

        String readNameLine;

        readNameLine = record1.getReadName() + "|" + record2.getReadName() + "|";

        return new Tuple2<>(sampleID + "\t" + clusterName,
                new Tuple4<>(readGroupID, rawReadCount, correctedReadCount, readNameLine)
        );
    }

    private String getReadGroupID(SAMRecord record){

        String rgID = "NORGID";
        SAMReadGroupRecord groupRecord = record.getReadGroup();
        if (groupRecord == null){
            return rgID.intern();
        }
        rgID = groupRecord.getReadGroupId();
        return rgID;
    }

}
