package org.bgi.flexlab.metas.profiling;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.reference.ReferenceGeneTable;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordInsertSizeFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasCorrectionModelBase;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasCorrectionModelFactory;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;

import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;
import scala.Tuple4;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;


/**
 * ClassName: COMGProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public final class COMGProfilingMethod extends ProfilingMethodBase {


    private MetasSamRecordInsertSizeFilter insertSizeFilter;

    private ReferenceGeneTable referenceGeneTable;

    private GCBiasCorrectionModelBase gcBiasCorrectionModel;
    private GCBiasCorrectionModelFactory gcBiasCorrectionModelFactory;


    private boolean doInsRecalibration;
    private boolean doGCRecalibration;

    public COMGProfilingMethod(MetasOptions options){
        super(options);
        this.gcBiasCorrectionModel = this.gcBiasCorrectionModelFactory.getGCBiasCorrectionModel();

        this.gcBiasCorrectionModelFactory = new GCBiasCorrectionModelFactory(options.getGcBiasCorrectionModelType(),
                options.getGcBiasCoefficientsFilePath());

        this.insertSizeFilter = new MetasSamRecordInsertSizeFilter(options.getInsertSize(), this.referenceGeneTable);

        this.profilingAnalysisMode = options.getProfilingAnalysisMode();

        this.doInsRecalibration = options.isDoInsRecalibration();
        this.doGCRecalibration = options.isDoGcBiasRecalibration();

        if (this.doGCRecalibration){
            this.referenceGeneTable = new ReferenceGeneTable(options.getReferenceMatrixFilePath(),
                    options.getSpeciesGenomeGCFilePath());
        } else {
            this.referenceGeneTable = new ReferenceGeneTable(options.getReferenceMatrixFilePath());
        }
    }


    /**
     * TODO: 提供内容控制参数，控制输出结果所包含的内容。
     *
     * @param readMetasSamPairRDD 键是对应的 SamPair 的 "sampleID\tread name"，值 SamPairRecord 聚合了该 read name
     *                            对应的所有有效的 MetasSamRecord 信息。
     * @return
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD, Partitioner partitioner){

        if (this.doInsRecalibration && this.sequencingMode.equals(SequencingMode.PAIREND)){
            this.insertSizeFilter.training(readMetasSamPairRDD);
        }

        if (partitioner == null){
            partitioner = new HashPartitioner(200);
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
        return readMetasSamPairRDD.flatMapToPair(tupleKeyValue -> this.computeReadCount(tupleKeyValue))
                .reduceByKey(partitioner, (a, b) -> new Tuple4<>(a._1(), a._2()+b._2(), a._3()+b._3(), a._4()+b._4()))
                .mapToPair(tuple ->new Tuple2<>(StringUtils.split(tuple._1, '\t')[0], this.profilingResultGenerator(tuple)));
    }

    private ProfilingResultRecord profilingResultGenerator(Tuple2<String, Tuple4<String, Integer, Double, String>> rawResultPair){
        ProfilingResultRecord resultRecord = new ProfilingResultRecord();

        resultRecord.setClusterName(rawResultPair._1);
        resultRecord.setReadGroupID(rawResultPair._2._1());
        resultRecord.setRawReadCount(rawResultPair._2._2());
        resultRecord.setCorrectedReadCount(rawResultPair._2._3());
        resultRecord.setAbundance(rawResultPair._2._3()/this.referenceGeneTable.getGeneLength(rawResultPair._1));

        if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
            resultRecord.setReadNameString(rawResultPair._2._4().getBytes(StandardCharsets.UTF_8));
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
    private Iterator<Tuple2<String, Tuple4<String, Integer, Double, String>>> computeReadCount (Tuple2<String, MetasSamPairRecord> tupleKeyValue){

        ArrayList<Tuple2<String, Tuple4<String, Integer, Double, String>>> readCountTupleList = new ArrayList<>(2);

        String sampleID = tupleKeyValue._1.split("\t")[0];
        MetasSamRecord samRecord1 = tupleKeyValue._2.getFirstRecord();
        MetasSamRecord samRecord2 = tupleKeyValue._2.getSecondRecord();


        if (tupleKeyValue._2.isPairedMode()){

            if (tupleKeyValue._2.isProperPaired()) {
                readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, samRecord1, samRecord2));
            } else if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES) &&
                    this.isPairedAtSpeciesLevel(samRecord1, samRecord2)) {
                readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, samRecord1, samRecord2));
            } else {
                assert this.sequencingMode.equals(SequencingMode.PAIREND);
                if (samRecord1 != null) {
                    if (this.insertSizeFilter.filter(samRecord1)) {
                        readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, samRecord1, null));
                    }
                }
                if (samRecord2 != null) {
                    if (this.insertSizeFilter.filter(samRecord2)) {
                        readCountTupleList.add(this.pairedCountTupleGenerator(sampleID, null, samRecord2));
                    }
                }
            }
        } else {
            readCountTupleList.add(this.singleCountTupleGenerator(sampleID, samRecord1));
        }

        return readCountTupleList.iterator();
    }

    private Boolean isPairedAtSpeciesLevel(MetasSamRecord record1, MetasSamRecord record2){
        return this.referenceGeneTable.getGeneSpeciesName(record1.getReferenceName()).equals(
                this.referenceGeneTable.getGeneSpeciesName(record2.getReferenceName())
        );
    }

    private Tuple2<String, Tuple4<String, Integer, Double, String>> singleCountTupleGenerator(String sampleID,
                                                                                      MetasSamRecord record){
        String clusterName;
        Integer rawReadCount;
        Double correctedReadCount;
        String readNameLine;

        clusterName = (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES))?
                this.referenceGeneTable.getGeneSpeciesName(record.getReferenceName()):
                record.getReferenceName();
        rawReadCount = 1;
        if (this.doGCRecalibration) {
            correctedReadCount = this.gcBiasCorrectionModel.correctedCountForSingle(record.getGCContent(),
                    this.referenceGeneTable.getSpeciesGenoGC(record.getReferenceName()));
        } else {
            correctedReadCount = (double) rawReadCount;
        }
        readNameLine = record.getReadName() + "|";

        return new Tuple2<>(sampleID + "\t" + clusterName, new Tuple4<>(record.getReadGroup().getReadGroupId(),
                rawReadCount, correctedReadCount, readNameLine));
    }

    private Tuple2<String, Tuple4<String, Integer, Double, String>> pairedCountTupleGenerator(String sampleID,
                                                                                      MetasSamRecord record1,
                                                                                      MetasSamRecord record2){
        final String clusterName;
        final Integer rawReadCount;
        final Double correctedReadCount;
        String readGroupID = "Unknown";

        String geneName = (record1 != null)? record1.getReferenceName():record2.getReferenceName();

        StringBuilder readNameLine = new StringBuilder();


        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)){
            clusterName = this.referenceGeneTable.getGeneSpeciesName(geneName);
        } else {

            clusterName = geneName;
        }

        rawReadCount = 1;
        if (this.doGCRecalibration) {
            correctedReadCount = this.gcBiasCorrectionModel.correctedCountForPair(record1.getGCContent(),
                    record2.getGCContent(), this.referenceGeneTable.getSpeciesGenoGC(geneName));
        } else {
            correctedReadCount = (double) rawReadCount;
        }
        if (record1 != null){
            readGroupID = record1.getReadGroup().getReadGroupId();
            readNameLine.append(record1.getReadName()).append("|");
        }
        if (record2 != null){
            readGroupID = record2.getReadGroup().getReadGroupId();
            readNameLine.append(record2.getReadName()).append("|");
        }

        return new Tuple2<>(sampleID + "\t" + clusterName, new Tuple4<>(readGroupID, rawReadCount, correctedReadCount, readNameLine.toString()));
    }

}
