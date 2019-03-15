package org.bgi.flexlab.metas.profiling;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordInsertSizeFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasCorrectionModelBase;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasCorrectionModelFactory;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;

import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;


/**
 * ClassName: COMGProfilingMethod
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public final class COMGProfilingMethod extends ProfilingMethodBase {

    private GCBiasCorrectionModelBase gcBiasCorrectionModel;

    private MetasSamRecordInsertSizeFilter insertSizeFilter;

    private boolean doInsRecalibration;

    public COMGProfilingMethod(MetasOptions options){
        super(options);
        this.gcBiasCorrectionModel = this.gcBiasCorrectionModelFactory.getGCBiasCorrectionModel();

        this.gcBiasCorrectionModelFactory = new GCBiasCorrectionModelFactory(options.getModelName(),
                options.getGcBiasModelCoefficientsFile());

        this.insertSizeFilter = new MetasSamRecordInsertSizeFilter(options.getInsertSize(), this.referenceInformation);

        this.doInsRecalibration = options.isDoInsRecalibration();
    }


    /**
     * TODO: 提供内容控制参数，控制输出结果所包含的内容。
     * TODO: ReadsName 的连接方法尝试使用 jointer 或者 SpringBuilder
     *
     * @param readMetasSamPairRDD 键是对应的 SamPair 的 read name，值 SamPairRecord 聚合了该 read name
     *                            对应的所有有效的 MetasSamRecord 信息。
     * @return
     */
    @Override
    public JavaRDD<ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD){

        if (this.doInsRecalibration && this.sequencingMode.equals(SequencingMode.PAIREND)){
            this.insertSizeFilter.training(readMetasSamPairRDD);
        }

        return readMetasSamPairRDD.flatMapToPair(readSamPairRec -> this.computeReadCount(readSamPairRec._2))
                .filter(tuple -> tuple._2._1() > 0)
                .reduceByKey((a, b) -> new Tuple3<>(a._1()+b._1(), a._2()+b._2(), a._3()+b._3()))
                .map(rawResultPair -> this.profilingResultGenerator(rawResultPair));
    }

    private ProfilingResultRecord profilingResultGenerator(Tuple2<String, Tuple3<Integer, Double, String>> rawResultPair){
        ProfilingResultRecord resultRecord = new ProfilingResultRecord();

        resultRecord.setClusterName(rawResultPair._1);
        resultRecord.setRawReadCount(rawResultPair._2._1());
        resultRecord.setCorrectedReadCount(rawResultPair._2._2());
        resultRecord.setAbundance(rawResultPair._2._2()/this.referenceInformation.getReferenceLength(rawResultPair._1));

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
     * @param samPairRecord The object that store the properly mapped MetasSamRecords for both single-end
     *                      and paired-end sequencing mode.
     * @return Iterator of scala.Tuple2<> where the 1st element is reference name, the 2nd element is a
     *     scala.Tuple3<> containing raw read count (uncorrected), corrected read count and merged read
     *     names (form: "read1/1|read1/2|read2/1|read3/2|...|").
     */
    private Iterator<Tuple2<String, Tuple3<Integer, Double, String>>> computeReadCount (MetasSamPairRecord samPairRecord){

        ArrayList<Tuple2<String, Tuple3<Integer, Double, String>>> readCountTupleList = new ArrayList<>(2);

        MetasSamRecord samRecord1 = samPairRecord.getFirstRecord();
        MetasSamRecord samRecord2 = samPairRecord.getSecondRecord();


        if (samPairRecord.isProperPaired()){

            readCountTupleList.add(this.pairedCountTupleGenerator(samRecord1, samRecord2));

        } else if (samPairRecord.isPairedMode()){
            if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES) &&
                    this.isPairedAtSpeciesLevel(samRecord1, samRecord2)) {
                readCountTupleList.add(this.pairedCountTupleGenerator(samRecord1, samRecord2));
            } else {
                assert this.sequencingMode.equals(SequencingMode.PAIREND);
                if (samRecord1 != null) {
                    if (this.insertSizeFilter.filter(samRecord1)) {
                        readCountTupleList.add(this.pairedCountTupleGenerator(samRecord1, null));
                    }
                }
                if (samRecord2 != null) {
                    if (this.insertSizeFilter.filter(samRecord2)) {
                        readCountTupleList.add(this.pairedCountTupleGenerator(null, samRecord2));
                    }
                }
            }
        } else {
            readCountTupleList.add(this.singleCountTupleGenerator(samRecord1));
        }

        return readCountTupleList.iterator();
    }

    private Boolean isPairedAtSpeciesLevel(MetasSamRecord record1, MetasSamRecord record2){
        return this.referenceInformation.getReferenceSpeciesName(record1.getReferenceName()).equals(
                this.referenceInformation.getReferenceSpeciesName(record2.getReferenceName())
        );
    }

    private Tuple2<String, Tuple3<Integer, Double, String>> singleCountTupleGenerator(MetasSamRecord record){

        String clusterName;
        Integer rawReadCount;
        Double correctedReadCount;
        String readNameLine;


        clusterName = (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES))?
                this.referenceInformation.getReferenceSpeciesName(record.getReferenceName()):
                record.getReferenceName();
        rawReadCount = 1;
        correctedReadCount = this.gcBiasCorrectionModel.correctedCountForSingle(record.getGCContent(),
                this.referenceInformation.getReferenceGCContent(record.getReferenceName()));
        readNameLine = record.getReadName() + "|";

        return new Tuple2<>(clusterName, new Tuple3<>(rawReadCount, correctedReadCount, readNameLine));
    }

    private Tuple2<String, Tuple3<Integer, Double, String>> pairedCountTupleGenerator(MetasSamRecord record1,
                                                                                      MetasSamRecord record2){
        final String clusterName;
        final Integer rawReadCount;
        final Double correctedReadCount;

        final String readName1;
        final String readName2;
        final String readNameLine;

        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)){
            clusterName = this.referenceInformation.getReferenceSpeciesName((record1 != null)? record1.getReferenceName():record2.getReferenceName());
        } else {
            clusterName = (record1 != null)? record1.getReferenceName():record2.getReferenceName();
        }

        rawReadCount = 1;
        correctedReadCount = this.gcBiasCorrectionModel.correctedCountForPair(record1.getGCContent(),
                record2.getGCContent(), this.referenceInformation.getReferenceGCContent(clusterName));

        readName1 = (record1 != null)? record1.getReadName() + "|" : "";
        readName2 = (record2 != null)? record2.getReadName() + "|" : "";
        readNameLine = readName1 + readName2;

        return new Tuple2<>(clusterName, new Tuple3<>(rawReadCount, correctedReadCount, readNameLine));
    }

}
