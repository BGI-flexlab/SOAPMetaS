package org.bgi.flexlab.metas.profiling;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasCorrectionModelBase;
import org.bgi.flexlab.metas.io.profilingio.ProfilingResultRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamPairRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamRecord;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;

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
    private int insDeviation = 0;

    public COMGProfilingMethod(MetasOptions options){
        super(options);
        this.gcBiasCorrectionModel = this.gcBiasCorrectionModelFactory.getGCBiasCorrectionModel();
    }


    /**
     *
     * @param readMetasSamPairRDD
     * @return
     */
    @Override
    public JavaRDD<ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD){
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
        resultRecord.setAbundance(rawResultPair._2._2()/this.referenceInfomation.getReferenceLength(rawResultPair._1));

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
                if (samRecord1 != null) {
                    if (this.satisfyInsertSizeThreshold(samRecord1)) {
                        readCountTupleList.add(this.pairedCountTupleGenerator(samRecord1, null));
                    }
                }
                if (samRecord2 != null) {
                    if (this.satisfyInsertSizeThreshold(samRecord2)) {
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
        return this.referenceInfomation.getReferenceSpeciesName(record1.getReferenceName()).equals(
                this.referenceInfomation.getReferenceSpeciesName(record2.getReferenceName())
        );
    }

    private Tuple2<String, Tuple3<Integer, Double, String>> singleCountTupleGenerator(MetasSamRecord record){

        String clusterName;
        Integer rawReadCount;
        Double correctedReadCount;
        String readNameLine;


        clusterName = (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES))?
                this.referenceInfomation.getReferenceSpeciesName(record.getReferenceName()):
                record.getReferenceName();
        rawReadCount = 1;
        correctedReadCount = this.gcBiasCorrectionModel.correctedCountForSingle(record.getGCContent(),
                this.referenceInfomation.getReferenceGCContent(record.getReferenceName()));
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

        clusterName = (record1 != null)? record1.getReferenceName():record2.getReferenceName();

        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)){
            clusterName = this.referenceInfomation.getReferenceSpeciesName(clusterName);
        }

        rawReadCount = 1;
        correctedReadCount = this.gcBiasCorrectionModel.correctedCountForPair(record1.getGCContent(),
                record2.getGCContent(), this.referenceInfomation.getReferenceGCContent(clusterName));

        readName1 = (record1 != null)? record1.getReadName() + "|" : "";
        readName2 = (record2 != null)? record2.getReadName() + "|" : "";
        readNameLine = readName1 + readName2;

        return new Tuple2<>(clusterName, new Tuple3<>(rawReadCount, correctedReadCount, readNameLine));
    }

    /**
     *
     * @param samRecord The instance of MetasSamRecord in pair-end sequencing mode which is not properly
     *                  mapped as pair.
     * @return true if the mate read is probably mapped to the sequence region outside around reference marker.
     */
    private Boolean satisfyInsertSizeThreshold(MetasSamRecord samRecord){

        if (samRecord.getReadNegativeStrandFlag()){
            return (this.referenceInfomation.getReferenceLength(samRecord.getReferenceName())-samRecord.getAlignmentStart())
                    < (this.insertSize + this.standardReadLength);
        } else {
            return samRecord.getAlignmentStart() < (this.insertSize - samRecord.getReadLength() + this.standardReadLength);
        }
    }

    public void setProfilingParameters(String paraName, Object paraValue){
        switch (paraName){
            case "InsertSize": {
                // mean value of insert size distribution.
                this.insertSize = (int) paraValue;
                break;
            }

            case "InsertSizeSD":{
                // standard deviation of insert size distribution
                this.insDeviation = (int) paraValue;
                break;
            }
        }
    }
}
