package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDPartitioner;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelBase;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.*;

/**
 * ClassName: MEPHProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class MEPHProfilingMethod extends ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;

    private boolean doGCRecalibration;
    private GCBiasModelBase gcBiasRecaliModel;

    private final HashMap<String, ArrayList<String>>> markers2exts;
    private final HashMap<String, Integer>> markers2len;

    public MEPHProfilingMethod(MetasOptions options, JavaSparkContext jsc){
        super(options, jsc);
    }

    /**
     * TODO: 需要完善基于METAPHLAN策略的丰度计算方法。
     *
     * @param readMetasSamPairRDD
     * @return
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD, Partitioner partitioner){
        return null;
    }

    /*
     Input:

     After mapToPair:
      key: sampleID
      value: HashMap<markerName, tuple<1, gc_recali_value>>
      Partition: default

     After reduceByKey:
      Type:
      key: sampleID
      value: HashMap<markerName, tuple<count, gc_recali_count>>


     Output:
      Type: JavaPairRDD
      key: sampleID
      value: ProfilingResultRecord (clusterName includes taxonomy information)
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaRDD<SAMRecord> samRecordJavaRDD, JavaSparkContext ctx) {

        SampleIDPartitioner sampleIDPartitioner = new SampleIDPartitioner(this.sampleIDbySampleName.size() + 1);

        Broadcast<HashMap<String, Integer>>  sampleNamesBroadcast = ctx.broadcast(this.sampleIDbySampleName);

        return samRecordJavaRDD.mapToPair(samRecord -> {
            String rg = samRecord.getStringAttribute("RG");
            int sampleID = sampleNamesBroadcast.value().get(rg);
            return countTupleGenerator(String.valueOf(sampleID), samRecord);
        }).reduceByKey(sampleIDPartitioner, (a, b) -> {
            HashMap<String, Tuple2<Integer, Double>> c;
            if (a.size() < b.size()) {
                c = new HashMap<>(b);
                a.forEach((k, v) -> c.merge(k, v, (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
            } else {
                c = new HashMap<>(a);
                b.forEach((k, v) -> c.merge(k, v,  (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
            }
            return c;
        }).mapPartitionsToPair(, true)
    }

    private Tuple2<String, HashMap<String, Tuple2<Integer, Double>>> countTupleGenerator(String sampleID, SAMRecord record) {

        String markerName = record.getReferenceName();
        Double recaliReadCount = 1.0;

        //LOG.info("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Count Single Record: " + record.toString() + " || Reference Gene name: " + geneName);

        if (false){
            // TODO: SAM filter
            return new Tuple2<>(null, null);
        }

        if (this.doGCRecalibration) {
            recaliReadCount = this.gcBiasRecaliModel.recalibrateForSingle(
                    SequenceUtil.calculateGc(record.getReadBases()),
                    //this.referenceInfoMatrix.value().getSpeciesGenoGC(this.referenceInfoMatrix.value().getGeneSpeciesName(geneName))
            );
        }

        HashMap<String, Tuple2<Integer, Double>> readCount = new HashMap<>(2);
        readCount.put(markerName, new Tuple2<>(1, recaliReadCount));
        return new Tuple2<>(sampleID, readCount);
    }

    private ProfilingResultRecord profilingResultGenerator(
            String clusterName, Tuple4<String, Integer, Double, String> result) {

        ProfilingResultRecord resultRecord;

        //if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
        //    resultRecord = new ProfilingEveResultRecord();
        //} else {
        //
        //}
        resultRecord = new ProfilingResultRecord();

        resultRecord.setClusterName(clusterName);

        resultRecord.setSmTag(result._1());

        resultRecord.setRawReadCount(result._2());
        resultRecord.setrecaliReadCount(result._3());
        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.MARKERS)) {
            int geneLen = this.referenceInfoMatrix.value().getGeneLength(clusterName);
            if (geneLen > 0) {
                resultRecord.setAbundance(result._3() / geneLen);
            } else {
                resultRecord.setAbundance(0.0);
            }
        } else {
            int genoLen = this.referenceInfoMatrix.value().getSpeciesGenoLen(clusterName);
            if (genoLen > 0) {
                resultRecord.setAbundance(result._3() / genoLen);
            } else {
                resultRecord.setAbundance(0.0);
            }
        }
        resultRecord.setReadNameString(result._4());

        return resultRecord;
    }

    @Override
    public void setSampleIDbySampleName(HashMap<String, Integer> sampleIDbySampleName) {
        this.sampleIDbySampleName = sampleIDbySampleName;
    }

    private class CladeNode {
        int cladeGenomeLen; // Average genome length of sub-taxas of the clade.
        int kingdom; // 1: archaea; 2: bacteria; 3: eukaryotes; 4: viruses
        CladeNode fatherClade;
        HashSet<String> childCladeNameSet;
        HashMap<String, Integer> markers2nreads;
    }
}
