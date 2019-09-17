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
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.reference.ReferenceInfoMatrix;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
//import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordInsertSizeFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelBase;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelFactory;

import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.*;

/**
 * ClassName: METAPHLANProfilingMethod
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class METAPHLANProfilingMethod extends ProfilingMethodBase implements Serializable{

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(METAPHLANProfilingMethod.class);

    //private MetasSAMRecordInsertSizeFilter insertSizeFilter;

    //final private Broadcast<ReferenceInfoMatrix> referenceInfoMatrix;
    private ReferenceInfoMatrix referenceInfoMatrix;

    //private boolean doInsRecalibration;
    private boolean doGCRecalibration;

    private GCBiasModelBase gcBiasRecaliModel;

    private double quantile = 0.1;

    public METAPHLANProfilingMethod(MetasOptions options, JavaSparkContext jsc) {

        super(options, jsc);

        //this.profilingAnalysisMode = options.getProfilingAnalysisMode();

        //this.doInsRecalibration = options.isDoInsRecalibration();
        this.doGCRecalibration = options.isDoGcBiasRecalibration();

        if (this.doGCRecalibration) {
            LOG.info("[SOAPMetas::" + METAPHLANProfilingMethod.class.getName() + "] Do GC recalibration.");
            this.gcBiasRecaliModel = new GCBiasModelFactory(options.getGcBiasRecaliModelType(),
                    options.getGcBiasModelInput()).getGCBiasRecaliModel();
            //this.gcBiasRecaliModel.outputCoefficients(options.getProfilingOutputHdfsDir() + "/builtin_model.json");
        } else {
            LOG.info("[SOAPMetas::" + METAPHLANProfilingMethod.class.getName() + "] Skip GC recalibration.");

        }
        //final ReferenceInfoMatrix refMatrix = new ReferenceInfoMatrix(this.metasOpt.getReferenceMatrixFilePath(), this.metasOpt.getSpeciesGenomeGCFilePath());
        //final Broadcast<ReferenceInfoMatrix> broadcastRefMatrix = this.jscontext.broadcast(refMatrix);
        //this.referenceInfoMatrix = jsc.broadcast(new ReferenceInfoMatrix(options.getReferenceMatrixFilePath(), options.getSpeciesGenomeGCFilePath()));
        this.referenceInfoMatrix = new ReferenceInfoMatrix(options.getReferenceMatrixFilePath(), options.getSpeciesGenomeGCFilePath());

        //this.insertSizeFilter = new MetasSAMRecordInsertSizeFilter(options.getInsertSize());

    }


    /**
     * TODO: 提供内容控制参数，控制输出结果所包含的内容。
     *
     * @param readMetasSamPairRDD 键是对应的 SamPair 的 "sampleID\tread name"，值 SamPairRecord 聚合了该 read name
     *                            对应的所有有效的 SAMRecord 信息。
     * @return ProfilingResultRecord RDD that stores reads count results.
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD,
                                                                   Partitioner partitioner) {

        if (partitioner == null) {
            partitioner = new HashPartitioner(200);
        }


        //if (this.doInsRecalibration) {
        //    this.insertSizeFilter.training(readMetasSamPairRDD);
        //}

        /*
         Input:
          key: sampleID[\treadName]
          value: MetasSAMPairRecord
          partition: [sampleID + readname]

         After mapToPair:
          key: sampleID\tSpeciesName\tsmTag
          value: Map<marker, 1>
          partition: [sampleID + SpeciesName]

         After reduceByKey:
          key: sampleID\tSpeciesName\tsmTag
          value: Map<marker, count>
          partition: sampleID + clusterName

         After mapToPair:
          key: sampleID
          value: ProfilingResultRecord
          partition: sampleID + clusterName
          */

        return readMetasSamPairRDD.mapToPair(tuple ->
                {
                    String sampleID = StringUtils.split(tuple._1, '\t')[0];
                    SAMRecord samRecord = tuple._2.getFirstRecord();
                    String marker = samRecord.getReferenceName();
                    String speciesName = this.referenceInfoMatrix.getGeneSpeciesName(marker);
                    Map<String, Tuple2<Integer, Double>> ret = new HashMap<>(2);
                    if (this.doGCRecalibration){
                        Double recaliCount = this.gcBiasRecaliModel.recalibrateForSingle(
                                SequenceUtil.calculateGc(samRecord.getReadBases()),
                                this.referenceInfoMatrix.getSpeciesGenoGC(speciesName)
                        );
                        ret.put(marker, new Tuple2<>(1, recaliCount));
                    } else {
                        ret.put(marker, new Tuple2<>(1, 1.0));
                    }
                    return new Tuple2<>(sampleID + '\t' + speciesName + '\t' + getSampleTag(samRecord), ret);
                }).reduceByKey(partitioner, (a, b) -> {
                    Map<String, Tuple2<Integer, Double>> c;
                    if (a.size() < b.size()) {
                        c = new HashMap<>(b);
                        a.forEach((k, v) -> c.merge(k, v, (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
                    } else {
                        c = new HashMap<>(a);
                        b.forEach((k, v) -> c.merge(k, v,  (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
                    }
                    return c;
                }).mapToPair(this::computeAbundance);
    }

    private Tuple2<String, ProfilingResultRecord> computeAbundance (Tuple2<String, Map<String, Tuple2<Integer, Double>>> tuple) {
        double abundance = 0.0;
        int rawReadCount = 0;
        double recaliReadCount = 0.0;

        String[] keyEle = StringUtils.split(tuple._1, '\t'); //sampleID, SpeciesName, smTag
        Map<String, Tuple2<Integer, Double>> marker2nreads = tuple._2;

        int totalMarker = marker2nreads.size();
        if (totalMarker == 0){
            return new Tuple2<>(keyEle[0], this.profilingResultGenerator(keyEle[1], keyEle[2], 0, 0, 0));
        }

        if (keyEle[1].startsWith("t") || keyEle[1].contains("_sp")) {
            int nonZero = 0;
            for(Tuple2<Integer, Double> rc: marker2nreads.values()){
                if (rc._1 > 0) {nonZero ++;}
            }
            if (nonZero < totalMarker * 0.7){
                return new Tuple2<>(keyEle[0], this.profilingResultGenerator(keyEle[1], keyEle[2], 0, 0, 0));
            }
        }

        int quant = new Double(this.quantile * (double) totalMarker).intValue();
        ArrayList<Tuple4<Double, Integer, Integer, Double>> rat_nreads = new ArrayList<>(totalMarker + 1);
        int rat = 0;
        for (Map.Entry<String, Tuple2<Integer, Double>> entry: marker2nreads.entrySet()){
            int genLen = this.referenceInfoMatrix.getGeneLength(entry.getKey());
            int rawRC = entry.getValue()._1;
            double recaliRC = entry.getValue()._2;
            rat += genLen;
            rat_nreads.add(new Tuple4<>(recaliRC/(double) genLen, genLen, rawRC, recaliRC));
        }

        if (rat <= 0){
            abundance = 0.0;
        } else {
            // tavg_g
            rat_nreads.sort(Comparator.comparing(Tuple4::_1));
            int rGenLen = 0;
            for (int i = quant; i < totalMarker - quant; i++){
                rGenLen += rat_nreads.get(i)._2();
                rawReadCount += rat_nreads.get(i)._3();
                recaliReadCount += rat_nreads.get(i)._4();
            }
            if (rGenLen == 0){
                abundance = 0.0;
            } else {
                abundance = recaliReadCount / (double) rGenLen;
            }
        }
        return new Tuple2<>(keyEle[0], this.profilingResultGenerator(keyEle[1], keyEle[2], rawReadCount, recaliReadCount, abundance));
    }

    /**
     * Generate ProfilingResultRecord instance.
     *
     *
     */
    private ProfilingResultRecord profilingResultGenerator(
            String speciesName, String smTag, int rawReadCount, double recaliReadCount, double abundance) {

        ProfilingResultRecord resultRecord = new ProfilingResultRecord();

        resultRecord.setClusterName(speciesName);

        resultRecord.setSmTag(smTag);

        resultRecord.setRawReadCount(rawReadCount);
        resultRecord.setrecaliReadCount(recaliReadCount);

        resultRecord.setAbundance(abundance);

        return resultRecord;
    }

    private String getSampleTag(SAMRecord record) {

        String smTag;

        SAMReadGroupRecord groupRecord = record.getReadGroup();

        smTag = groupRecord.getSample();

        if (smTag == null) {
            smTag = "NOSMTAG";
        }

        return smTag;
    }
}
