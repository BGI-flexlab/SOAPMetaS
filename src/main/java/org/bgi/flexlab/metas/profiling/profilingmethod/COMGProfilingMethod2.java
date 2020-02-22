package org.bgi.flexlab.metas.profiling.profilingmethod;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.reference.ReferenceInfoMatrix;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordAlignLenFilter;
import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordIdentityFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelBase;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelFactory;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.HashMap;


/**
 * ClassName: COMGProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public final class COMGProfilingMethod2 extends ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(COMGProfilingMethod2.class);

    private Broadcast<ReferenceInfoMatrix> referenceInfoMatrix = null;

    private boolean doGCRecalibration;

    private GCBiasModelBase gcBiasRecaliModel;

    private String referenceMatrixFilePath;

    private String speciesGenomeGCFilePath;

    private boolean doIdentityFiltering = false;
    private boolean doAlignLenFiltering = false;

    private MetasSAMRecordIdentityFilter identityFilter;
    private MetasSAMRecordAlignLenFilter alignLenFilter;

    public COMGProfilingMethod2(MetasOptions options, JavaSparkContext jsc) {
        super(options, jsc);

        //this.profilingAnalysisMode = options.getProfilingAnalysisMode();

        this.doGCRecalibration = options.isDoGcBiasRecalibration();

        if (this.doGCRecalibration) {
            LOG.debug("[SOAPMetas::" + COMGProfilingMethod2.class.getName() + "] Do GC recalibration.");
            this.gcBiasRecaliModel = new GCBiasModelFactory(options.getGcBiasRecaliModelType(),
                    options.getGcBiasModelInput()).getGCBiasRecaliModel();
            //this.gcBiasRecaliModel.outputCoefficients(options.getProfilingOutputHdfsDir() + "/builtin_model.json");
        } else {
            LOG.debug("[SOAPMetas::" + COMGProfilingMethod2.class.getName() + "] Skip GC recalibration.");
        }

        this.doIdentityFiltering = options.isDoIdentityFiltering();
        this.doAlignLenFiltering = options.isDoAlignLenFiltering();

        if (doIdentityFiltering) {
            this.identityFilter = new MetasSAMRecordIdentityFilter(options.getMinIdentity());
        }
        if (doAlignLenFiltering) {
            this.alignLenFilter = new MetasSAMRecordAlignLenFilter(options.getMinAlignLength());
        }

        referenceMatrixFilePath = options.getReferenceMatrixFilePath();

        speciesGenomeGCFilePath = options.getSpeciesGenomeGCFilePath();

        //应避免过早创建对象和广播变量
        //final ReferenceInfoMatrix refMatrix = new ReferenceInfoMatrix(this.metasOpt.getReferenceMatrixFilePath(), this.metasOpt.getSpeciesGenomeGCFilePath());
        //final Broadcast<ReferenceInfoMatrix> broadcastRefMatrix = this.jscontext.broadcast(refMatrix);
    }

    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD,
                                                                   Partitioner partitioner) {
        return null;
    }

    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaRDD<SAMRecord> reads,
                                                                   JavaSparkContext ctx) {

        if(referenceInfoMatrix == null){
            referenceInfoMatrix = ctx.broadcast(new ReferenceInfoMatrix(referenceMatrixFilePath, speciesGenomeGCFilePath));
        }
        LOG.info("[SOAPMetas::" + COMGProfilingMethod2.class.getName() + "] Start COMG Profiling.");
//        samRecord.getStringAttribute("RG")

        //RGID 作为 sampleName
        //TODO 获取sampleID
        Broadcast<HashMap<String, Integer>>  sampleNamesBroadcast = ctx.broadcast(this.sampleIDbySampleName);

        return reads.mapToPair(samRecord -> {
            String rg = samRecord.getStringAttribute("RG");
            int sampleID = sampleNamesBroadcast.value().get(rg);
            return countTupleGenerator(String.valueOf(sampleID), samRecord);})
                .filter(tuple -> tuple._1 != null)
                .reduceByKey((a, b) -> new Tuple4<>(a._1(), a._2() + b._2(), a._3() + b._3(), ""))
                .mapToPair(tuple -> {
                    String[] keyStr = StringUtils.split(tuple._1, '\t');
                    return new Tuple2<>(
                            keyStr[0],
                            this.profilingResultGenerator(keyStr[1], tuple._2));
                });
    }

    @Override
    public void setSampleIDbySampleName(HashMap<String, Integer> sampleIDbySampleName) {
        this.sampleIDbySampleName = sampleIDbySampleName;

    }

    /**
     * Generate ProfilingResultRecord instance.
     *
     * @param clusterName String of cluster name. "Cluster" is related to analysis level.
     * @param result      Tuple4 of result list. Tuple4< SMTag, raw read count (unrecalibrated), recalibrated read count, merged read>
     * @return
     */
    private ProfilingResultRecord profilingResultGenerator(
            String clusterName, Tuple4<String, Integer, Double, String> result) {

        ProfilingResultRecord resultRecord;

        //if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
        //    resultRecord = new ProfilingEveResultRecord();
        //} else {
        //
        //}

        if (this.outputFormat.equals("CAMI")) {
            resultRecord = new ProfilingResultRecord(8);
        } else if (this.outputFormat.equals("DETAILED")) {
            resultRecord = new ProfilingResultRecord(4);
        } else {
            resultRecord = new ProfilingResultRecord(2);
        }

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

    private Tuple2<String, Tuple4<String, Integer, Double, String>> computeReadCount(SAMRecord samRecord) {

        String RGID = samRecord.getStringAttribute("RG");

        //LOG.trace("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Record in SE mode. " +
        //        samRecord.getReadName());

        return this.countTupleGenerator(RGID, samRecord);
    }


    private Tuple2<String, Tuple4<String, Integer, Double, String>> countTupleGenerator(
            String sampleID, SAMRecord record) {

        if (record.isSecondaryAlignment() || record.getMappingQuality() <= 5 ) { //TODO: 添加质量分数的 option
            return new Tuple2<>(null, null);
        }

        if (doIdentityFiltering && this.identityFilter.filter(record)){
            return new Tuple2<>(null, null);
        }

        if (doAlignLenFiltering && this.alignLenFilter.filter(record)) {
            return new Tuple2<>(null, null);
        }

        String clusterName;
        Integer rawReadCount = 1;
        Double recaliReadCount;

        String geneName = record.getReferenceName();
        String sampleName = record.getStringAttribute("RG");

        //LOG.info("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Count Single Record: " + record.toString() + " || Reference Gene name: " + geneName);

        if (this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES)) {
            clusterName = this.referenceInfoMatrix.value().getGeneSpeciesName(geneName);
        } else {
            clusterName = geneName;
        }

        if (clusterName == null) {
            return new Tuple2<>(null, null);
        }

        if (this.doGCRecalibration) {
            recaliReadCount = this.gcBiasRecaliModel.recalibrateForSingle(
                    SequenceUtil.calculateGc(record.getReadBases()),
                    this.referenceInfoMatrix.value().getSpeciesGenoGC(this.referenceInfoMatrix.value().getGeneSpeciesName(geneName))
            );
        } else {
            recaliReadCount = (double) rawReadCount;
        }


        return new Tuple2<>(sampleID + '\t' + clusterName, new Tuple4<>(sampleName,
                rawReadCount, recaliReadCount, ""));
    }

}
