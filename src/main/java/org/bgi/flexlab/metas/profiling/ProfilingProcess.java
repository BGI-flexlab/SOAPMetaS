package org.bgi.flexlab.metas.profiling;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.io.profilingio.ProfilingResultRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamPairRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamRecord;
import org.bgi.flexlab.metas.profiling.filter.RawMetasSamRecordFilter;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.ProfilingPipelineMode;
import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;

/**
 * ClassName: ProfilingProcess
 * Description: 控制丰度计算流程。
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingProcess {

    private MetasOptions metasOpt;

    private SequencingMode seqMode;
    private ProfilingAnalysisMode analysisMode;
    private ProfilingPipelineMode pipelineMode;

    private double minIdentity;
    private ProfilingUtils pUtil;


    public ProfilingProcess(final MetasOptions options){
        this.metasOpt = options;
        this.processInitialize();
    }

    private void processInitialize(){

        this.seqMode = this.metasOpt.getSequencingMode();
        this.analysisMode = this.metasOpt.getProfilingAnalysisMode();
        this.pipelineMode = this.metasOpt.getProfilingPipelineMode();

        this.minIdentity = 0;
        this.pUtil = new ProfilingUtils(this.metasOpt);

    }

    /**
     * Runs profiling. All options should have been set.
     */
    public JavaRDD<ProfilingResultRecord> runProfilingProcess(JavaRDD<MetasSamRecord> metasSamRecordRDD){

        // Note: filter() operation of rdd will return a new RDD containing only the elements that
        // makes the filter return true.
        RawMetasSamRecordFilter rawRecFilter = new RawMetasSamRecordFilter(this.minIdentity);

        ProfilingMethodBase profilingMethod = getProfilingMethod();

        assert (profilingMethod != null);


        /**
         * Creating readname-metasSamRecord pair.
         * In paired end sequencing mode, the read name has no "/1" or "/2" suffix.
         * In single end sequencing mode, the read name remains unchanged.
         *
         * 关于输入fastq的文件格式，此处只针对"@readname/1 @readname/2"的形式，对于其他pair-end的name形式暂时不兼容。
         * bowtie2的结果可能造成read的/1/2后缀丢失，因此，需要在进行sampairrecord的mapping过程中需要考虑不同的seqMode。
         */



        JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD = metasSamRecordRDD
                .filter(rawRecFilter)
                .mapToPair(record -> new Tuple2<>(this.pUtil.samRecordNameModifier(record), record))
                .groupByKey()
                .mapToPair(readSamGroup -> new Tuple2<>(readSamGroup._1, this.pUtil.readSamListToSamPair(readSamGroup._2)))
                .filter(item -> (item._2 != null));


        JavaPairRDD<String, ProfilingResultRecord> clusterProfilingResultRDD = profilingMethod.runProfiling(readMetasSamPairRDD);

        Double totalAbundance = clusterProfilingResultRDD.map(resultRec -> resultRec._2.getAbundance()).reduce((a,b) -> a+b);

        JavaRDD<ProfilingResultRecord> profilingResultRecordRDD = clusterProfilingResultRDD
                .map(clusterResult -> {
                    ProfilingResultRecord resultRecord = clusterResult._2;
                    resultRecord.setRelativeAbun(this.pUtil.computeRelativeAbundance(resultRecord.getAbundance(), totalAbundance));
                    return resultRecord;
                });

        return profilingResultRecordRDD;
    }

    /**
     * Choosing proper profiling pipeline.
     *
     * @return ProfilingMethodBase New profiling pipeline instance of selected software.
     */
    public ProfilingMethodBase getProfilingMethod(){
        try {
            if(this.pipelineMode.equals(ProfilingPipelineMode.METAPHLAN)){
                return new METAPHLANProfilingMethod(this.metasOpt);
            } else if (this.pipelineMode.equals(ProfilingPipelineMode.COMG)){
                return new COMGProfilingMethod(this.metasOpt);
            }
        } catch (final NullPointerException e){
            e.printStackTrace();
        } catch (final Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
