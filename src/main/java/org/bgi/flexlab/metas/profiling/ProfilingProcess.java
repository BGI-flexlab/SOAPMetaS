package org.bgi.flexlab.metas.profiling;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.io.profilingio.ProfilingResultRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamPairRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamRecord;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordIdentityFilter;
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

    private ProfilingUtils pUtil;

    private boolean doIdentityFiltering = false;
    private boolean doInsRecalibration = false;

    private MetasSamRecordIdentityFilter identityFilter;

    public ProfilingProcess(final MetasOptions options){
        this.metasOpt = options;
        this.processInitialize();
    }

    private void processInitialize(){

        this.seqMode = this.metasOpt.getSequencingMode();
        this.analysisMode = this.metasOpt.getProfilingAnalysisMode();
        this.pipelineMode = this.metasOpt.getProfilingPipelineMode();

        this.doInsRecalibration = this.metasOpt.isDoInsRecalibration();
        this.pUtil = new ProfilingUtils(this.metasOpt);

        this.doIdentityFiltering = this.metasOpt.isDoIdentityFiltering();
        this.identityFilter = new MetasSamRecordIdentityFilter();
    }

    /**
     * Runs profiling. All options should have been set.
     *
     * @param metasSamRecordRDD The RDD of MetasSamRecord instances generated from SamReader.
     * @return The RDD of ProfilingResultRecord, which stores the data of all the profiling result and
     *     and other necessary information.
     */
    public JavaRDD<ProfilingResultRecord> runProfilingProcess(JavaRDD<MetasSamRecord> metasSamRecordRDD){

        ProfilingMethodBase profilingMethod = getProfilingMethod();

        assert (profilingMethod != null);

        /**
         * Note: filter() operation of rdd will return a new RDD containing only the elements that
         * makes the filter return true.
         */
        JavaRDD<MetasSamRecord> cleanMetasSamRecordRDD = metasSamRecordRDD
                .filter(rec -> ! rec.getReadUnmappedFlag());

        if (this.doIdentityFiltering){
            cleanMetasSamRecordRDD = cleanMetasSamRecordRDD.filter(this.identityFilter);
        }

        /**
         * Creating readname-metasSamRecord pair.
         * In paired end sequencing mode, the read name has no "/1" or "/2" suffix.
         * In single end sequencing mode, the read name remains unchanged.
         *
         * The method is currently designed for read name in the form of "@readname/1 @readname/2" and
         * not compatible with other forms.
         * The results generated from bowtie2 may lose the "/1/2" suffix of read name, so it is important
         * to check the sequencing mode in the spark mapToPair(the 2nd of following) step for samPairRecord.
         *
         */

        JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD = cleanMetasSamRecordRDD
                .mapToPair(record -> new Tuple2<>(this.pUtil.samRecordNameModifier(record), record))
                .groupByKey()
                .mapToPair(readSamGroup -> new Tuple2<>(readSamGroup._1, this.pUtil.readSamListToSamPair(readSamGroup._2)))
                .filter(item -> (item._2 != null));

        JavaRDD<ProfilingResultRecord> profilingResultRecordRDD = profilingMethod.runProfiling(readMetasSamPairRDD);



        //NOTE: 相对丰度的计算不应该放在profiling result结果中，因为它是全局相关的，在输出结果的时候进行计算最好
        //Double totalAbundance = rawProfilingRecordRDD.map(resultRec -> resultRec.getAbundance()).reduce((a,b) -> a+b);
        //JavaRDD<ProfilingResultRecord> profilingResultRecordRDD = rawProfilingRecordRDD
        //        .map(resultRecord -> {
        //            ProfilingResultRecord updatedResultRecord = resultRecord;
        //            updatedResultRecord.setRelativeAbun(this.pUtil.computeRelativeAbundance(resultRecord.getAbundance(), totalAbundance));
        //            return updatedResultRecord;
        //        });

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
        } catch (final RuntimeException e){
            e.printStackTrace();
        }
        return null;
    }
}
