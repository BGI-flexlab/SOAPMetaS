package org.bgi.flexlab.metas.profiling;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordIdentityFilter;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.ProfilingPipelineMode;
import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;

import java.util.List;


/**
 * ClassName: ProfilingProcess
 * Description: Control the profiling process.
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingProcess {

    private MetasOptions metasOpt;
    private JavaSparkContext jscontext;

    private SequencingMode seqMode;
    private ProfilingAnalysisMode analysisMode;
    private ProfilingPipelineMode pipelineMode;

    private ProfilingUtils pUtil;

    private boolean doIdentityFiltering = false;

    private MetasSamRecordIdentityFilter identityFilter;

    public ProfilingProcess(final MetasOptions options, final JavaSparkContext context){
        this.metasOpt = options;
        this.jscontext = context;
        this.processInitialize();
    }

    private void processInitialize(){

        this.seqMode = this.metasOpt.getSequencingMode();
        this.analysisMode = this.metasOpt.getProfilingAnalysisMode();
        this.pipelineMode = this.metasOpt.getProfilingPipelineMode();

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
         * TODO: groupByKey方法考虑用其他的bykey方法替换，参考 https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
         *
         * TODO: 注意在最后输出结果时计算相对丰度
         *
         */
        JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD = cleanMetasSamRecordRDD
                .mapToPair(record -> new Tuple2<>(this.pUtil.samRecordNameModifier(record), record))
                .groupByKey()
                .mapValues(new SamRecordListMergeFunction(this.seqMode))
                .filter(item -> (item._2 != null));


        /**
         * TODO: 后续输入要改成 sample-ref 模式的键，record 都有 sample tag，需要对不同的 sample 进行聚合处理
         */
        JavaRDD<ProfilingResultRecord> profilingResultRecordRDD = profilingMethod.runProfiling(readMetasSamPairRDD, null);



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
