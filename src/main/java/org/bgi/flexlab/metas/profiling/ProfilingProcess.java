package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordIdentityFilter;
import org.bgi.flexlab.metas.profiling.profilingmethod.COMGProfilingMethod;
import org.bgi.flexlab.metas.profiling.profilingmethod.MEPHProfilingMethod;
import org.bgi.flexlab.metas.profiling.profilingmethod.ProfilingMethodBase;
//import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;

/**
 * ClassName: ProfilingProcess
 * Description: Control the profiling process.
 *
 * @author heshixu@genomics.cn
 */

@Deprecated
public class ProfilingProcess {

    private MetasOptions metasOpt;
    private JavaSparkContext jscontext;

    private SequencingMode seqMode;
    //private ProfilingAnalysisMode analysisMode;
    private String pipeline;

    private ProfilingUtils pUtil;

    private boolean doIdentityFiltering = false;

    private MetasSAMRecordIdentityFilter identityFilter;

    public ProfilingProcess(final MetasOptions options, final JavaSparkContext context){
        this.metasOpt = options;
        this.jscontext = context;
        this.processInitialize();
    }

    private void processInitialize(){

        this.seqMode = this.metasOpt.getSequencingMode();
        //this.analysisMode = this.metasOpt.getProfilingAnalysisMode();
        this.pipeline = this.metasOpt.getProfilingPipeline();

        this.pUtil = new ProfilingUtils(this.metasOpt);

        this.doIdentityFiltering = this.metasOpt.isDoIdentityFiltering();
        if (this.doIdentityFiltering) {
            this.identityFilter = new MetasSAMRecordIdentityFilter(this.metasOpt.getMinIdentity());
        }
    }


    /**
     * Runs profiling. All options should have been set.
     *
     * @param metasSamRecordRDD The RDD of SAMRecord instances generated from SamReader.
     * @return The RDD of ProfilingResultRecord, which stores the data of all the profiling result and
     *     and other necessary information.
     */
    public JavaPairRDD<String, ProfilingResultRecord> runProfilingProcess(JavaPairRDD<String, SAMRecord> metasSamRecordRDD){

        ProfilingMethodBase profilingMethod = getProfilingMethod();

        /**
         * Note: filter() operation of rdd will return a new RDD containing only the elements that
         * makes the filter return true.
         */
        JavaPairRDD<String, SAMRecord>  cleanMetasSamRecordRDD = metasSamRecordRDD
                .filter(tuple -> ! tuple._2.getReadUnmappedFlag());

        if (this.doIdentityFiltering){
            cleanMetasSamRecordRDD = cleanMetasSamRecordRDD;
        }

        /**
         * Creating readname-SAMRecord pair.
         * In paired end sequencing mode, the read name has no "/1" or "/2" suffix.
         * In single end sequencing mode, the read name remains unchanged.
         *
         * The method is currently designed for read name in the form of "@readname/1 @readname/2" and
         * not compatible with other forms.
         * The results generated from bowtie2 may lose the "/1/2" suffix of read name, so it is important
         * to check the sequencing mode in the spark mapToPair(the 2nd of following) step for samPairRecord.
         *
         * TODO: 注意多样本模式下bowtie中的readname已经不包含末尾的/1/2后缀。在读取fastq的时候就已经去除后缀。因此完全依赖CIGAR来判断pair信息。
         *
         * TODO: groupByKey方法考虑用其他的bykey方法替换，参考 https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
         *
         * TODO: 注意在最后输出结果时计算相对丰度
         *
         */
        JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD = cleanMetasSamRecordRDD
                .mapToPair(tuple -> new Tuple2<>(this.pUtil.samRecordNameModifier(tuple._2), tuple._2))
                .groupByKey()
                .mapValues(new SamRecordListMergeFunction(this.seqMode))
                .filter(item -> (item._2 != null));


        JavaPairRDD<String, ProfilingResultRecord> profilingResultRecordRDD = profilingMethod.runProfiling(readMetasSamPairRDD, null);



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
        if(this.pipeline.equals("meph")){
            return new COMGProfilingMethod(this.metasOpt, this.jscontext);
        } else if (this.pipeline.equals("comg")){
            return new MEPHProfilingMethod(this.metasOpt, this.jscontext);
        } else {
            return null;
        }
    }
}
