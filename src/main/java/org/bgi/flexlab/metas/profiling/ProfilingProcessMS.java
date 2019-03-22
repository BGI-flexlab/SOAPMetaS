package org.bgi.flexlab.metas.profiling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.mapreduce.SampleIDReadNamePartitioner;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import org.bgi.flexlab.metas.data.structure.sam.SAMMultiSampleList;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordIdentityFilter;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.ProfilingPipelineMode;


import java.io.IOException;
import java.util.List;

/**
 * ClassName: ProfilingProcessMS
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingProcessMS {

    private static final Log LOG = LogFactory.getLog(ProfilingProcessMS.class); // The LOG

    private MetasOptions metasOpt;
    private JavaSparkContext jscontext;

    private ProfilingPipelineMode pipelineMode;

    private int numPartitionEachSample;
    private int sampleCount;

    private ProfilingUtils pUtil;

    private boolean doIdentityFiltering = false;
    private MetasSamRecordIdentityFilter identityFilter;



    public ProfilingProcessMS(final MetasOptions options, final JavaSparkContext context){
        this.metasOpt = options;
        this.jscontext = context;
        this.processInitialize();
    }

    private void processInitialize(){

        this.numPartitionEachSample = Math.max(this.metasOpt.getNumPartitionEachSample(), 1);

        this.pipelineMode = this.metasOpt.getProfilingPipelineMode();

        this.pUtil = new ProfilingUtils(this.metasOpt);

        this.doIdentityFiltering = this.metasOpt.isDoIdentityFiltering();
        this.identityFilter = new MetasSamRecordIdentityFilter();
    }

    public JavaRDD<ProfilingResultRecord> runProfilingProcess(List<String> samFileList){

        this.sampleCount = samFileList.size();

        this.jscontext;

        /**
         * Note: filter() operation of rdd will return a new RDD containing only the elements that
         * makes the filter return true.
         */
        JavaRDD<MetasSamRecord> cleanMetasSamRecordRDD = metasSamRecordRDD
                .filter(rec -> ! rec.getReadUnmappedFlag());

        if (this.doIdentityFiltering){
            cleanMetasSamRecordRDD = cleanMetasSamRecordRDD.filter(this.identityFilter);
        }

        sampleCount
    }

    public JavaRDD<ProfilingResultRecord> runProfilingProcess(String multiSAMFileList){

        try {
            SAMMultiSampleList samMultiSampleList = new SAMMultiSampleList(multiSAMFileList, true, true);

            /**
             * Note: filter() operation of rdd will return a new RDD containing only the elements that
             * makes the filter return true.
             */
            JavaRDD<MetasSamRecord> cleanMetasSamRecordRDD = metasSamRecordRDD
                    .filter(rec -> ! rec.getReadUnmappedFlag());


            if (this.doIdentityFiltering){
                cleanMetasSamRecordRDD = cleanMetasSamRecordRDD.filter(this.identityFilter);
            }

            this.sampleCount = samMultiSampleList.getSampleCount();
        } catch (IOException e){
            LOG.error("Portable error: Multi-sample SAM file list can't be loaded.");
            e.printStackTrace();
        }

    }

    /**
     * Runs profiling. All options should have been set.
     *
     * @param metasSamRecordRDD The RDD of MetasSamRecord. Key is sampleID\treadName
     * @return The RDD of ProfilingResultRecord, which stores the data of all the profiling result and
     *     and other necessary information.
     */
    private JavaRDD<ProfilingResultRecord> runProfilingProcess(JavaPairRDD<String, MetasSamRecord> metasSamRecordRDD){


        // Add one more partition for files without sample information.
        int numPartition = this.numPartitionEachSample * this.sampleCount + 1;
        SampleIDReadNamePartitioner sampleIDPartitioner = new SampleIDReadNamePartitioner(numPartition,
                this.numPartitionEachSample);
        /*
        Creating readname-metasSamRecord pair.
        In paired end sequencing mode, the read name has no "/1" or "/2" suffix.
        In single end sequencing mode, the read name remains unchanged.

        The method is currently designed for read name in the form of "@readname/1 @readname/2" and
        not compatible with other forms.

        The results generated from bowtie2 may lose the "/1/2" suffix of read name, so it is important
        to check the sequencing mode in the spark mapToPair(the 2nd of following) step for samPairRecord.

        TODO: 注意在最后输出结果时计算相对丰度

        After mapToPair && filter:
         key: sampleID\treadName
         value: MetasSamPairRecord
        */
        JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD = metasSamRecordRDD
                .groupByKey(sampleIDPartitioner)
                .mapValues(new SamRecordListMergeFunction(this.metasOpt.getSequencingMode()))
                .filter(item -> (item._2 != null));


        /*
        Since clusterName functions similarly as readName does, so we use
        */
        JavaRDD<ProfilingResultRecord> profilingResultRecordRDD = getProfilingMethod().runProfiling(readMetasSamPairRDD, sampleIDPartitioner);



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
