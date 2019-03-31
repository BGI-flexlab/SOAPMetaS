package org.bgi.flexlab.metas.profiling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDReadNamePartitioner;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDPartitioner;
import org.bgi.flexlab.metas.data.mapreduce.input.sam.MetasSamInputFormat;
import org.bgi.flexlab.metas.data.mapreduce.input.sam.MetasSamRecordWritable;
import org.bgi.flexlab.metas.data.mapreduce.output.profiling.ProfilingResultWriteFunction;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import org.bgi.flexlab.metas.data.structure.sam.SAMMultiSampleList;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordIdentityFilter;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import scala.Tuple2;


import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * ClassName: ProfilingProcessMS
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class ProfilingProcessMS {

    private static final Log LOG = LogFactory.getLog(ProfilingProcessMS.class); // The LOG

    private MetasOptions metasOpt;
    private JavaSparkContext jscontext;

    private String pipeline;
    private ProfilingAnalysisMode analysisMode;

    private int numPartitionEachSample;
    private int sampleCount;

    private boolean doIdentityFiltering = false;
    private MetasSamRecordIdentityFilter identityFilter;

    private String tmpDir;
    private String outputHdfsDir;


    public ProfilingProcessMS(final MetasOptions options, final JavaSparkContext context){
        this.metasOpt = options;
        this.jscontext = context;
        this.processInitialize();
    }

    /**
     * TODO: Add createOutputDirectory method if needed.
     */
    private void processInitialize(){

        this.numPartitionEachSample = Math.max(this.metasOpt.getNumPartitionEachSample(), 1);

        this.pipeline = this.metasOpt.getProfilingPipeline();
        this.analysisMode = this.metasOpt.getProfilingAnalysisMode();

        this.doIdentityFiltering = this.metasOpt.isDoIdentityFiltering();
        if (this.doIdentityFiltering) {
            this.identityFilter = new MetasSamRecordIdentityFilter(this.metasOpt.getMinIdentity());
        }

        this.tmpDir = this.jscontext.getLocalProperty("spark.local.dir");

        //We set the tmp dir
        if ((this.tmpDir == null || this.tmpDir == "null")
                && this.metasOpt.getProfilingTmpDir() != null
                && !this.metasOpt.getProfilingTmpDir().isEmpty()) {

            this.tmpDir = this.metasOpt.getProfilingTmpDir();
        }

        if (this.tmpDir == null || this.tmpDir == "null") {
            this.tmpDir = jscontext.hadoopConfiguration().get("hadoop.tmp.dir");
        }

        if (this.tmpDir.startsWith("file:")) {
            this.tmpDir = this.tmpDir.replaceFirst("file:", "");
        }

        File tmpFileDir = new File(this.tmpDir);

        if(!tmpFileDir.isDirectory() || !tmpFileDir.canWrite()) {
            this.tmpDir = "/tmp/";
        }

        this.outputHdfsDir = this.metasOpt.getProfilingOutputHdfsDir();
        createOutputFolder(this.jscontext.hadoopConfiguration(), this.outputHdfsDir);

    }

    private void createOutputFolder(Configuration hadoopConf, String directoryPath) {
        try {
            FileSystem fs = FileSystem.get(hadoopConf);

            // Path variable
            Path outputDir = new Path(directoryPath);

            // Directory creation
            if (!fs.exists(outputDir)) {
                fs.mkdirs(outputDir);
            }
            else {
                fs.delete(outputDir, true);
                fs.mkdirs(outputDir);
            }

            fs.close();
        }
        catch (IOException e) {
            LOG.error("Fail to create SAM output directory.");
            e.printStackTrace();
        }
    }

    /**
     *
     * @param samFileList Paths string list. Format: ReadGroupID\tPath
     * @return Output file paths string.
     */
    public List<String> runProfilingProcess(List<String> samFileList){
        String multiSamListFile = this.tmpDir + "/" + "MultipleSampleSAMListFile";

        File samList = new File(multiSamListFile);
        FileOutputStream fos1;
        BufferedWriter bw1;

        try {
            fos1 = new FileOutputStream(samList);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            for (int i = 0; i < samFileList.size(); i++) {
                bw1.write(samFileList.get(i));
                bw1.newLine();
            }

            bw1.close();
        } catch (FileNotFoundException e){
            e.printStackTrace();
            LOG.error("[" + this.getClass().getName() + "] :: MultiSampleFileList not found. " + e.toString());
        } catch (IOException e){
            e.printStackTrace();
            LOG.error("[" + this.getClass().getName() + "] :: MultiSampleFileList IO error. " + e.toString());
        }

        return runProfilingProcess(multiSamListFile);
    }

    public List<String> runProfilingProcess(String multiSamListFile){

        JavaPairRDD<String, MetasSamRecord> cleanMetasSamRecordRDD = null;

        try {
            SAMMultiSampleList samMultiSampleList = new SAMMultiSampleList(multiSamListFile, true, true);
            Configuration conf = this.jscontext.hadoopConfiguration();
            conf.set("metas.data.mapreduce.input.sammultisamplelist", this.metasOpt.getMultiSampleList());


            /**
             * Note: filter() operation of rdd will return a new RDD containing only the elements that
             * makes the filter return true.
             */
            cleanMetasSamRecordRDD = this.jscontext
                    .newAPIHadoopFile(samMultiSampleList.getAllSAMFilePath(), MetasSamInputFormat.class,
                            Text.class, MetasSamRecordWritable.class, conf)
                    .filter(tup -> tup._2 != null)
                    .mapToPair(tup -> new Tuple2<>(tup._1.toString(), tup._2.get()));


            if (this.doIdentityFiltering){
                cleanMetasSamRecordRDD = cleanMetasSamRecordRDD.filter(this.identityFilter);
            }

            this.sampleCount = samMultiSampleList.getSampleCount();

        } catch (IOException e){
            LOG.error("Portable error: Multi-sample SAM file list can't be loaded.");
            e.printStackTrace();
        }

        return runProfilingProcess(cleanMetasSamRecordRDD);
    }

    /**
     * Runs profiling. All options should have been set.
     *
     * @param metasSamRecordRDD The RDD of MetasSamRecord. Key is sampleID\treadName
     * @return The RDD of ProfilingResultRecord, which stores the data of all the profiling result and
     *     and other necessary information.
     */
    private List<String> runProfilingProcess(JavaPairRDD<String, MetasSamRecord> metasSamRecordRDD){


        // Add one more partition for files without sample information.
        int numPartition = this.numPartitionEachSample * this.sampleCount + 1;
        SampleIDReadNamePartitioner sampleIDClusterNamePartitioner = new SampleIDReadNamePartitioner(numPartition,
                this.numPartitionEachSample);
        SampleIDPartitioner sampleIDPartitioner = new SampleIDPartitioner(this.sampleCount + 1);

        /*
        InputRDD:
         key: sampleID\treadName
         value: MetasSamRecord
         partition: defaultPartition

        After groupByKey:
         key: sampleID\treadName
         value: Interable<MetasSamRecord>
         partition: sampleID + clusterName

        After mapValues && filter:
         key: sampleID\treadName
         value: MetasSamPairRecord
         partition: sampleID + clusterName
        */
        JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD = metasSamRecordRDD
                .groupByKey(sampleIDClusterNamePartitioner)
                .mapValues(new SamRecordListMergeFunction(this.metasOpt.getSequencingMode()))
                .filter(item -> (item._2 != null));

        readMetasSamPairRDD.persist(StorageLevel.MEMORY_ONLY_SER());

        /*
        Since clusterName functions similarly as readName does, so we use the same partitioner.

        After runProfiling:
         key: sampleID
         value: ProfilingResultRecord (Tuple3<raw read count (uncorrected), corrected read count, merged read>)
         partition: sampleID + clusterName

        Note: Result Record doesn't contains relative abundance. And the RDD has been partitioned
         according to the sampleID and clusterName.hash.
        */
        JavaPairRDD<String, ProfilingResultRecord> profilingResultRecordRDD = getProfilingMethod()
                .runProfiling(readMetasSamPairRDD, sampleIDClusterNamePartitioner);


        profilingResultRecordRDD.persist(StorageLevel.MEMORY_AND_DISK());
        readMetasSamPairRDD.unpersist();

        /*
        Input:
         key: sampleID
         value: ProfilingResultRecord
         partition: sampleID + clusterName

        After mapToPair:
         key: sampleID
         value: (Double) abundance
         partition: sampleID + clusterName

        After reduceByKey:
         key: sampleID
         value: (Double) sampleTotalAbundance
         partition: sampleID
         */
        Map<String, Double> sampleTotalAbundanceMap = profilingResultRecordRDD
                .mapValues(v -> v.getAbundance())
                .reduceByKey(sampleIDPartitioner, (abun1, abun2) -> abun1+abun2)
                .collectAsMap();


        /*
        Input:
         key: sampleID
         value: ProfilingResultRecord (no relative abundance)
         partition: sampleID + clusterName

        After partitionBy:
         key: sampleID
         value: ProfilingResultRecord (no relative abundance)
         partition: sampleID

        After mapPartitions:
         outputHDFSDir/profiling/<appID>-Profiling-<readGroupID>.abundance[.evaluation]
         */
        List<String> outputFilePathsList = profilingResultRecordRDD
                .partitionBy(sampleIDPartitioner)
                .mapPartitions(new ProfilingResultWriteFunction(profilingResultRecordRDD.context(), this.outputHdfsDir, sampleTotalAbundanceMap, this.analysisMode), true)
                .collect();

        profilingResultRecordRDD.unpersist();
        return outputFilePathsList;
    }

    /**
     * Choosing proper profiling pipeline.
     *
     * @return ProfilingMethodBase New profiling pipeline instance of selected software.
     */
    public ProfilingMethodBase getProfilingMethod(){
        try {
            if(this.pipeline.equals("metaphlan")){
                return new COMGProfilingMethod(this.metasOpt);
            } else if (this.pipeline.equals("comg")){
                return new METAPHLANProfilingMethod(this.metasOpt);
            }
        } catch (final NullPointerException e){
            e.printStackTrace();
        } catch (final RuntimeException e){
            e.printStackTrace();
        }
        return null;
    }


}
