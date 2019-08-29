package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.SOAPMetas;
import org.bgi.flexlab.metas.data.mapreduce.input.sam.MetaSamSource;
import org.bgi.flexlab.metas.data.mapreduce.output.profiling.ProfilingOutputFormat;
import org.bgi.flexlab.metas.data.mapreduce.output.profiling.RelativeAbundanceFunction;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDPartitioner;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDReadNamePartitioner;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.SAMMultiSampleList;
import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordIdentityFilter;
import org.bgi.flexlab.metas.util.DataUtils;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: ProfilingNewProcessMS
 * Description: New multiple-sample profiling process class. Exploit new SAM InputFormat for
 *     paired-ended and single-ended reads respectively.
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingNewProcessMS2 {
    private static final Log LOG = LogFactory.getLog(ProfilingNewProcessMS2.class); // The LOG

    private MetasOptions metasOpt;
    private JavaSparkContext jscontext;

    private String pipeline;
    private ProfilingAnalysisMode analysisMode;
    private SequencingMode seqMode;

    private SAMMultiSampleList samMultiSampleList;
    private int numPartitionEachSample;

    private boolean doIdentityFiltering = false;
    private MetasSAMRecordIdentityFilter identityFilter;

    private String tmpDir;
    private String outputHdfsDir;

    private JobConf jcf;

    public ProfilingNewProcessMS2(final MetasOptions options, final JavaSparkContext context){
        this.metasOpt = options;
        this.jscontext = context;
        this.processConstruct();
    }

    private void processConstruct(){

        this.numPartitionEachSample = Math.max(this.metasOpt.getNumPartitionEachSample(), 1);

        this.pipeline = this.metasOpt.getProfilingPipeline();
        this.analysisMode = this.metasOpt.getProfilingAnalysisMode();
        this.seqMode = this.metasOpt.getSequencingMode();

        this.doIdentityFiltering = this.metasOpt.isDoIdentityFiltering();
        if (this.doIdentityFiltering) {
            this.identityFilter = new MetasSAMRecordIdentityFilter(this.metasOpt.getMinIdentity());
        }
        Configuration conf = this.jscontext.hadoopConfiguration();
        this.jcf = new JobConf(conf);

        this.tmpDir = this.metasOpt.getProfilingTmpDir();
        if (this.tmpDir == null || this.tmpDir.isEmpty()) {
            this.tmpDir = "/tmp/" + this.jscontext.appName() + "_TEMP/profiling";
        }
        try {
            DataUtils.createHDFSFolder(conf, "file://" + this.tmpDir);
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] Fail to create profiling temp directory. " + e.toString());
        }

        this.outputHdfsDir = this.metasOpt.getProfilingOutputHdfsDir();
        //try {
        //    DataUtils.createHDFSFolder(conf, this.outputHdfsDir);
        //} catch (IOException e){
        //    LOG.error("[SOAPMetas::" + ProfilingNewProcessMS.class.getName() + "] Fail to create profiling output directory. " + e.toString());
        //}
        LOG.info("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] Profiling process: Temp directory: " + this.tmpDir +
                " . Output Directpry: " + this.outputHdfsDir);

        // These config items are used in MetasSAMWFInputFormat
        this.jcf.set("metas.profiling.outputHdfsDir", this.outputHdfsDir);
        this.jcf.set("metas.application.name", this.jscontext.appName());
    }

    /**
     *
     * @param samFileList Paths string list. Format: ReadGroupID\tPath
     */
    public void processInitialize(List<String> samFileList){
        String multiSamListFile = this.tmpDir + "/" + "tmp-multiSampleSAMList-" + this.jscontext.appName();

        File samList = new File(multiSamListFile);
        FileOutputStream fos1;
        BufferedWriter bw1;

        try {
            fos1 = new FileOutputStream(samList);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            for (String samInput: samFileList) {
                bw1.write(samInput);
                bw1.newLine();
            }

            bw1.close();
            fos1.close();
        } catch (FileNotFoundException e){
            LOG.error("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] MultiSampleFileList not found. " + e.toString());
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] MultiSampleFileList IO error. " + e.toString());
        }

        this.processInitialize(multiSamListFile);
    }

    public void processInitialize(String multiSamListFile){

        try {
            samMultiSampleList = new SAMMultiSampleList(multiSamListFile, this.metasOpt.isLocalFS(), false, true);
            this.jscontext.hadoopConfiguration().set("metas.data.mapreduce.input.samsamplelist", multiSamListFile);

        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] Portable error: Multi-sample SAM file list can't be loaded.");
            e.printStackTrace();
        }
    }

    /**
     * Runs profiling. All options should have been set.
     *
     * @return The RDD of ProfilingResultRecord, which stores the data of all the profiling result and
     *     and other necessary information.
     */
    public List<String> runProfilingProcess(){

        int sampleCount = samMultiSampleList.getSampleCount();
        String filePath = this.samMultiSampleList.getAllSAMFilePath();

        // Add one more partition for files without sample information.
        int numPartition = this.numPartitionEachSample * sampleCount + 1;
        SampleIDReadNamePartitioner sampleIDClusterNamePartitioner = new SampleIDReadNamePartitioner(numPartition,
                this.numPartitionEachSample);
        SampleIDPartitioner sampleIDPartitioner = new SampleIDPartitioner(sampleCount + 1);
        LOG.info("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] SampleCount: " + sampleCount +
                " Partition each sample: " + this.numPartitionEachSample + " Total Partition Number: " + numPartition);

        LOG.info("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] All input sam file paths: " + filePath);


        ProfilingMethodBase profilingMethod = getProfilingMethod();

        ///*
        //Function: Read SAM file. No extra operation during reading.
        //Note: filter() operation of rdd will return a new RDD containing only the elements that makes the filter return true.
        //---
        //After newAPIHadoopFile && filter
        //key: sampleID + "\t" + readName.replaceFirst("/[12]$", "") <Text>
        //value: SAMRecordWritable
        //partition: defaultPartitoner
        //---
        //After mapToPair:
        //key: sampleID + "\t" + readName.replaceFirst("/[12]$", "")
        //value: SAMRecord
        //partition: defaultPartitoner
        // */
        //JavaPairRDD<String, SAMRecord> cleanMetasSamRecordRDD = this.jscontext
        //        .newAPIHadoopFile(filePath, MetasSAMInputFormat.class, Text.class, SAMRecordWritable.class,
        //                this.jscontext.hadoopConfiguration())
        //        .mapToPair(rec -> {
        //            //LOG.trace("[SOAPMetas::" + ProfilingNewProcessMS.class.getName() + "] Remaining samRecod: " + rec._1.toString());
        //            return new Tuple2<>(rec._1.toString(), rec._2.get());
        //        })
        //        .filter(tup -> tup._2 != null);
        //if (this.doIdentityFiltering){
        //    cleanMetasSamRecordRDD = cleanMetasSamRecordRDD.filter(this.identityFilter);
        //}
        ///*
        //SamRecord Group and merge.
        //---
        //InputRDD:
        // key: sampleID\treadName
        // value: SAMRecord
        // partition: defaultPartition
        //---
        //After groupByKey:
        // key: sampleID\treadName
        // value: Interable<SAMRecord>
        // partition: sampleID + readName
        //---
        //After mapValues && filter:
        // key: sampleID\treadName
        // value: MetasSAMPairRecord
        // partition: sampleID + readName
        //*/
        //JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD = cleanMetasSamRecordRDD
        //        .groupByKey(sampleIDClusterNamePartitioner)
        //        .mapValues(new SamRecordListMergeFunction(this.metasOpt.getSequencingMode()))
        //        .filter(item -> (item._2 != null));

        // New function for reading SAM file. Construct pair-record during input process.


        // TODO  根据 metasOpt.getReferenceMatrixFilePath() 和 samMultiSampleList 创建header

        final SAMFileHeader header = SOAPMetas.getHeader(new Path("file:///hwfssz1/BIGDATA_COMPUTING/huangzhibo/workitems/SOAPMeta/SRS014287_header.sam"), jscontext.hadoopConfiguration());


//        List<String> sampleNames = new ArrayList<>();
//        for (SAMReadGroupRecord rg: header.getReadGroups()) {
//            sampleNames.add(rg.getSample());
//        }

        MetaSamSource samSource = new MetaSamSource();
        JavaRDD<SAMRecord> reads = null;
        try {
            reads = samSource.getReads(jscontext, filePath, 1024*1024*256, header);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        if (this.doIdentityFiltering){
//            reads = reads.map(rec -> {
//                if (this.identityFilter.filter(rec)){
//                    v.setFirstRecord(rec2);
//                    v.setSecondRecord(null);
//                    v.setProperPaired(false);
//                    rec1 = null;
//                }
//                if (this.identityFilter.filter(rec2)) {
//                    v.setSecondRecord(null);
//                    v.setProperPaired(false);
//                    rec2 = null;
//                }
//                if (rec1 == null && rec2 == null){
//                    v = null;
//                }
//                return v;
//            }).filter(rec -> rec._2 != null);
//        }

        /*
        Since clusterName functions similarly as readName does, so we use the same partitioner.
        Input:
         key: sampleID
         value: MetasSAMPairRecord
         partition: default (per input file)

        After runProfiling:
         key: sampleID
         value: ProfilingResultRecord (Tuple3<raw read count (unrecalibrated), recalibrated read count, merged read>)
         partition: sampleID + clusterName

        After partitionBy:
         key: sampleID
         value: ProfilingResultRecord (Tuple3<raw read count (unrecalibrated), recalibrated read count, merged read>)
         partition: sampleID

        After mapPartitionsToPair:
         key: sampleID
         value: ProfilingResultRecord (Traw read count (unrecalibrated), recalibrated read count, merged read, rel abun)
         partition: no change

        Note: Result Record doesn't contains relative abundance. And the RDD has been partitioned
         according to the sampleID and clusterName.hash.
        */
//        if (this.metasOpt.isDoInsRecalibration()) {
//            reads.persist(StorageLevel.MEMORY_AND_DISK());
//        }

        JavaPairRDD<String, ProfilingResultRecord> profilingResultRecordRDD = profilingMethod
                .runProfiling(reads, jscontext);

        JavaPairRDD<String, ProfilingResultRecord> relAbunResultRDD = profilingResultRecordRDD
                .partitionBy(sampleIDPartitioner)
                .mapPartitionsToPair(new RelativeAbundanceFunction(), true);

//        if (this.metasOpt.isDoInsRecalibration()) {
//            cleanMetasSamRecordRDD.unpersist();
//        }

        relAbunResultRDD.persist(StorageLevel.MEMORY_AND_DISK());

        //String resultsDir = this.outputHdfsDir + "/results"

        // save rdd profiling file
        relAbunResultRDD.saveAsHadoopFile(this.outputHdfsDir, String.class, ProfilingResultRecord.class, ProfilingOutputFormat.class, this.jcf);

        //if (this.analysisMode.equals(ProfilingAnalysisMode.PROFILE)) {
        //
        //} else {
        //    relAbunResultRDD.saveAsHadoopFile(this.outputHdfsDir, String.class,
        //            ProfilingEveResultRecord.class, ProfilingEveOutputFormat.class, this.jcf);
        //}


        ///*
        //Input:
        // key: sampleID
        // value: ProfilingResultRecord (no relative abundance)
        // partition: sampleID + clusterName
        // */
        //// get abundance file path which will be used to save rdd file
        //List<String> outputFilePathsList = relAbunResultRDD
        //        .mapPartitionsWithIndex(new ProfilingPathGenerateFunction(jscontext.appName(), this.outputHdfsDir, this.analysisMode), true)
        //        .collect();
        relAbunResultRDD.unpersist();
        List<String> outputFilePathsList = new ArrayList<>(2);
        outputFilePathsList.add(this.outputHdfsDir + "/" + this.jscontext.appName() + "-Profiling-SAMPLE_*.abundance.evaluation");
        return outputFilePathsList;
    }

    /**
     * Choosing proper profiling pipeline.
     *
     * @return ProfilingMethodBase New profiling pipeline instance of selected software.
     */
    private ProfilingMethodBase getProfilingMethod(){
        if(this.pipeline.equals("comg")){
            return new COMGProfilingMethod2(this.metasOpt, this.jscontext);
        } else if (this.pipeline.equals("metaphlan")){
            return new METAPHLANProfilingMethod(this.metasOpt, this.jscontext);
        } else {
            return new COMGProfilingMethod2(this.metasOpt, this.jscontext);
        }
    }
}
