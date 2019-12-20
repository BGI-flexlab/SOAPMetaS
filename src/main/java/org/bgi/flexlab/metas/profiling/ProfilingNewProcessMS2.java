package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.mapreduce.input.sam.MetaSamSource;
import org.bgi.flexlab.metas.data.mapreduce.input.sam.SAMFileHeaderFactory;
import org.bgi.flexlab.metas.data.mapreduce.output.profiling.ProfilingOutputFormat;
import org.bgi.flexlab.metas.data.mapreduce.output.profiling.RelativeAbundanceFunction;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDPartitioner;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.SAMMultiSampleList;
import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordIdentityFilter;
import org.bgi.flexlab.metas.profiling.profilingmethod.COMGProfilingMethod2;
import org.bgi.flexlab.metas.profiling.profilingmethod.MEPHNewProfilingMethod;
import org.bgi.flexlab.metas.profiling.profilingmethod.MEPHProfilingMethod;
import org.bgi.flexlab.metas.profiling.profilingmethod.ProfilingMethodBase;
import org.bgi.flexlab.metas.util.DataUtils;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
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

    private SAMMultiSampleList samMultiSampleList; //需要简化
    private int numPartitionEachSample;

    private boolean doIdentityFiltering = false;
    private MetasSAMRecordIdentityFilter identityFilter;

    private String tmpDir;
    private String outputHdfsDir;

    private JobConf jcf;

    private String referenceMatrixFilePath;

    private String speciesGenomeGCFilePath;

    private boolean skipRelAbun = false;

    public ProfilingNewProcessMS2(final MetasOptions options, final JavaSparkContext context){
        this.metasOpt = options;
        this.jscontext = context;
        this.processConstruct();

        referenceMatrixFilePath = options.getReferenceMatrixFilePath();

        speciesGenomeGCFilePath = options.getSpeciesGenomeGCFilePath();
    }

    private void processConstruct(){

        this.numPartitionEachSample = Math.max(this.metasOpt.getNumPartitionEachSample(), 1);

        this.pipeline = this.metasOpt.getProfilingPipeline();
        this.analysisMode = this.metasOpt.getProfilingAnalysisMode();
        this.seqMode = this.metasOpt.getSequencingMode();
        if (this.pipeline.equals("meph")) {
            this.skipRelAbun = true;
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

        String filePath = samMultiSampleList.getAllSAMFilePath();
        LOG.info("[SOAPMetas::" + ProfilingNewProcessMS2.class.getName() + "] All input sam file paths: " + filePath);

        HashMap<String, Integer> sampleIDs = samMultiSampleList.getSampleIDbySampleName();

        ProfilingMethodBase profilingMethod = getProfilingMethod();


        final SAMFileHeader header = new SAMFileHeaderFactory().createHeader(referenceMatrixFilePath, sampleIDs.keySet());

        // TODO  根据 metasOpt.getReferenceMatrixFilePath() 和 samMultiSampleList 创建header
//        final SAMFileHeader header = SOAPMetas.getHeader(new Path("file:///hwfssz1/BIGDATA_COMPUTING/huangzhibo/workitems/SOAPMeta/SRS014287_header.sam"), jscontext.hadoopConfiguration());

        profilingMethod.setSampleIDbySampleName(sampleIDs);

        MetaSamSource samSource = new MetaSamSource();
        JavaRDD<SAMRecord> reads = null;
        try {
            reads = samSource.getReads(jscontext, filePath, 1024*1024*256, header);
        } catch (IOException e) {
            e.printStackTrace();
        }

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
                .partitionBy(new SampleIDPartitioner(sampleIDs.size() + 1))
                .mapPartitionsToPair(new RelativeAbundanceFunction(this.skipRelAbun), true);

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
        } else if (this.pipeline.equals("meph")) {
            return new MEPHProfilingMethod(this.metasOpt, this.jscontext);
        } else if (this.pipeline.equals("mephn")) {
            return new MEPHNewProfilingMethod(this.metasOpt, this.jscontext);
        } else {
            return new COMGProfilingMethod2(this.metasOpt, this.jscontext);
        }
    }
}
