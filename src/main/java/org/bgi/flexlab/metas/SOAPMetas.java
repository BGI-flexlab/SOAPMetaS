package org.bgi.flexlab.metas;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMTextHeaderCodec;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.alignment.AlignmentProcessMS;
import org.bgi.flexlab.metas.data.mapreduce.input.sam.HdfsHeaderLineReader;
//import org.bgi.flexlab.metas.profiling.ProfilingNewProcessMS;
import org.bgi.flexlab.metas.profiling.ProfilingNewProcessMS2;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasTrainingProcess;

import java.io.*;
import java.util.List;

/**
 * ClassName: SOAPMetas
 * Description: Entry class of the Tool.
 *
 * TODO: 如果是 se 的数据，采用 pe 的参数会发生什么
 *
 * @author heshixu@genomics.cn
 */

public class SOAPMetas {

    private static final Logger LOG = LogManager.getLogger(SOAPMetas.class);

    public static void main(String[] args){

        long ALIGNMENTJSC_TIME = 0;
        long ALIGNMENT_TIME = 0;
        long PROFILINGJSC_TIME = 0;
        long PROFILING_TIME = 0;
        long STARTTIME = System.currentTimeMillis();

        SparkConf sconf = new SparkConf().setAppName("SOAPMetas-" + System.nanoTime());

        // User configuration: executor-memory, num-executor/executorCores, task_cpus
        // executor_number = sc.statusTracker.getExecutorInfos.length - 1
        // yarn executor number: sc.getConf.get("spark.executor.instances", "-1")

        // these four values can be obtain before jsc starts, but won't exist if user doesn't set when submit-submit
        //System.out.println(sconf.get("spark.executor.instances", "-1")); // --num-executor (only for yarn)
        //System.out.println(sconf.get("spark.task.cpus", "-1"));
        //System.out.println(sconf.get("spark.executor.cores", "-1")); // --executor-cores
        //System.out.println(sconf.get("spark.executor.memory", "-1")); // --executor-memory

        //System.exit(0);
        String exeNum = sconf.get("spark.executor.instances", "-1");
        String exeMem = sconf.get("spark.executor.memory", "-1");
        String taskCpus = sconf.get("spark.task.cpus", "-1");
        String exeCores = sconf.get("spark.executor.cores", "-1");

        // Options initialize
        MetasOptions metasOptions = new MetasOptions(args);
        //try {
        //    DataUtils.createFolder(jsc.hadoopConfiguration(), metasOptions.getHdfsOutputDir());
        //} catch (IOException e) {
        //    LOG.error("[SOAPMetas::" + SOAPMetas.class.getName() + "] Fail to create output directory: " +
        //            metasOptions.getHdfsOutputDir());
        //    jsc.close();
        //    System.exit(1);
        //}

        if (exeNum.equals("-1")) {
            sconf.set("spark.executor.instances", metasOptions.getAlignmentExeNumber());
        } else {
            LOG.warn("[SOAPMetas::" + SOAPMetas.class.getName() + "] The executor number is set by spark-submit, we recommend to set it by SOAPMetaS \"--align-executor-number\" and \"--prof-executor-number\"");
        }

        if (exeMem.equals("-1")) {
            sconf.set("spark.executor.memory", metasOptions.getAlignmentExeMemory());
        } else {
            LOG.warn("[SOAPMetas::" + SOAPMetas.class.getName() + "] The executor memory is set by spark-submit, we recommend to set it by SOAPMetaS \"--align-executor-memory\" and \"--prof-executor-memory\"");
        }
        
        if (taskCpus.equals("-1") && exeCores.equals("-1")) {
            sconf.set("spark.executor.cores", metasOptions.getAlignmentExeCores()).set("spark.task.cpus", metasOptions.getAlignmentExeCores());
        } else {
            LOG.warn("[SOAPMetas::" + SOAPMetas.class.getName() + "] We recommend setting task cpus and executor cores by SOAPMetaS option for better control of performance. Since the alignment process only support one-task-one-executor, we readjust both \"spark.executor.cores\" and \"spark.task.cpus\" to the larger provided value in alignment process.");
            String cpuValue = Integer.toString(Math.max(Math.max(Integer.parseInt(taskCpus), Integer.parseInt(exeCores)), Integer.parseInt(metasOptions.getAlignmentExeCores())));
            sconf.set("spark.executor.cores", cpuValue).set("spark.task.cpus", cpuValue);
        }

        //sconf.set("spark.executor.cores", "1").set("spark.task.cpus", "1")
        //        .set("spark.executor.memory", metasOptions.getAlignmentExeMemory())
        //        .set("spark.executor.instances", metasOptions.getAlignmentExeNumber());
        //.set("spark.eventLog.dir", "/tmp/SOAPMetas/spark-events");


        JavaSparkContext jsc = new JavaSparkContext(sconf);
        //jsc.hadoopConfiguration().set("metas.application.name", jsc.appName());

        ALIGNMENTJSC_TIME = System.currentTimeMillis() - STARTTIME;
        STARTTIME = System.currentTimeMillis();

        List<String> alignmentOutputList = null;

        if (metasOptions.doAlignment()) {
            //Alignment process
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start initializing alignment process.");
            AlignmentProcessMS alignmentMS = new AlignmentProcessMS(metasOptions, jsc);

            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start running alignment process.");
            // Output list format:
            // readGroupID    outputHDFSDir/alignment/<appId>-RDDPart<index>-<readGroupID>.sam
            alignmentOutputList = alignmentMS.runAlignment();
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete multiple sample alignment process.");

            ALIGNMENT_TIME = System.currentTimeMillis() - STARTTIME;
            STARTTIME = System.currentTimeMillis();

            if (metasOptions.mergeSamBySample()) {
                LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Merging of sam is not supported in current version.");
                //LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start merge SAM output.");
                //LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete merging SAM output.");
            }
        } else {
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Skip alignment process.");
        }

        //create multi-header
        //SAMFileHeader samFileHeader = getHeader(new Path("file:///hwfssz1/BIGDATA_COMPUTING/huangzhibo/workitems/SOAPMeta/SRS014287_header.sam"), jsc.hadoopConfiguration());

        //GC Training control
        //if gc training, no profiling process (standard data)
        if (metasOptions.isGCBiasTrainingMode()){

            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start training GC bias recalibration model.");
            GCBiasTrainingProcess modelTraining = new GCBiasTrainingProcess(metasOptions);

            if (alignmentOutputList == null){
                modelTraining.trainGCBiasModel(jsc, metasOptions.getSAMSampleList());
            } else {
                modelTraining.trainGCBiasModel(jsc, alignmentOutputList.iterator());
            }


            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Comoplete training GC Bias recalibration model, " +
                    "the model file is " + metasOptions.getGcBiasModelOutput() + " . Exit program.");
            jsc.close();
            System.exit(0);
        }

        List<String> profilingOutputList;

        if (metasOptions.doProfiling()) {
            if (!jsc.sc().isStopped()) {
                jsc.stop();
            }
            if (exeNum.equals("-1")) {
                sconf.set("spark.executor.instances", metasOptions.getProfilingExeNumber());
            } else {
                LOG.warn("[SOAPMetas::" + SOAPMetas.class.getName() + "] The executor number is set by spark-submit, we recommend to set it by SOAPMetaS \"--align-executor-number\" and \"--prof-executor-number\"");
            }
            if (exeMem.equals("-1")) {
                sconf.set("spark.executor.memory", metasOptions.getProfilingExeMemory());
            } else {
                LOG.warn("[SOAPMetas::" + SOAPMetas.class.getName() + "] The executor memory is set by spark-submit, we recommend to set it by SOAPMetaS \"--align-executor-memory\" and \"--prof-executor-memory\"");
            }
            if (exeCores.equals("-1")) {
                sconf.set("spark.executor.cores", metasOptions.getProfilingExeCores());
            } else {
                sconf.set("spark.executor.cores", exeCores);
            }
            if (taskCpus.equals("-1")) {
                sconf.set("spark.task.cpus", metasOptions.getProfilingTaskCpus());
            } else {
                sconf.set("spark.task.cpus", taskCpus);
            }
            jsc = new JavaSparkContext(sconf);

            PROFILINGJSC_TIME = System.currentTimeMillis() - STARTTIME;
            STARTTIME = System.currentTimeMillis();

            //ProfilingProcess
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start initializing profiling process.");
            ProfilingNewProcessMS2 profilingMS = new ProfilingNewProcessMS2(metasOptions, jsc);
            //ProfilingNewProcessMS profilingMS = new ProfilingNewProcessMS(metasOptions, jsc);
            // Output list format:
            // outputHDFSDir/profiling/<appID>-Profiling-<readGroupID>.abundance[.evaluation]
            if (alignmentOutputList != null) {
                profilingMS.processInitialize(alignmentOutputList);
            } else {
                profilingMS.processInitialize(metasOptions.getSAMSampleList());
            }

            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start running profiling process.");
            profilingOutputList = profilingMS.runProfilingProcess();

            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete profiling process.");

            if (profilingOutputList == null){
                LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] No output file list. Please " +
                        "check log file and output directory.");
            } else {
                LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Output profiling result files are: " +
                        StringUtils.join(profilingOutputList, ','));
            }
            PROFILING_TIME = System.currentTimeMillis() - STARTTIME;
        } else if (alignmentOutputList != null) {

            File tmpDir = new File(metasOptions.getDriverTmpDir());
            if (! tmpDir.exists()){
                tmpDir.mkdirs();
            }

            String multiSamListFile = tmpDir + "/" + "tmp-multiSampleSAMList-" + jsc.appName();

            File samList = new File(multiSamListFile);

            FileOutputStream fos1;
            BufferedWriter bw1;

            try {
                fos1 = new FileOutputStream(samList);
                bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

                for (String samInput: alignmentOutputList) {
                    bw1.write(samInput);
                    bw1.newLine();
                }

                bw1.close();
                fos1.close();
            } catch (IOException e){
                LOG.error("[SOAPMetas::" + SOAPMetas.class.getName() + "] MultiSampleFileList IO error. " + e.toString());
            }
        }

        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete analysis.");

        // delete all temp file?

        jsc.close();
        System.out.println("[SOAPMetas::" + SOAPMetas.class.getName() + "] Alignment JSC creation time: " + Double.toString((double) ALIGNMENTJSC_TIME/1000) + "s");
        System.out.println("[SOAPMetas::" + SOAPMetas.class.getName() + "] Alignment time: " + Double.toString((double) ALIGNMENT_TIME/1000) + "s");
        System.out.println("[SOAPMetas::" + SOAPMetas.class.getName() + "] Profiling JSC creation time: " + Double.toString((double) PROFILINGJSC_TIME/1000) + "s");
        System.out.println("[SOAPMetas::" + SOAPMetas.class.getName() + "] Profiling time: " + Double.toString((double) PROFILING_TIME/1000) + "s");
        System.exit(0);
    }

    public static SAMFileHeader getHeader(Path headerPath, Configuration conf) {

        SAMFileHeader header = null;
        try {
            HdfsHeaderLineReader reader = new HdfsHeaderLineReader(headerPath, conf);
            SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
            header = codec.decode(reader, null);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

        return header;
    }

}
