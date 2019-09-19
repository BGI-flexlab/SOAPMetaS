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
import org.bgi.flexlab.metas.profiling.ProfilingNewProcessMS;
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

        SparkConf sconf = new SparkConf().setAppName("SOAPMetas-" + System.nanoTime());
        sconf.set("spark.network.timeout", "600");
                //.set("spark.eventLog.dir", "/tmp/SOAPMetas/spark-events");
        JavaSparkContext jsc = new JavaSparkContext(sconf);
        //jsc.hadoopConfiguration().set("metas.application.name", jsc.appName());

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

            if (metasOptions.mergeSamBySample()) {
                LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Merging of sam is not supported in current version.");
                //LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start merge SAM output.");
                //LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete merging SAM output.");
            }
        }


        //create multi-header
//        SAMFileHeader samFileHeader = getHeader(new Path("file:///hwfssz1/BIGDATA_COMPUTING/huangzhibo/workitems/SOAPMeta/SRS014287_header.sam"), jsc.hadoopConfiguration());


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
            //ProfilingProcess
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start initializing profiling process.");
            ProfilingNewProcessMS profilingMS = new ProfilingNewProcessMS(metasOptions, jsc);
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
        } else if (alignmentOutputList != null) {
            String multiSamListFile = metasOptions.getAlignmentTmpDir() + "/" + "tmp-multiSampleSAMList-" + jsc.appName();

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
