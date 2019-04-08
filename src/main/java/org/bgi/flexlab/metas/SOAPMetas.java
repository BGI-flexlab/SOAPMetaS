package org.bgi.flexlab.metas;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.alignment.AlignmentProcessMS;
import org.bgi.flexlab.metas.profiling.ProfilingProcessMS;

import java.util.List;

/**
 * ClassName: SOAPMetas
 * Description: Entry class of the Tool.
 *
 * @author heshixu@genomics.cn
 */

public class SOAPMetas {

    private static final Logger LOG = LogManager.getLogger(SOAPMetas.class);

    public static void main(String[] args){

        SparkConf sconf = new SparkConf().setAppName("SOAPMetas-" + System.nanoTime());
                //.set("spark.eventLog.dir", "/tmp/SOAPMetas/spark-events");
        JavaSparkContext jsc = new JavaSparkContext(sconf);
        //jsc.hadoopConfiguration().set("hadoop.tmp.dir", "/tmp/SOAPMetas/hadoop");

        // Options initialize
        MetasOptions metasOptions = new MetasOptions(args);

        //Alignment process
        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start initializing alignment process.");
        AlignmentProcessMS alignmentMS = new AlignmentProcessMS(metasOptions, jsc);

        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start running alignment process.");
        // Output list format:
        // readGroupID    outputHDFSDir/alignment/<appId>-RDDPart<index>-<readGroupID>.sam
        List<String> alignmentOutputList = alignmentMS.runAlignment();
        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete multiple sample alignment process.");
        System.exit(0);

        if (metasOptions.isMergeOutputSamBySample()){
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Merging of sam is not supported in current version.");
            //LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start merge SAM output.");
            //LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete merging SAM output.");
        }

        //GC Training control
        //if gc training, no profiling process (standard data)
        if (metasOptions.isGCBiasTrainingMode()){
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start training GC bias correction model.");

            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Comoplete training GC Bias correctiong model, the model file is " + metasOptions.getGcBiasTrainingOutputFile() + " . Exit program.");
        }


        //ProfilingProcess
        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start initializing profiling process.");
        ProfilingProcessMS profilingMS = new ProfilingProcessMS(metasOptions, jsc);

        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start running profiling process.");
        // Output list format:
        // outputHDFSDir/profiling/<appID>-Profiling-<readGroupID>.abundance[.evaluation]
        List<String> profilingOutputList = profilingMS.runProfilingProcess(alignmentOutputList);
        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete profiling process.");

        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Output profiling results file is following:");
        for (String outPath: profilingOutputList){
            LOG.info("\t\tFile path: " + outPath);
        }
        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete analysis.");

        // delete all temp file?
        System.exit(0);
    }
}
