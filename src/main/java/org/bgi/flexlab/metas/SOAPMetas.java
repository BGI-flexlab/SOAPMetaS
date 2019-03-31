package org.bgi.flexlab.metas;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    public static void main(String[] args){
        Log LOG = LogFactory.getLog("SOAPMetas");

        SparkConf sparkConf = new SparkConf().setAppName("SOAPMetas");
                //.set("spark.local.dir", "/tmp/SOAPMetas/spark")
                //.set("spark.eventLog.dir", "/tmp/SOAPMetas/spark-events");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //jsc.hadoopConfiguration().set("hadoop.tmp.dir", "/tmp/SOAPMetas/hadoop");

        // Options initialize
        MetasOptions metasOptions = new MetasOptions(args);

        System.exit(0);

        //Alignment process
        LOG.info("Start initializing alignment process.");
        AlignmentProcessMS alignmentMS = new AlignmentProcessMS(metasOptions, jsc);

        LOG.info("Start running alignment process.");
        // Output list format:
        // readGroupID    outputHDFSDir/alignment/<appId>-RDDPart<index>-<readGroupID>.sam
        List<String> alignmentOutputList = alignmentMS.runAlignment();
        LOG.info("Complete multiple sample alignment process.");

        if (metasOptions.isMergeOutputSamBySample()){
            LOG.info("Start merge SAM output.");

            LOG.info("Complete merging SAM output.");
        }

        //GC Training control
        //if gc training, no profiling process (standard data)
        if (metasOptions.isGCBiasTrainingMode()){
            LOG.info("Start training GC bias correction model.");

            LOG.info("Comoplete training GC Bias correctiong model, the model file is " + metasOptions.getGcBiasTrainingOutputFile() + " . Exit program.");
        }


        //ProfilingProcess
        LOG.info("Start initializing profiling process.");
        ProfilingProcessMS profilingMS = new ProfilingProcessMS(metasOptions, jsc);

        LOG.info("Start running profiling process.");
        // Output list format:
        // outputHDFSDir/profiling/<appID>-Profiling-<readGroupID>.abundance[.evaluation]
        List<String> profilingOutputList = profilingMS.runProfilingProcess(alignmentOutputList);
        LOG.info("Complete profiling process.");

        LOG.info("Output profiling results file is following:");
        for (String outPath: profilingOutputList){
            LOG.info("\t\tFile path: " + outPath);
        }
        LOG.info("Complete analysis.");
        System.exit(0);
    }
}
