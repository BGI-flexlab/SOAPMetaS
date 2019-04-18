package org.bgi.flexlab.metas;

import org.apache.commons.lang3.StringUtils;
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
 * TODO: 更改重要的 assert 语句。
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

        //GC Training control
        //if gc training, no profiling process (standard data)
        if (metasOptions.isGCBiasTrainingMode()){
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start training GC bias correction model.");

            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Comoplete training GC Bias correctiong model, " +
                    "the model file is " + metasOptions.getGcBiasTrainingOutputFile() + " . Exit program.");
            jsc.close();
            System.exit(0);
        }

        List<String> profilingOutputList;

        if (metasOptions.doProfiling()) {
            //ProfilingProcess
            LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Start initializing profiling process.");
            ProfilingProcessMS profilingMS = new ProfilingProcessMS(metasOptions, jsc);
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
        }

        LOG.info("[SOAPMetas::" + SOAPMetas.class.getName() + "] Complete analysis.");

        // delete all temp file?

        jsc.close();
        System.exit(0);
    }
}
