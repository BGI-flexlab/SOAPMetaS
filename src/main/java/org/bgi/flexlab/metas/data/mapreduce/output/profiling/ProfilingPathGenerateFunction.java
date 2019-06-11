package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function2;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;


/**
 * ClassName: ProfilingPathGenerateFunction
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingPathGenerateFunction implements Serializable,
        Function2<Integer, Iterator<Tuple2<String, ProfilingResultRecord>>,  Iterator<String>> {

    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(ProfilingPathGenerateFunction.class); // The LOG

    final private String outputDir;
    final private ProfilingAnalysisMode profilingAnalysisMode;
    final private String appName;

    public ProfilingPathGenerateFunction(String appName, String outputHdfsDir, ProfilingAnalysisMode mode){
        this.outputDir = outputHdfsDir;
        this.profilingAnalysisMode = mode;
        this.appName = appName;
    }

    @Override
    public Iterator<String> call(Integer index,
                                 Iterator<Tuple2<String, ProfilingResultRecord>> tuple2Iterator) {

        String smTag;
        Tuple2<String, ProfilingResultRecord> firstTuple;

        LOG.trace("[SOAPMetas::" + ProfilingPathGenerateFunction.class.getName() + "] Current Partition index: " + index);

        if (tuple2Iterator.hasNext()){
            firstTuple = tuple2Iterator.next();
            smTag = firstTuple._2.getSmTag();
            LOG.info("[SOAPMetas::" + ProfilingPathGenerateFunction.class.getName() + "] Current sample tag: " + smTag);
        } else {
            LOG.trace("[SOAPMetas::" + ProfilingPathGenerateFunction.class.getName() + "] Empty partition index: " + index);
            return new ArrayList<String>(0).iterator();
        }

        ArrayList<String> outputPaths = new ArrayList<>(2);
        String outputProfilingFile;

        if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.PROFILE)) {
            outputProfilingFile = this.outputDir + "/" + this.appName + "-Profiling-SAMPLE_" + smTag + ".abundance";
        } else if(this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
            outputProfilingFile = this.outputDir + "/" + this.appName + "-Profiling-SAMPLE_" + smTag + ".abundance.evaluation";
        } else {
            LOG.error("[SOAPMetas::" + ProfilingPathGenerateFunction.class.getName() + "] Profiling Analysis Mode has wrong value");
            outputProfilingFile = this.outputDir + "/" + this.appName + "-Profiling-SAMPLE_" + smTag + ".abundance";
        }

        outputPaths.add(outputProfilingFile);

        Tuple2<String, ProfilingResultRecord> newTuple;
        while (tuple2Iterator.hasNext()) {
            newTuple = tuple2Iterator.next();
            if (!newTuple._2.getSmTag().equals(smTag)) {
                LOG.warn("[SOAPMetas::" + ProfilingPathGenerateFunction.class.getName() + "] Sample is not partitioned correctly. Current sample Tag: "
                        + newTuple._2.getSmTag() +
                        " First sample of current partition: " + smTag);
            }
        }

        tuple2Iterator = null;

        return outputPaths.iterator();
    }
}
