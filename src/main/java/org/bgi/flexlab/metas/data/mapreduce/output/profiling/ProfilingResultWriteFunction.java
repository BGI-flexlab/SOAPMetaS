package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * ClassName: ProfilingResultWriteFunction
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class ProfilingResultWriteFunction implements Serializable,
        Function2<Integer, Iterator<Tuple2<String, ProfilingResultRecord>>,  Iterator<String>> {

    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(ProfilingResultWriteFunction.class); // The LOG

    final private String outputDir;
    final private Map<String, Double> totalAbundanceMap;
    final private ProfilingAnalysisMode profilingAnalysisMode;
    final private String appID;

    public ProfilingResultWriteFunction (SparkContext context, String outputHdfsDir, Map<String, Double> abundanceMap, ProfilingAnalysisMode mode){
        this.outputDir = outputHdfsDir;
        this.totalAbundanceMap = abundanceMap;
        this.profilingAnalysisMode = mode;
        this.appID = context.applicationId();
    }

    @Override
    public Iterator<String> call(Integer index,
                                 Iterator<Tuple2<String, ProfilingResultRecord>> tuple2Iterator) {

        String sampleID;
        String readGroupID;
        Tuple2<String, ProfilingResultRecord> firstTuple;

        LOG.trace("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Current Partition index: " + index);

        if (tuple2Iterator.hasNext()){
            firstTuple = tuple2Iterator.next();
            sampleID = firstTuple._1;
            readGroupID = firstTuple._2.getReadGroupID();
        } else {
            LOG.trace("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Empty partition index: " + index);
            return new ArrayList<String>(0).iterator();
        }

        ArrayList<String> outputPaths = new ArrayList<>(2);
        Tuple2<String, ProfilingResultRecord> newTuple;
        String outputProfilingFile;

        if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.PROFILE)) {
            outputProfilingFile = this.outputDir + "/" + this.appID + "-Sample" + sampleID + "-Profiling-" + readGroupID + ".abundance";
        } else if(this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
            outputProfilingFile = this.outputDir + "/" + this.appID + "-Sample" + sampleID + "-Profiling-" + readGroupID + ".abundance.evaluation";
        } else {
            LOG.error("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Profiling Analysis Mode has wrong value");
            outputProfilingFile = this.outputDir + "/" + this.appID + "-Sample" + sampleID + "-Profiling-" + readGroupID + ".abundance";
        }

        outputPaths.add(outputProfilingFile);

        File fileO = new File(outputProfilingFile);
        FileOutputStream fileOS;
        BufferedWriter bw;

        try {

            fileOS = new FileOutputStream(fileO);
            bw = new BufferedWriter(new OutputStreamWriter(fileOS));

            if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.PROFILE)) {

                bw.write("cluster name(marker/species),fragment number,corrected frag num,relative abundance");
                bw.newLine();
                bw.write(outputFormatProfile(firstTuple));
                bw.newLine();

                while (tuple2Iterator.hasNext()) {
                    newTuple = tuple2Iterator.next();

                    if (!newTuple._2.getReadGroupID().equals(readGroupID)) {
                        LOG.warn("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Current " +
                                "ReadGroup: " + readGroupID + " . Omit wrong partitioned record of RG-" +
                                newTuple._2.getReadGroupID() + ": " + newTuple._2.toString());
                        continue;
                    }

                    bw.write(outputFormatProfile(newTuple));
                    bw.newLine();
                }

            } else if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {

                bw.write("cluster name(marker/species),fragment number,corrected frag num,relative abundance,read Name List");
                bw.newLine();
                bw.write(outputFormatEvaluation(firstTuple));
                bw.newLine();

                while (tuple2Iterator.hasNext()) {
                    newTuple = tuple2Iterator.next();
                    if (!newTuple._2.getReadGroupID().equals(readGroupID)) {
                        LOG.warn("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Current " +
                                "ReadGroup: " + readGroupID + " . Omit wrong partitioned record of RG-" +
                                newTuple._2.getReadGroupID() + ": " + newTuple._2.toString());
                        continue;
                    }
                    bw.write(outputFormatEvaluation(newTuple));
                    bw.newLine();
                }

            } else {
                LOG.error("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Profiling Analysis Mode has wrong value.");
                bw.write("cluster name(marker/species),fragment number,corrected frag num,relative abundance");
                bw.newLine();
                bw.write("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Analysis mode wrong, no output");
                bw.newLine();
            }

            bw.close();
            fileOS.close();

        } catch (FileNotFoundException e){
            LOG.error("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Can't find file " +
                    outputProfilingFile + " . " + e.toString());
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ProfilingResultWriteFunction.class.getName() + "] Fail to write file " +
                    outputProfilingFile + " . " + e.toString());
        }

        tuple2Iterator = null;

        return outputPaths.iterator();
    }

    private String outputFormatProfile(Tuple2<String, ProfilingResultRecord> resultTuple){
        StringBuilder builder = new StringBuilder(resultTuple._2.getClusterName());
        return builder.append(",").append(resultTuple._2.getRawReadCount()).append(",")
                .append(resultTuple._2.getCorrectedReadCount()).append(",")
                .append(resultTuple._2.getAbundance()/this.totalAbundanceMap.get(resultTuple._1)).append(",")
                .toString();
    }
    private String outputFormatEvaluation(Tuple2<String, ProfilingResultRecord> resultTuple){
        StringBuilder builder = new StringBuilder(resultTuple._2.getClusterName());
        return builder.append(",").append(resultTuple._2.getRawReadCount()).append(",")
                .append(resultTuple._2.getCorrectedReadCount()).append(",")
                .append(resultTuple._2.getAbundance()/this.totalAbundanceMap.get(resultTuple._1)).append(",")
                .append(resultTuple._2.getReadNameString()).toString();
    }
}
