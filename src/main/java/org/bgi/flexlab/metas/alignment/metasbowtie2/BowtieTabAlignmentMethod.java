package org.bgi.flexlab.metas.alignment.metasbowtie2;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;
import org.bgi.flexlab.metas.alignment.AlignmentMethodBase;
import org.bgi.flexlab.metas.alignment.AlignmentToolWrapper;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * ClassName: BowtieTabAlignmentMethod
 * Description: The class is used for alignment of bowtie2 tab5 format (para: --tab5).
 *
 * @author heshixu@genomics.cn
 */

public class BowtieTabAlignmentMethod extends AlignmentMethodBase
        implements Serializable, Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<String>> {

    private static final long serialVersionUID = 1L;

    public BowtieTabAlignmentMethod(SparkContext context, AlignmentToolWrapper toolWrapper){
        super(context, toolWrapper);
        ((MetasBowtie) toolWrapper).setTab5Mode();
    }

    private ArrayList<String> runMultiSampleAlignment(String readGroupID, String smTag, File tab5File, File outSamFile, File logFile){
        this.toolWrapper.setInputFile(tab5File.getAbsolutePath());
        this.toolWrapper.setOutputFile(outSamFile.getAbsolutePath());
        this.toolWrapper.setReadGroupID(readGroupID);
        this.toolWrapper.setSMTag(smTag);
        this.toolWrapper.setAlnLog(logFile.getAbsolutePath());

        this.toolWrapper.run();

        return this.copyResults(outSamFile.getName(), logFile.getName(), readGroupID, smTag);
    }

    /**
     *
     * @param index
     * @param elementIter
     * @return
     * @throws Exception
     */
    @Override
    public Iterator<String> call(Integer index, Iterator<Tuple2<String, String>> elementIter) {

        LOG.trace("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Current Partition index: " + index);

        if (!elementIter.hasNext()){
            LOG.trace("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Empty partition index: " + index);
            return new ArrayList<String>(0).iterator();
        }

        ArrayList<String> returnedValues = new ArrayList<>(2);
        Tuple2<String, String> element;

        //String tab5FilePath;
        //String outSamFileName;
        //String logFile;
        String readGroupID;
        String smTag;
        String partRGSM;

        element = elementIter.next();
        partRGSM = element._1;

        String[] temp;
        temp = StringUtils.split(partRGSM, '\t');
        readGroupID = temp[0];
        smTag = temp[1];
        temp = null;

        //tab5FilePath = this.tmpDir + '/' + this.appId + "-SOAPMetas-RDDPart" + index + "-RG_" + readGroupID + "-SM_" + smTag + ".tab5";
        //outSamFileName = this.appId + "-SOAPMetas-RDDPart" + index + "-RG_" + readGroupID + "-SM_" + smTag + ".sam";
        //logFile = this.appId + "-SOAPMetas-RDDPart" + index + "-RG_" + readGroupID + "-SM_" + smTag + "-alignment.log";

        // Set writable and deleteOnExit so that files can be deleted by group accounts if the Application is interrupted.
        File outSamFile = new File(this.tmpDir + "/" + this.appId + "-SOAPMetas-RDDPart" + index + "-RG_" + readGroupID + "-SM_" + smTag + ".sam");
        outSamFile.setWritable(true, false);
        outSamFile.deleteOnExit();
        File logFile = new File(this.tmpDir + '/' + this.appId + "-SOAPMetas-RDDPart" + index + "-RG_" + readGroupID + "-SM_" + smTag + "-alignment.log");
        logFile.setWritable(true, false);
        logFile.deleteOnExit();

        File tab5File = new File(this.tmpDir + '/' + this.appId + "-SOAPMetas-RDDPart" + index + "-RG_" + readGroupID + "-SM_" + smTag + ".tab5");
        tab5File.setWritable(true, false);
        tab5File.deleteOnExit();
        FileOutputStream fos1;
        BufferedWriter bw1;

        LOG.info("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Writing input file for bowtie2: " + tab5File.getAbsolutePath());

        try {
            fos1 = new FileOutputStream(tab5File);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            // Write first line to file.
            bw1.write(element._2);
            bw1.newLine();

            while (elementIter.hasNext()) {
                element = elementIter.next();

                if (!element._1.equals(partRGSM)) {
                    LOG.warn("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] RG:" + readGroupID +
                            " SM:" + smTag + ". Omit wrong partitioned sequence in part (" + element._1 + "): "
                            + StringUtils.split(element._2, '\t')[0]);
                    continue;
                }

                bw1.write(element._2);
                bw1.newLine();
            }

            bw1.close();

            //We do not need the input data anymore, as it is written in a local file
            elementIter = null;

            // This is where the actual local alignment takes place
            returnedValues = this.runMultiSampleAlignment(readGroupID, smTag, tab5File, outSamFile, logFile);

            // Delete the temporary file, as results have been copied to the specified output directory
            if (tab5File.delete()) {
                LOG.debug("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Delete temp tab5 " +
                        "file: " + tab5File.getAbsolutePath());
            } else {
                LOG.warn("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Fail to delete " +
                        "temp tab5 file: " + tab5File.getAbsolutePath());
            }
        } catch (FileNotFoundException e){
            LOG.error("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Can't find temp tab5 file " +
                    tab5File.getAbsolutePath() + " . " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Fail to write temp tab5 " +
                    "file: " + tab5File.getAbsolutePath() + " . " + e.toString());
        }

        return returnedValues.iterator();

    }
}
