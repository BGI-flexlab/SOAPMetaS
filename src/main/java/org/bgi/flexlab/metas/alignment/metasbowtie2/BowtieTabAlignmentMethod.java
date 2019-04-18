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

    private ArrayList<String> runMultiSampleAlignment(String readGroupID, String tab5FileName, String outSamFileName){
        this.toolWrapper.setInputFile(tab5FileName);
        this.toolWrapper.setOutputFile(this.tmpDir + "/" + outSamFileName);
        this.toolWrapper.setReadGroupID(readGroupID);

        this.toolWrapper.run();

        return this.copyResults(outSamFileName, readGroupID);
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

        String tab5FileName;
        String outSamFileName;
        String readGroupID;

        LOG.trace("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Current Partition index: " + index);

        if (!elementIter.hasNext()){
            LOG.trace("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Empty partition index: " + index);
            return new ArrayList<String>(0).iterator();
        }

        ArrayList<String> returnedValues = new ArrayList<>();
        Tuple2<String, String> element;

        element = elementIter.next();
        readGroupID = element._1;

        tab5FileName = this.tmpDir + "/" + this.appId + "-RDDPart" + index + "-" + readGroupID + ".tab5";
        outSamFileName = this.appId + "-RDDPart" + index + "-" + readGroupID + ".sam";

        LOG.info("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Writing input file for bowtie2: " + tab5FileName);

        File tab5File = new File(tab5FileName);
        FileOutputStream fos1;
        BufferedWriter bw1;


        try {
            fos1 = new FileOutputStream(tab5File);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            // Write first line to file.
            bw1.write(element._2);
            bw1.newLine();

            while (elementIter.hasNext()) {
                element = elementIter.next();

                if (!element._1.equals(readGroupID)) {
                    LOG.warn("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] ReadGroup: " + readGroupID + ". Omit wrong partitioned sequence: "
                            + StringUtils.split(element._2, '\t')[0] + " of group " + element._1);
                    continue;
                }

                bw1.write(element._2);
                bw1.newLine();
            }

            bw1.close();

            //We do not need the input data anymore, as it is written in a local file
            elementIter = null;

            // This is where the actual local alignment takes place
            returnedValues = this.runMultiSampleAlignment(readGroupID, tab5FileName, outSamFileName);

            // Delete the temporary file, as results have been copied to the specified output directory
            if (tab5File.delete()) {
                LOG.debug("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Delete temp tab5 " +
                        "file: " + tab5FileName);
            } else {
                LOG.warn("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Fail to delete " +
                        "temp tab5 file: " + tab5FileName);
            }
        } catch (FileNotFoundException e){
            LOG.error("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Can't find temp tab5 file " +
                    tab5FileName + " . " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + BowtieTabAlignmentMethod.class.getName() + "] Fail to write temp tab5 " +
                    "file: " + tab5FileName + " . " + e.toString());
        }

        return returnedValues.iterator();

    }
}
