package org.bgi.flexlab.metas.alignment;

/**
 * ClassName: AlignmentMethodBase
 * Description: Basic abstract class of single/paired end alignment method.
 *
 * @author heshixu@genomics.cn
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

public class AlignmentMethodBase implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final Logger LOG = LogManager.getLogger(AlignmentMethodBase.class);

    protected String appName;
    protected String appId;
    protected String tmpDir;
    protected String outDir;
    protected AlignmentToolWrapper toolWrapper;

    /**
     * Constructor for this class
     *
     * *Changes:
     *  + change Bwa object to toolWrapper object
     *
     * @brief This constructor creates a AlignmentMethod object to process in each one of the mappers
     * @param context The SparkContext to use
     * @param toolWrapper The alignment tool object used to perform the alignment
     */
    public AlignmentMethodBase(SparkContext context, AlignmentToolWrapper toolWrapper) {

        this.appId = context.applicationId();
        this.appName = context.appName();
        this.toolWrapper = toolWrapper;
        this.tmpDir = this.toolWrapper.getTmpDir();
        this.outDir = this.toolWrapper.getOutputHdfsDir();

        this.LOG.info("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] " + this.appId + " - " + this.appName);
    }

    /**
     * Function that performs an actual alignment
     * @param outputSamFileName String representing the output sam location
     * @param fastqFileName1 String representing the first of the FASTQ files
     * @param fastqFileName2 String representing the second of the FASTQ files. If single reads, it can be empty.
     */
    public void alignReads(String outputSamFileName, String fastqFileName1, String fastqFileName2) {
        // First, the two input FASTQ files are set
        this.toolWrapper.setInputFile(fastqFileName1);

        if (this.toolWrapper.isPairedReads()) {
            toolWrapper.setInputFile2(fastqFileName2);
        }

        this.toolWrapper.setOutputFile(this.tmpDir + "/" +outputSamFileName);

        //We run BWA with the corresponding options set
        this.toolWrapper.run();
    }

    /**
     * Function that copy local sam results to HDFS
     *
     * *Changes:
     *  + Change Bwa object to toolWrapper object.
     *
     * @param outputSamFileName The output where the final results will be stored
     * @return An ArrayList containing all the file locations
     */
    public ArrayList<String> copyResults(String outputSamFileName, String readGroupID, String smTag) {
        ArrayList<String> returnedValues = new ArrayList<>(2);

        LOG.info("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] " + this.appId + " - " +
                this.appName + " Copy output sam files to output directory.");

        try {
            //if (outputDir.startsWith("hdfs")) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            fs.copyFromLocalFile(true, true,
                    new Path(this.toolWrapper.getOutputFile()),
                    new Path(this.toolWrapper.getOutputHdfsDir() + "/" + outputSamFileName)
            );
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] Original alignment result file: "
                    + this.toolWrapper.getOutputFile() + ". " + e.toString());
        }

        //// Delete the old results file
        //File localSam = new File(this.toolWrapper.getOutputFile());
        //if (!localSam.delete()){
        //    LOG.warn("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] Fail to delete temp SAM output file: "
        //            + this.toolWrapper.getOutputFile());
        //}

        if (smTag != null) {
            returnedValues.add(readGroupID + '\t' + smTag + '\t' + this.toolWrapper.getOutputHdfsDir() + "/" + outputSamFileName);
        } else {
            returnedValues.add("NORGID\tNOSMTAG\t" + this.toolWrapper.getOutputHdfsDir() + "/" + outputSamFileName);
        }

        return returnedValues;
    }

    /**
     * @param readBatchID Identification for the sam file
     * @return A String for the sam file name
     */
    public String getOutputSamFilename(Integer readBatchID) {
        return this.appName + "-" + this.appId + "-" + readBatchID + ".sam";
    }

    /**
     *
     * @param readBatchID Identification for the sam file
     * @param fastqFileName1 First of the FASTQ files
     * @param fastqFileName2 Second of the FASTQ files
     * @return
     */
    public ArrayList<String> runAlignmentProcess(Integer readBatchID, String fastqFileName1, String fastqFileName2) {
        //The output filename (without the tmp directory)
        String outputSamFileName = this.getOutputSamFilename(readBatchID);
        this.alignReads(outputSamFileName, fastqFileName1, fastqFileName2);

        // Copy the result to HDFS
        return this.copyResults(outputSamFileName, "NORGID", "NOSMTAG");
    }
}
