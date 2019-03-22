package org.bgi.flexlab.metas.alignment;

/**
 * ClassName: AlignmentMethodBase
 * Description: Basic abstract class of single/paired end alignment method.
 *
 * @author: heshixu@genomics.cn
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

public class AlignmentMethodBase implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final Log LOG = LogFactory.getLog(AlignmentMethodBase.class);

    protected String appName;
    protected String appId;
    protected String tmpDir;
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
        this.tmpDir = context.getLocalProperty("spark.local.dir");
        this.toolWrapper = toolWrapper;

        //We set the tmp dir
        if ((this.tmpDir == null || this.tmpDir == "null")
                && this.toolWrapper.getTmpDir() != null
                && !this.toolWrapper.getTmpDir().isEmpty()) {

            this.tmpDir = this.toolWrapper.getTmpDir();
        }

        if (this.tmpDir == null || this.tmpDir == "null") {
            this.tmpDir = context.hadoopConfiguration().get("hadoop.tmp.dir");
        }

        if (this.tmpDir.startsWith("file:")) {
            this.tmpDir = this.tmpDir.replaceFirst("file:", "");
        }

        File tmpFileDir = new File(this.tmpDir);

        if(!tmpFileDir.isDirectory() || !tmpFileDir.canWrite()) {
            this.tmpDir = "/tmp/";
        }


        this.LOG.info("["+this.getClass().getName()+"] :: " + this.appId + " - " + this.appName);
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

        if(this.tmpDir.endsWith("/")) {
            this.toolWrapper.setOutputFile(this.tmpDir + outputSamFileName);
        }
        else{
            this.toolWrapper.setOutputFile(this.tmpDir + "/" +outputSamFileName);
        }


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
    public ArrayList<String> copyResults(String outputSamFileName) {
        ArrayList<String> returnedValues = new ArrayList<>();

        this.LOG.info("["+this.getClass().getName()+"] :: " + this.appId + " - " + this.appName + " Copying files...");

        try {
            //if (outputDir.startsWith("hdfs")) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            fs.copyFromLocalFile(
                    new Path(this.toolWrapper.getOutputFile()),
                    new Path(this.toolWrapper.getOutputHdfsDir() + "/" + outputSamFileName)
            );
        } catch (IOException e) {
            e.printStackTrace();
            this.LOG.error(e.toString());
        }

        // Delete the old results file
        File tmpSamFullFile = new File(this.toolWrapper.getOutputFile());
        tmpSamFullFile.delete();

        returnedValues.add(this.toolWrapper.getOutputHdfsDir() + "/" + outputSamFileName);

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
        return this.copyResults(outputSamFileName);
    }
}
