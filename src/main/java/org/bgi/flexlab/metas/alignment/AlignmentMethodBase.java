package org.bgi.flexlab.metas.alignment;

/**
 * ClassName: AlignmentMethodBase
 * Description: Basic abstract class of single/paired end alignment method.
 *
 * @author heshixu@genomics.cn
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
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
        this.tmpDir = this.toolWrapper.getAlnTmpDir();
        this.outDir = this.toolWrapper.getOutputHdfsDir();

        //LOG.info("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] " + this.appId + " - " + this.appName);
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
     * @param outSamFileName The name of the file of final results.
     * @return An ArrayList containing all the file locations
     */
    public ArrayList<String> copyResults(String outSamFileName, String alnLogFileName, String readGroupID, String smTag) {
        ArrayList<String> returnedValues = new ArrayList<>(2);

        //boolean deleteSrc = true;
        boolean deleteSrc = ! this.toolWrapper.isRetainTemp();

        //LOG.trace("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] " + this.appId + " - " +
        //        this.appName + " Copy output sam files to output directory.");
        String outDir = this.toolWrapper.getOutputHdfsDir();
        String hdfsSamPath = outDir + "/" + outSamFileName;
        String hdfsLogPath = outDir + "/" + alnLogFileName;
        Path samSrc =  new Path(this.toolWrapper.getOutputFile());
        Path samDst = new Path(hdfsSamPath);
        Path logSrc = new Path(this.toolWrapper.getAlnLog());
        Path logDst = new Path(hdfsLogPath);

        Configuration conf = new Configuration();

        FileSystem srcFS = null;
        try {
             srcFS = FileSystem.newInstanceLocal(conf);
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] Fail to construct source FileSystem. Source directory: " + this.toolWrapper.getAlnTmpDir() + " . Message: " + e.toString());
        }
        FileSystem dstFS = null;
        try {
            if (outDir.startsWith("file://")) {
                dstFS = FileSystem.newInstanceLocal(conf);
            } else {
                dstFS = FileSystem.get(conf);
            }
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] Fail to construct destination FileSystem. Destination directory: " + outDir + " . Message: " + e.toString());
        }

        try {
            if (srcFS != null && dstFS != null) {
                FileUtil.copy(srcFS, samSrc, dstFS, samDst, deleteSrc, conf);
            } else {
                LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] FileSystem is NULL for source file: " + samSrc.getName() + " . dest file: " + samDst.getName());
            }
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] Fail to copy SAM file + " + samSrc + ". " + e.toString());
        } catch (Exception e) {
            LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] SAM file copy exception. " + this.appId + " - " + this.appName + ": src: " + this.toolWrapper.getOutputFile() + " | dst: " + hdfsSamPath);
        }

        try {
            if (srcFS != null && dstFS != null) {
                FileUtil.copy(srcFS, logSrc, dstFS, logDst, deleteSrc, conf);
            } else {
                LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] FileSystem is NULL for source file: " + logSrc.getName() + " . dest file: " + logDst.getName());
            }
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] Fail to copy alignment log file + " + samSrc + ". " + e.toString());
        } catch (Exception e) {
            LOG.error("[SOAPMetas::" + AlignmentMethodBase.class.getName() + "] Log file copy exception. " + this.appId + " - " + this.appName + ": src: " + this.toolWrapper.getAlnLog() + " | dst: " + hdfsLogPath);
        }

        if (smTag != null) {
            returnedValues.add(readGroupID + '\t' + smTag + '\t' + hdfsSamPath);
        } else {
            returnedValues.add("NORGID\tNOSMTAG\t" + hdfsSamPath);
        }

        return returnedValues;
    }

    ///**
    // * @param readBatchID Identification for the sam file
    // * @return A String for the sam file name
    // */
    //public String getOutputSamFilename(Integer readBatchID) {
    //    return this.appName + "-" + this.appId + "-" + readBatchID + ".sam";
    //}

    ///**
    // *
    // * @param readBatchID Identification for the sam file
    // * @param fastqFileName1 First of the FASTQ files
    // * @param fastqFileName2 Second of the FASTQ files
    // * @return
    // */
    //public ArrayList<String> runAlignmentProcess(Integer readBatchID, String fastqFileName1, String fastqFileName2) {
    //    //The output filename (without the tmp directory)
    //    String outputSamFileName = this.getOutputSamFilename(readBatchID);
    //    this.alignReads(outputSamFileName, fastqFileName1, fastqFileName2);
    //    // Copy the result to HDFS
    //    return this.copyResults(outputSamFileName, "NORGID", "NOSMTAG");
    //}
}
