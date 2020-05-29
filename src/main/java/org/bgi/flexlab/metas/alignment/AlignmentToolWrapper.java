package org.bgi.flexlab.metas.alignment;

import org.bgi.flexlab.metas.util.SequencingMode;

import java.io.Serializable;

/**
 * ClassName: AlignmentToolWrapper
 * Description: Tools wrapper for bowtie/bwa (or others if needed). The basic class for MetasBowtie.java
 * and Bwa.java of SparkBWA. The script is abstracted from com.github.sparkbwa.Bwa
 *
 * @author heshixu@genomics.cn
 */

public abstract class AlignmentToolWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private String inputFile;
    private String inputFile2;
    private String outputFile;
    private String alignmentIndexPath;

    private String readGroupID;
    private String smTag;

    private String samOutputHdfsDir;
    //private String localTmpDir;
    private String alnTmpDir;
    protected String alnLog;

    private boolean isRetainTemp = false;

    private SequencingMode sequencingMode;

    /**
     * Getter for the second of the input files
     *
     * @return A String containing the second FASTQ file name
     */
    public String getInputFile2() {
        return this.inputFile2;
    }

    /**
     * Setter for the second of the input files
     *
     * @param inputFile2 A String containing the second FASTQ file name
     */
    public void setInputFile2(String inputFile2) {
        this.inputFile2 = inputFile2;
    }

    /**
     * Getter for the first of the input files
     *
     * @return A String containing the first FASTQ file name
     */
    public String getInputFile() {
        return this.inputFile;
    }

    /**
     * Setter for the first of the input files
     *
     * @param inputFile A String containing the first FASTQ file name
     */
    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    /**
     * Getter for the output file
     *
     * @return A String containing the ouput SAM file name
     */
    public String getOutputFile() {
        return this.outputFile;
    }

    /**
     * Setter for the ouput file
     *
     * @param outputFile A String containing the output SAM file name
     */
    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public void setAlnLog(String alnLog) {
        this.alnLog = alnLog;
    }

    public String getAlnLog() {
        return alnLog;
    }

    /**
     * Setter for the sequencing mode.
     *
     * @param mode SequencingMode element.
     */
    public void setSequencingMode(SequencingMode mode){
        this.sequencingMode = mode;
    }

    /**
     * Getter for the option of using the paired reads entries or not
     *
     * @return A boolean value that is true if paired reads are used or false otherwise
     */
    public boolean isPairedReads() {
        return this.sequencingMode.equals(SequencingMode.PAIREDEND);
    }

    /**
     * Getter for the option of using the single reads entries or not
     *
     * @return A boolean value that is true if single reads are used or false otherwise
     */
    public boolean isSingleReads() {
        return this.sequencingMode.equals(SequencingMode.SINGLEEND);
    }

    /**
     * Getter for the index path
     *
     * @return A String containing the index path
     */
    public String getIndexPath() {
        return this.alignmentIndexPath;
    }

    /**
     * Setter for the index path
     *
     * @param indexPath A String that indicates the location of the index path
     */
    public void setIndexPath(String indexPath) {
        this.alignmentIndexPath = indexPath;
    }

    /**
     * Getter for the output HDFS path
     *
     * @return A String containing the output HDFS path
     */
    public String getOutputHdfsDir() {
        return this.samOutputHdfsDir;
    }

    /**
     * Setter for the output HDFS path
     *
     * @param outputHdfsDir A String that indicates the output location for the SAM files in HDFS
     */
    public void setOutputHdfsDir(String outputHdfsDir) {
        this.samOutputHdfsDir = outputHdfsDir;
    }

    //public String getLocalTmpDir() {
    //    return this.localTmpDir;
    //}
    //public void setLocalTmpDir(String tmpPath) {
    //    this.localTmpDir = tmpPath;
    //}

    public String getAlnTmpDir() {
        return alnTmpDir;
    }

    public void setAlnTmpDir(String alnTmpDir) {
        this.alnTmpDir = alnTmpDir;
    }

    public void setReadGroupID(String readGroupID) {
        this.readGroupID = readGroupID;
    }

    public String getReadGroupID() {
        return readGroupID;
    }

    public void setSMTag(String smTag) {
        this.smTag = smTag;
    }

    public String getSMTag() {
        return smTag;
    }

    public void setRetainTemp(boolean retainTemp) {
        isRetainTemp = retainTemp;
    }

    public boolean isRetainTemp() {
        return isRetainTemp;
    }

    /**
     *
     * @param alnStep Param to know if the aln algorithm is going to be used and BWA functions need to be executes more than once
     * @return A String array containing the parameters to launch BWA
     */
    protected abstract String[] parseArguments(int alnStep);

    /**
     * This Function is responsible for creating the options that are going to be passed to BWA
     *
     * *Change:
     *  + Deprecate aln step argument. Here we wrap the step control into Tool Wrapper.
     *
     *
     * TODO: 后续整合 BWA Wrapper 的时候记得在 run() 方法中实现 alnStep 的步骤控制。
     *
     * @return A Strings array containing the options which BWA was launched
     */
    public abstract int run();
}
