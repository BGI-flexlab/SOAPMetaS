package org.bgi.flexlab.metas.alignment;

import org.bgi.flexlab.metas.util.SequencingMode;

import java.io.Serializable;

/**
 * ClassName: AlignmentToolWrapper
 * Description: Tools wrapper for bowtie/bwa (or others if needed). The basic class for MetasBowtie.java
 * and Bwa.java of SparkBWA. The script is abstracted from com.github.sparkbwa.Bwa
 *
 * @author: heshixu@genomicsw.cn
 */

public abstract class AlignmentToolWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private String inputFile = "";
    private String inputFile2 = "";
    private String outputFile = "alignment.outputTmp";
    private String alignmentIndexPath;

    private String samOutputHdfsDir;

    private String tmpDirectory;

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
        return this.sequencingMode.equals(SequencingMode.PAIREND);
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

    /**
     * Getter for the tmp path
     *
     * @return A String containing the tmp path
     */
    public String getTmpDir() {
        return this.tmpDirectory;
    }


    /**
     * Setter for the tmp path
     *
     * @param tmpPath A String that indicates the tmp dir
     */
    public void setTmpDir(String tmpPath) {
        this.tmpDirectory = tmpPath;
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
