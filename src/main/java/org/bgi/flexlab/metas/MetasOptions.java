package org.bgi.flexlab.metas;

import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.ProfilingPipelineMode;
import org.bgi.flexlab.metas.util.SequencingMode;

import java.io.File;

/**
 * ClassName: MetasOptions
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MetasOptions {

    /**
     * TODO: 需要完善options的读取和初始化.
     */

    // Running mode group.
    private ProfilingAnalysisMode profilingAnalysisMode;
    private SequencingMode sequencingMode;
    private ProfilingPipelineMode profilingPipelineMode;
    private ProfilingAnalysisLevel profilingAnalysisLevel;

    // Alignment process arguments group.
    private String alignmentTool = "bowtie";

    private String alignmentIndexPath = "";
    private boolean alignmentShortIndex = true;

    private String inputFastqPath = "";
    private String inputFastqPath2 = "";

    private String samOutputHdfsDir = "";
    private int partitionNumber = 0;
    private String alignmentTmpDir = "";

    private boolean sortFastqReads = false;
    private boolean sortFastqReadsHdfs 	= false;
    private boolean mergeOutputSamBySample = false;

    private String extraAlignmentArguments = "";

    // GCBias correction arguments group.
    private String gcBiasCorrectionModelType = "default";
    private File gcBiasCoefficientsTrainingTargetFile;
    private File gcBiasCoefficientsModelFile;
    private String modelName = "default";
    private boolean gcBiasTrainingMode = false;
    private int scanWindowSize;

    // Insert size related training.
    private boolean doInsRecalibration;

    // Profiling process arguments group.
    private String referenceMatrixFilePath;
    private int insertSize;
    private int readLength;
    private boolean doIdentityFiltering = false;
    private double minIdentity = 0;

    /**
     * Constructor
     */
    public MetasOptions() {}

    /**
     * Constructor
     * @param args Arguments from command line.
     */
    public MetasOptions(String[] args){}



    public String getReferenceMatrixFilePath(){
        return this.referenceMatrixFilePath;
    }

    /*
    Analysis mode method group.
     */

    public ProfilingAnalysisMode getProfilingAnalysisMode(){
        return this.profilingAnalysisMode;
    }

    public SequencingMode getSequencingMode(){
        return this.sequencingMode;
    }

    public ProfilingPipelineMode getProfilingPipelineMode(){
        return this.profilingPipelineMode;
    }

    public ProfilingAnalysisLevel getProfilingAnalysisLevel(){
        return this.profilingAnalysisLevel;
    }

    /*
    Filteration related method group.
     */

    public boolean isDoInsRecalibration(){
        return this.doInsRecalibration;
    }

    /**
     * TODO: 如果有没有设定值就返回默认值，如果有设定值就返回设定值
     * @return
     */
    public int getInsertSize(){
        return this.insertSize;
    }

    public double getMinIdentity(){
        return this.minIdentity;
    }

    public boolean isDoIdentityFiltering(){
        return this.doIdentityFiltering;
    }

    public int getReadLength(){
        return this.readLength;
    }

    /*
    GCBias recalibration related method group
     */

    public int getScanWindowSize() {
        return scanWindowSize;
    }

    public File getGcBiasModelCoefficientsFile(){
        return this.gcBiasCoefficientsModelFile;
    }

    public File getGcBiasTrainingTargetCoefficientsFile(){
        return this.gcBiasCoefficientsTrainingTargetFile;
    }

    public String getModelName(){
        return this.modelName;
    }

    public boolean isGCBiasTrainingMode(){
        return this.gcBiasTrainingMode;
    }

    public String getGcBiasCorrectionModelType(){
        return this.gcBiasCorrectionModelType;
    }

    /*
    Alignment related method group.
     */

    public String getInputFastqPath() {
        return this.inputFastqPath;
    }

    public String getInputFastqPath2(){
        return this.inputFastqPath2;
    }

    public String getAlignmentIndexPath(){
        return this.alignmentIndexPath;
    }

    public String getSamOutputHdfsDir() {
        return this.samOutputHdfsDir;
    }

    public boolean isAlignmentShortIndex() {
        return this.alignmentShortIndex;
    }

    public int getPartitionNumber() {
        return this.partitionNumber;
    }

    public boolean isSortFastqReads() {
        return this.sortFastqReads;
    }

    public boolean isMergeOutputSamBySample() {
        return this.mergeOutputSamBySample;
    }

    public String getAlignmentTool(){
        return this.alignmentTool;
    }

    public boolean isSortFastqReadsHdfs() {
        return this.sortFastqReadsHdfs;
    }

    public String getExtraAlignmentArguments(){
        return this.extraAlignmentArguments;
    }

    public String getAlignmentTmpDir() {
        return alignmentTmpDir;
    }
}
