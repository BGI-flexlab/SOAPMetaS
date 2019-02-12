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

    private ProfilingAnalysisMode profilingAnalysisMode;
    private SequencingMode sequencingMode;
    private ProfilingPipelineMode profilingPipelineMode;
    private ProfilingAnalysisLevel profilingAnalysisLevel;

    private String gcBiasCorrectionModelType = "default";

    private String referenceMatrixFilePath;

    private boolean insRecalibration;
    private int insertSize;
    private int readLength;

    private int scanWindowSize;

    private File gcBiasCoefficientsTrainingTargetFile;
    private File gcBiasCoefficientsModelFile;
    private String modelName = "default";
    private boolean isTrainingMode = false;

    public MetasOptions() {}

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

    public String getGcBiasCorrectionModelType(){
        return this.gcBiasCorrectionModelType;
    }

    public String getReferenceMatrixFilePath(){
        return this.referenceMatrixFilePath;
    }

    public boolean getInsRecalibration(){
        return this.insRecalibration;
    }

    /**
     * TODO: 如果有没有设定值就返回默认值，如果有设定值就返回设定值
     * @return
     */
    public int getInsertSize(){

        return this.insertSize;
    }

    public int getReadLength(){
        return this.readLength;
    }

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

    public boolean getTrainingMode(){
        return this.isTrainingMode;
    }
}
