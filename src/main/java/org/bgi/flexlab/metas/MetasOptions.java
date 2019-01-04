package org.bgi.flexlab.metas;

import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.ProfilingPipelineMode;
import org.bgi.flexlab.metas.util.SequencingMode;

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
    private String referenceMatrixFilePath;
    private int insertSize;
    private int readLength;

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

    public String getReferenceMatrixFilePath(){
        return this.referenceMatrixFilePath;
    }

    public int getInsertSize(){
        return this.insertSize;
    }

    public int getReadLength(){
        return this.readLength;
    }

}
