package org.bgi.flexlab.metas;

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
     * TODO: Options initialization.
     */

    private ProfilingAnalysisMode profilingAnalysisMode;
    private SequencingMode sequencingMode;
    private ProfilingPipelineMode profilingPipelineMode;
    private ProfilingAnalysisLevel profilingAnalysisLevel;

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

}
