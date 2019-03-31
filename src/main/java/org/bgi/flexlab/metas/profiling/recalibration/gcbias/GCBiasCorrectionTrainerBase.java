package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import java.util.ArrayList;

/**
 * ClassName: GCBiasCorrectionTrainerBase
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public abstract class GCBiasCorrectionTrainerBase {

    protected GCBiasCorrectionModelBase model;

    // Set a single group of coverage and gc values as one point of training dataset.
    public abstract void setPointValue(double cov, double windowGC, double refGC);

    // Pre-training is for groping for proper start values for nls function.
    public abstract void preTrain();

    // The training process for the model.
    public abstract void train();

    // Return trained model;
    public GCBiasCorrectionModelBase getTrainedModel(){
        return this.model;
    }
}
