package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

/**
 * ClassName: GCBiasModelTrainerBase
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public abstract class GCBiasModelTrainerBase {

    protected GCBiasModelBase model;

    // Set a single group of coverage and gc values as one point of training dataset.
    public abstract void setPointValue(double cov, double windowGC, double refGC);

    // Pre-training is for groping for proper start values for nls function.
    public abstract void preTrain();

    // The training process for the model.
    public abstract void train();

    // Return trained model;
    public GCBiasModelBase getTrainedModel(){
        return this.model;
    }
}
