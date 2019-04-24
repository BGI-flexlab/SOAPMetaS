package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

/**
 * ClassName: GCBiasModelFactory
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class GCBiasModelFactory {

    private String modelName;

    private String inputCoefficientsFile;

    public GCBiasModelFactory(String modelName, String coefficientsFile) {
        this.modelName = modelName;
        this.inputCoefficientsFile = coefficientsFile;
    }

    public GCBiasModelBase getGCBiasRecaliModel(){

        GCBiasDefaultModel model;

        switch (this.modelName){
            default:{
                if (this.inputCoefficientsFile != null){
                    model = new GCBiasDefaultModel(this.inputCoefficientsFile);
                } else {
                    model = new GCBiasDefaultModel();
                }
                break;
            }
        }

        return model;
    }
}
