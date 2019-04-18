package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import java.io.File;

/**
 * ClassName: GCBiasCorrectionModelFactory
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class GCBiasCorrectionModelFactory {

    private String modelName;

    private String inputCoefficientsFile;

    public GCBiasCorrectionModelFactory(String modelName, String coefficientsFile) {
        this.modelName = modelName;
        this.inputCoefficientsFile = coefficientsFile;
    }

    public GCBiasCorrectionModelBase getGCBiasCorrectionModel(){

        GCBiasCorrectionDefaultModel model;

        switch (this.modelName){
            case "builtin":{
              model = new GCBiasCorrectionDefaultModel(this.inputCoefficientsFile);
              break;
            }

            default:{
                model = null;
                break;
            }
        }

        assert !model.equals(null);
        return model;
    }
}
