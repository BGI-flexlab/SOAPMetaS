package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import org.renjin.script.RenjinScriptEngineFactory;
import org.renjin.sexp.DoubleVector;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;

/**
 * ClassName: GCBiasCorrectionDefaultModelTrainer
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class GCBiasCorrectionDefaultModelTrainer extends GCBiasCorrectionTrainerBase {

    /**
     * Array stores gc rate of window and reference genome, as well as the normalized reads coverage (counts).
     *
     * "Window" is the 100 bp fragment on reference sequence, and its gc is treated as "read gc" or "local
     * region gc" during training. The store oder of the three arrays must be identical.
     *
     */
    private ArrayList<Double> normCoverage;
    private ArrayList<Double> windowGCLevel;
    private ArrayList<Double> referenceGCLevel;

    private RenjinScriptEngineFactory rEngineFacory;

    private ScriptEngine rEngine;

    private String function;

    private double[] startCoefficients = new double[8];

    public GCBiasCorrectionDefaultModelTrainer(){
        super();
        this.model = new GCBiasCorrectionDefaultModel();
        this.normCoverage = new ArrayList<>(101);
        this.windowGCLevel = new ArrayList<>(101);
        this.referenceGCLevel = new ArrayList<>(101);

        this.rEngineFacory = new RenjinScriptEngineFactory();
        this.rEngine = rEngineFacory.getScriptEngine();

        //default function refers to <https://doi.org/10.1371/journal.pone.0165015>
        this.function = "para1*exp(-0.5*(readGC-para2/para3)^2)+para4+para5*readGC" +
                "+para6*readGC^2+para7*readGC^3+para8*log(genomeGC)";
        this.model.setFunction(this.function);

        this.setModelStartCoefficients(new double[]{0.812093, 49.34331, 8.886807, 6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158});
    }

    public void setPointValue(double cov, double windowGC, double refGC){
        this.normCoverage.add(cov);
        this.windowGCLevel.add(windowGC);
        this.referenceGCLevel.add(refGC);
    }

    /**
     * TODO: 考虑一下如何采用适当的方式将defaultModel中的模型转化成线性模式，或者考虑采用文献中已经有的参数，或者考虑多次训练？
     */
    public void preTrain(){
        //$k = 0.812093*exp(-0.5*(($GC-49.34331)/8.886807)**2) + 6.829778 + 0.2642576*$GC - 0.005291173*$GC**2 + 0.00003188492*$GC**3 - 2.502158*log($Genome_gc);
        //double[] coefficients = new double[]{0.812093, 49.34331, 8.886807, 6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158};;
        //this.setModelStartCoefficients(coefficients);
        return;
    }

    private void setModelStartCoefficients(double[] coes){
        //Default start value is from the referred article which is the same as default model's.
        assert coes.length == 8;
        for (int i=0; i<coes.length; i++){
            this.startCoefficients[i] = coes[i];
        }
    }

    private String getStartValueListString(){
        //Default start value is from the referred article which is the same as default model's.
        //{0.812093, 49.34331, 8.886807, 6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158};
        return String.format("para1=%g,para2=%g,para3=%g,para4=%g,para5=%g,para6=%g,para7=%g,para8=%g",
                this.startCoefficients[0],this.startCoefficients[1],this.startCoefficients[2],this.startCoefficients[3],
                this.startCoefficients[4],this.startCoefficients[5],this.startCoefficients[6],this.startCoefficients[7]);
    }

    /**
     * Training for relative best coefficients.
     */
    public void train(){
        Double[] trainedCoefficients = new Double[]{0.812093, 49.34331, 8.886807, 6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158};
        try {
            this.rEngine.put("normCov", this.normCoverage);
            this.rEngine.put("windowGC", this.windowGCLevel);
            this.rEngine.put("referenceGC", this.referenceGCLevel);
            this.rEngine.eval("GC = data.frame(cov=normCov, readGC=windowGC, genomeGC=referenceGC)");
            this.rEngine.eval("startValue = c(" + this.getStartValueListString() + ")");
            this.rEngine.eval("trained = nls(cov ~ "+ this.function +", data = GC, start = startValue)");
            DoubleVector trained = (DoubleVector) this.rEngine.eval("unname(coef(trained))");
            for (int i=0; i<trained.length(); i++){
                trainedCoefficients[i] = trained.getElementAsDouble(i);
            }
        } catch (ScriptException e){
            e.printStackTrace();
        } catch (RuntimeException e){
            e.printStackTrace();
        }
        this.model.setCoefficients(trainedCoefficients);
    }
}
