package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bgi.flexlab.metas.MetasOptions;
import org.renjin.script.RenjinScriptEngineFactory;
import org.renjin.sexp.DoubleVector;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.*;
import java.util.ArrayList;

/**
 * ClassName: GCBiasDefaultModelTrainer
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class GCBiasDefaultModelTrainer extends GCBiasModelTrainerBase implements Serializable {

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(GCBiasDefaultModelTrainer.class.getName());

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

    private String nlsOption = "";
    private String startValue = "p1=0.812093,p2=49.34331,p3=8.886807,p4=6.829778,p5=0.2642576,p6=-0.005291173,p7=3.188492E-5,p8=-2.502158";
    private int pointCount;

    //private double[] startCoefficients = new double[8];

    private boolean outputPoint = false;
    private String pointPath;

    public GCBiasDefaultModelTrainer(MetasOptions options){
        this.model = new GCBiasDefaultModel();
        this.normCoverage = new ArrayList<>(101);
        this.windowGCLevel = new ArrayList<>(101);
        this.referenceGCLevel = new ArrayList<>(101);

        setNLSControl(options.getNlsControl());
        setStartValue(options.getStartValue());
        if (options.isOutputPoint()){
            doOutputPoint();
            setPointPath(options.getPointPath());
        }

        //this.setModelStartCoefficients(new double[]{0.812093, 49.34331, 8.886807,
        //        6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158});
    }

    public void addPointValue(double cov, double windowGC, double refGC){
        this.normCoverage.add(cov);
        this.windowGCLevel.add(windowGC);
        this.referenceGCLevel.add(refGC);
        this.pointCount++;
    }

    private void setNLSControl(String controlList){
        ((GCBiasDefaultModel) this.model).setNlsControlList(controlList);
        this.nlsOption = controlList;
    }

    private void setStartValue(String startValue){
        ((GCBiasDefaultModel) this.model).setStartValue(startValue);
        this.startValue = startValue;
    }

    private void doOutputPoint(){
        this.outputPoint = true;
    }

    private void setPointPath(String filePath){
        this.pointPath = filePath;
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

    //private void setModelStartValue(double[] coes){
    //    //Default start value is from the referred article which is the same as default model's.
    //    assert coes.length == 8;
    //    for (int i=0; i<coes.length; i++){
    //        this.startCoefficients[i] = coes[i];
    //    }
    //}

    private String getStartValue(){
        //Default start value is from the referred article which is the same as default model's.
        //{0.812093, 49.34331, 8.886807, 6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158};
        //return String.format("p1=%g,p2=%g,p3=%g,p4=%g,p5=%g,p6=%g,p7=%g,p8=%g",
        //        this.startCoefficients[0],this.startCoefficients[1],this.startCoefficients[2],this.startCoefficients[3],
        //        this.startCoefficients[4],this.startCoefficients[5],this.startCoefficients[6],this.startCoefficients[7]);
        return this.startValue;
    }

    /**
     * Training for relative best coefficients.
     */
    public void train(){
        double[] trainedCoefficients = new double[]{0.812093, 49.34331, 8.886807,
                6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158};

        /*
        OUTPUT point.
         */
        if (this.outputPoint) {
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(new File(this.pointPath))))) {
                bw.write("normCoverage\twindowGCLevel\treferenceGCLevel");
                bw.newLine();
                for (int i = 0; i < this.pointCount; i++) {
                    bw.write(this.normCoverage.get(i).toString());
                    bw.write('\t');
                    bw.write(this.windowGCLevel.get(i).toString());
                    bw.write('\t');
                    bw.write(this.referenceGCLevel.get(i).toString());
                    bw.newLine();
                }
                bw.flush();
            } catch (FileNotFoundException e){
                LOG.error("[SOAPMetas::" + GCBiasDefaultModelTrainer.class.getName() + "] Can't find point " +
                        "output file: " + this.pointPath + " . " + e.toString());
            } catch (IOException e) {
                LOG.error("[SOAPMetas::" + GCBiasDefaultModelTrainer.class.getName() + "] Fail to output " +
                        "point matrix. " + e.toString());
            }
        } else {

            ScriptEngine rEngine = new RenjinScriptEngineFactory().getScriptEngine();
            //System.out.println("startValue = list(" + this.getStartValue() + ")");
            //System.out.println("controler = list(" + this.nlsOption + ")");
            //System.out.println("trained = nls(coverage ~ " + this.model.getFunction() + ", data = GC, start = startValue, control = controler)");
            try {
                rEngine.put("normCov", convertArray(this.normCoverage));
                rEngine.put("windowGC", convertArray(this.windowGCLevel));
                rEngine.put("referenceGC", convertArray(this.referenceGCLevel));
                rEngine.eval("GC = data.frame(coverage=normCov, readGC=windowGC, genomeGC=referenceGC)");
                rEngine.eval("startValue = list(" + this.getStartValue() + ")");
                rEngine.eval("controler = list(" + this.nlsOption + ")");
                rEngine.eval("trained = nls(coverage ~ "+ this.model.getFunction() +", data = GC, start = startValue, control = controler)");
                DoubleVector trained = (DoubleVector) rEngine.eval("unname(coef(trained))");

                int size = trained.length();
                for (int i=0; i<size; i++){
                    trainedCoefficients[i] = trained.getElementAsDouble(i);
                }
            } catch (ScriptException e) {
                LOG.error("[SOAPMetas::" + GCBiasDefaultModelTrainer.class.getName() + "] Model Trainer engine works abnormally. " +
                        e.toString());
            } catch (IndexOutOfBoundsException e){
                LOG.error("[SOAPMetas::" + GCBiasDefaultModelTrainer.class.getName() + "] Trained coefficients counts more than " +
                        + trainedCoefficients.length + " . Please check model function." + e.toString());
            }
            rEngine = null;
        }
        this.model.setCoefficients(trainedCoefficients);
    }

    private double[] convertArray(ArrayList<Double> arrayList){

        int size = arrayList.size();
        double[] out = new double[size];

        for (int i=0; i<size; i++){
            out[i] = arrayList.get(i);
        }

        return out;
    }
}
