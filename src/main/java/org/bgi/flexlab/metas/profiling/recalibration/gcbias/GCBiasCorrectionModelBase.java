package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import java.io.File;

/**
 * ClassName: GCBiasCorrectionModelBase
 * Description: The basic abstract class of GC bias correction model. Different model depends on different
 * correction function/method, such as LOWESS regression method or method in https://doi.org/10.1093/nar/gks001.
 * Pay attention that some classic models for CNV analysis of human genome don't need coefficients training,
 * and application of these models needs more consideration.
 *
 * @author heshixu@genomics.cn
 */

public abstract class GCBiasCorrectionModelBase {

    /**
     * GcBias校正所需的非线性回归模型，当前类是抽象类，后续如果需要引入不同的回归模型来优化校正结果，则可以基于此模型进行扩展。
     */

    //coefficients list of the model，具体的模型要根据函数本身来确定系数数组的大小
    protected Double[] coefficients;
    protected boolean coefficientsState = false;

    //回归模型的函数表达式，此表达式用于R语言的nls回归和模型读写
    protected String modelFunction;

    /**
     * Correction for pair-end sequencing data.
     *
     * @param read1GCContent GC content (rate) of read 1 (first segment in SAM file).
     * @param read2GCContent GC content of read 2.
     * @param genomeGCContent GC content of reference genome.
     * @return Double type number, corrected count of read/fragment.
     */
    public abstract Double correctedCountForPair(Double read1GCContent, Double read2GCContent, Double genomeGCContent);

    /**
     * Correction for single-end sequencing data.
     *
     * @param readGCContent GC content (rate) of read.
     * @param genomeGCContent GC content of reference genome.
     * @return Double type number, corrected count of read/fragment.
     */
    public abstract Double correctedCountForSingle(Double readGCContent, Double genomeGCContent);

    /**
     * The method is used to save coefficients to file so that the model could be reused.
     *
     * @param outFilePath Output file target to save coefficients.
     */
    public abstract void outputCoefficients(String outFilePath);

    /**
     * The method is used to read the saved model file.
     *
     * @param inFilePath Input file target to load coefficients.
     */
    public abstract void inputCoefficients(String inFilePath);

    /**
     * Setter for coefficients, from saved file or trainer.
     *
     * @param coefficients The concrete value from training result or coefficients file.
     */
    public abstract void setCoefficients(Double[] coefficients);

    protected void setCoefficientsState(boolean state){
        this.coefficientsState = state;
    }

    public boolean isCoefficientsSet(){
        return this.coefficientsState;
    }

    public void setFunction(String modelFunction) {
        this.modelFunction = modelFunction;
    }

    public String getFunction(){
        return this.modelFunction;
    }
}
