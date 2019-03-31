package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.spark.api.java.JavaRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

/**
 * ClassName: GCBiasTrainingProcess
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class GCBiasTrainingProcess {

    private MetasOptions metaOpt;

    private GCBiasCorrectionTrainerBase trainer;

    private int scanWindowSize;

    private String trainingResultFile;

    /**
     * TODO: trainer可以考虑提供工厂类，用于针对不同的model选取不同的trainer。
     *
     * @param options
     */
    public GCBiasTrainingProcess(MetasOptions options){
        this.metaOpt = options;
        this.scanWindowSize = this.metaOpt.getScanWindowSize();
        this.trainer = new GCBiasCorrectionDefaultModelTrainer();
        this.trainingResultFile = this.metaOpt.getGcBiasTrainingOutputFile();
    }

    /**
     * The main algorithm is based on the GCBiasUtils and GCBiasMetricsCollector class from Picard
     * library and article https://doi.org/10.1371/journal.pone.0165015.
     *
     * Another computation for totalReads of each species:
     * Unlike what the article describes (number of read starts divided by number of bac species),
     * in order to calculate the norm_cov of a single bacterium, we grouped the SamRecords and
     * calculated values by the key of species name.
     *
     * TODO: 这里的 MetasSamRecord 还是要考虑一下 PE 和 SE 数据的差异。
     * TODO: runTrainingProcess方法的输入参数 ReferenceSequence 可能需要单独根据species数据来创建
     * TODO: 不确定两种计算totalReads的方法对最后建模结果以及校正效果的影响。
     *
     * @param metasSamRecordRDD RDD created from SAM output of alignment process. Note that the alignment is between
     *                          reads and species genome.
     * @param refSeq Genome sequence of each reference species used in training process.
     */
    public void runTrainingProcess(JavaRDD<MetasSamRecord> metasSamRecordRDD, HashMap<String, ReferenceSequence> refSeq){

        HashMap<String, SpeciesGC> speciesGCMap = null;

        for (String species: refSeq.keySet()){
            speciesGCMap.put(species, new SpeciesGC(species, refSeq.get(species)));
        }

        List<Tuple2<String, Integer>> recordPosList = metasSamRecordRDD.filter(rec -> !rec.getReadUnmappedFlag())
                .map(rec -> new Tuple2<>(rec.getReferenceName(),
                        rec.getReadNegativeStrandFlag() ? rec.getAlignmentEnd() - this.scanWindowSize : rec.getAlignmentStart()))
                .collect();

        for (Tuple2<String, Integer> tup: recordPosList){
            speciesGCMap.get(tup._1).addRead(tup._2);
        }

        double totalReads = recordPosList.size()/speciesGCMap.size();

        for (String species: speciesGCMap.keySet()){

            final int[] windowByGC = speciesGCMap.get(species).getWindowsByGC();
            final int[] readsByGC = speciesGCMap.get(species).getReadsByGC();
            final double refGCRate = speciesGCMap.get(species).getSpeGCRate();

            final double totalWindows = sum(windowByGC);
            final double meanReadsPerWindow = totalReads / totalWindows;

            assert totalReads > 0;

            for (int i = 0; i < windowByGC.length; i++){
                if(windowByGC[i] > 0){
                    this.trainer.setPointValue(readsByGC[i]/windowByGC[i]/meanReadsPerWindow, windowByGC[i], refGCRate);
                }
            }
        }

        this.trainer.train();
        this.trainer.getTrainedModel().outputCoefficients(this.trainingResultFile);
    }

    private double sum(final int[] values) {
        final int length = values.length;
        double total = 0;
        for (int i = 0; i < length; i++) {
            total += values[i];
        }
        return total;
    }

    class SpeciesGC {
        private String name;
        private double speGCRate;
        private byte[] gc;
        private int[] windowsByGC;
        private int[] readsByGC;

        public SpeciesGC(String name, ReferenceSequence refSequence) {
            this.windowsByGC = GcBiasUtils.calculateRefWindowsByGc(refSequence,
                    GCBiasTrainingProcess.this.scanWindowSize);
            this.gc = GcBiasUtils.calculateAllGcs(refSequence.getBases(),
                    refSequence.getBases().length - GCBiasTrainingProcess.this.scanWindowSize,
                    GCBiasTrainingProcess.this.scanWindowSize);
            this.readsByGC = new int[this.windowsByGC.length];
            this.speGCRate = SequenceUtil.calculateGc(refSequence.getBases());
        }

        public void addRead(int pos) {
            if (pos > 0) {
                final int windowGc = gc[pos];
                if (windowGc >= 0) {
                    ++this.readsByGC[windowGc];
                }
            }
        }

        public int[] getWindowsByGC(){
            return this.windowsByGC;
        }

        public int[] getReadsByGC() {
            return this.readsByGC;
        }

        public double getSpeGCRate() {
            return speGCRate;
        }
    }
}
