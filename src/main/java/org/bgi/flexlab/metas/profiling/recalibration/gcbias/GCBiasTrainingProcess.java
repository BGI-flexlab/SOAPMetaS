package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.spark.api.java.JavaRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.io.samio.MetasSamRecord;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.List;

/**
 * ClassName: GCBiasTrainingProcess
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class GCBiasTrainingProcess {

    private MetasOptions metaOpt;

    private GCBiasCorrectionTrainerBase trainer;

    private int scanWindowSize;

    private File trainingResultFile;

    /**
     * TODO: trainer可以考虑提供工厂类，用于针对不同的model选取不同的trainer。
     *
     * @param options
     */
    public GCBiasTrainingProcess(MetasOptions options){
        this.metaOpt = options;
        this.scanWindowSize = this.metaOpt.getScanWindowSize();
        this.trainer = new GCBiasCorrectionDefaultModelTrainer();
        this.trainingResultFile = this.metaOpt.getGcBiasTrainingTargetCoefficientsFile();
    }

    /**
     * The main algorithm is based on the GCBiasUtils and GCBiasMetricsCollector class from Picard library. Unlike what the article
     * <https://doi.org/10.1371/journal.pone.0165015> describes (number of read starts divided by number of bac species), in order to
     * calculate the norm_cov of a single bacterium, we grouped the SamRecords and calculated values by the key of species name.
     *
     * @param metasSamRecordRDD
     * @param refSeq
     */
    public void runTrainingProcess(JavaRDD<MetasSamRecord> metasSamRecordRDD, HashMap<String, ReferenceSequence> refSeq){

        HashMap<String, SpeciesGC> speciesGCMap = null;

        for (String species: refSeq.keySet()){
            speciesGCMap.put(species, new SpeciesGC(species, refSeq.get(species)));
        }

        List<Tuple2<String, Integer>> recordPosList = metasSamRecordRDD.filter(rec -> !rec.getReadUnmappedFlag())
                .map(rec -> new Tuple2<>(rec.getReferenceName(), rec.getReadNegativeStrandFlag() ? rec.getAlignmentEnd() - this.scanWindowSize : rec.getAlignmentStart()))
                .collect();

        for (Tuple2<String, Integer> tup: recordPosList){
            speciesGCMap.get(tup._1).addRead(tup._2);
        }

        for (String species: speciesGCMap.keySet()){

            final int[] windowByGC = speciesGCMap.get(species).getWindowsByGC();
            final int[] readsByGC = speciesGCMap.get(species).getReadsByGC();
            final double refGCRate = speciesGCMap.get(species).getSpeGCRate();

            final double totalWindows = sum(windowByGC);
            final double totalReads = sum(readsByGC);
            final double meanReadsPerWindow = totalReads / totalWindows;

            assert totalReads > 0;

            for (int i = 0; i < windowByGC.length; i++){
                if(windowByGC[i] > 0){
                    this.trainer.setPointValue(readsByGC[i]/windowByGC[i]/meanReadsPerWindow, windowByGC[i], refGCRate);
                }
            }
        }

        this.trainer.train();
    }

    private double sum(final int[] values) {
        final int length = values.length;
        double total = 0;
        for (int i = 0; i < length; i++) {
            total += values[i];
        }
        return total;
    }

    public void writeModel(){
        this.trainer.getTrainedModel().outputCoefficients(this.trainingResultFile);
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
