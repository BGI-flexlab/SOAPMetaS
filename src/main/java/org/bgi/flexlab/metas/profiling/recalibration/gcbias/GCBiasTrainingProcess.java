package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import htsjdk.samtools.reference.FastaSequenceFile;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.seqdoop.hadoop_bam.SAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ClassName: GCBiasTrainingProcess
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class GCBiasTrainingProcess implements Serializable {

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(GCBiasTrainingProcess.class);

    private final int SPECIES_NUMBER = 100;

    private GCBiasModelTrainerBase trainer;

    private int scanWindowSize;

    private String refFastaFile;
    private String trainingResultFile;

    /**
     * TODO: trainer可以考虑提供工厂类，用于针对不同的model选取不同的trainer。
     *
     * @param options MetasOptions that wraps input options and arguments.
     */
    public GCBiasTrainingProcess(MetasOptions options){
        this.scanWindowSize = options.getScanWindowSize();

        this.trainer = new GCBiasDefaultModelTrainer(options);

        this.refFastaFile = options.getGcBiasTrainerRefFasta();
        this.trainingResultFile = options.getGcBiasModelOutput();
    }

    public void trainGCBiasModel(JavaSparkContext jsc, String multiSamListFile){
        List<String> alignmentResults = null;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(multiSamListFile))))) {

            alignmentResults = br.lines().collect(Collectors.toList());

        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + GCBiasTrainingProcess.class.getName() + "] Fail to read multi-SAM " +
                    "list file: " + multiSamListFile);
        }

        if (alignmentResults == null || alignmentResults.size() < 1){
            LOG.error("[SOAPMetas::" + GCBiasTrainingProcess.class.getName() + "]  No available " +
                    "alignment results in SAM format.");
            return;
        }

        this.trainGCBiasModel(jsc, alignmentResults.iterator());
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
     * TODO: 这里的 SAMRecord 还是要考虑一下 PE 和 SE 数据的差异。
     * TODO: 输入参数 ReferenceSequence 可能需要单独根据species数据来创建
     * TODO: 不确定两种计算totalReads的方法(average reads number of all species, number of reads grouped by species)对最后建模结果以及校正效果的影响。
     *
     * @param jsc JavaSparkContext of the original application.
     * @param alignmentResults Iterator of results paths String list generated from alignmentProcess.
     */
    public void trainGCBiasModel(JavaSparkContext jsc, Iterator<String> alignmentResults){

        StringBuilder samPathsStB = new StringBuilder(128);
        while(alignmentResults.hasNext()){
            String path = StringUtils.split(alignmentResults.next(), '\t')[2];
            if (!path.startsWith("file://")){
                samPathsStB.append("file://").append(path).append(',');
            } else {
                samPathsStB.append(path).append(',');
            }
        }

        samPathsStB.deleteCharAt(samPathsStB.length()-1);
        alignmentResults = null;

        FastaSequenceFile fastaSeqFile = new FastaSequenceFile(new File(this.refFastaFile), true);
        Map<String, SpeciesGC> speciesGCMap = new HashMap<>(SPECIES_NUMBER);
        ReferenceSequence refSeq;
        while ((refSeq = fastaSeqFile.nextSequence()) != null){
            speciesGCMap.put(refSeq.getName(), new SpeciesGC(refSeq));
        }
        refSeq = null;

        LOG.debug("[SOAPMetas::" + GCBiasTrainingProcess.class.getName() + "] Input SAM file for training: " + samPathsStB.toString());
        List<Tuple2<String, Integer>> recordPosList = jsc.newAPIHadoopFile(samPathsStB.toString(),
                SAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, jsc.hadoopConfiguration())
                .mapToPair(rec -> new Tuple2<>(rec._1.get(), rec._2.get())).values()
                .filter(rec -> !rec.getReadUnmappedFlag())
                .map(rec -> {
                    int alignmentPos;
                    if (rec.getReadNegativeStrandFlag()){
                        alignmentPos = rec.getAlignmentEnd() - this.scanWindowSize;
                    } else {
                        alignmentPos = rec.getAlignmentStart();
                    }
                    return new Tuple2<>(rec.getReferenceName(), alignmentPos);
                })
                .collect();
        samPathsStB = null;

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

            for (int i = 0; i < windowByGC.length; i++){
                if(windowByGC[i] > 0){
                    this.trainer.addPointValue((double) readsByGC[i]/windowByGC[i]/meanReadsPerWindow, windowByGC[i], refGCRate);
                }
            }
        }

        this.trainer.train();
        this.trainer.getTrainedModel().outputCoefficients(this.trainingResultFile);
    }

    private double sum(final int[] values) {
        double total = 0;
        for (int value: values) {
            total += value;
        }
        return total;
    }

    class SpeciesGC {
        private String name = null;
        private double speGCRate;
        private byte[] gc;
        private int[] windowsByGC;
        private int[] readsByGC;

        SpeciesGC(ReferenceSequence refSequence) {
            this.windowsByGC = GcBiasUtils.calculateRefWindowsByGc(refSequence,
                    GCBiasTrainingProcess.this.scanWindowSize);
            this.gc = GcBiasUtils.calculateAllGcs(refSequence.getBases(),
                    refSequence.getBases().length - GCBiasTrainingProcess.this.scanWindowSize,
                    GCBiasTrainingProcess.this.scanWindowSize);
            this.readsByGC = new int[this.windowsByGC.length];
            this.speGCRate = SequenceUtil.calculateGc(refSequence.getBases());
        }

        void addRead(int pos) {
            if (pos > 0) {
                final int windowGc = gc[pos];
                if (windowGc >= 0) {
                    ++this.readsByGC[windowGc];
                }
            }
        }

        int[] getWindowsByGC(){
            return this.windowsByGC;
        }

        int[] getReadsByGC() {
            return this.readsByGC;
        }

        double getSpeGCRate() {
            return speGCRate;
        }
    }
}
