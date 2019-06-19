package org.bgi.flexlab.metas.profiling.filter;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.math3.fitting.GaussianCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.bgi.flexlab.metas.data.structure.reference.ReferenceInfoMatrix;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * ClassName: MetasSAMRecordInsertSizeFilter
 * Description: Filter for insert size of paired-end sequencing data. The insert size values of
 * paired-end data are not fixed which can be caused by experimental factors, environmental factors
 * or many other factors.
 *
 * Note: The filter is merely suitable for pair-end sequencing mode. The main intention of the filter
 * is to detect whether the unmapped end of pair reads is located outside the reference gene (the
 * deviation of gene boundary), in other words, whether the "unmapped" is caused by boundary effect.
 * If so, the mapped end will be treated as "proper mapped read", or the mapped "single" end will
 * be filtered out.
 *
 * TODO: 根据实际测序数据分析，ins的分布并不是标准的正态分布模式，所以fitter需要重新考虑。
 * TODO: 注意insert size的基本目的，是为了判断比对边界问题对在PE测序模式下带来的影响。边界问题其实不用在意R2序列一半比对上一半比不上的情况（如果真的有一半，那也就不会被视为未比对上）。
 *
 * @author heshixu@genomics.cn
 */

public class MetasSAMRecordInsertSizeFilter implements MetasSAMRecordFilter, Serializable {

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(MetasSAMRecordInsertSizeFilter.class);

    private int meanInsertSize;

    private int insertSizeSD = 0;

    private int insTolerance = 100;

    private boolean doTraining = false;

    private ReferenceInfoMatrix referenceInfoMatrix;

    public MetasSAMRecordInsertSizeFilter(int insertSize, ReferenceInfoMatrix refInfo){
        this.meanInsertSize = insertSize;
        this.referenceInfoMatrix = refInfo;
    }

    @Override
    public boolean filter(SAMRecord record){
        if (this.doTraining){
            return this.insertSizeRangeCheck(record);
        } else {
            return this.satisfyInsertSizeThreshold(record);
        }
    }

    public void setMeanInsertSize(int meanIns){
        this.meanInsertSize = meanIns;
        LOG.trace("[SOAPMetas::" + MetasSAMRecordInsertSizeFilter.class.getName() + "] Trained mean insert size: " +
                meanIns);
    }

    public void setInserSizeSD(int insertSizeSD) {
        this.insertSizeSD = insertSizeSD;
        LOG.trace("[SOAPMetas::" + MetasSAMRecordInsertSizeFilter.class.getName() + "] Trained insert size SD: " +
                insertSizeSD);
    }

    /**
     * Collect insert size data of all properly-aligned read pair (pair-end sequencing mode) and construct
     * the (insert_size, fragment_count) data points (fragment_count means read-pair count). And use
     * GaussianCurveFitter in org.apache.common.math3.fitting package to fit the data points, gain the
     * mean insert size and standard deviation.
     *
     * @param readMetasSamPairRDD
     */
    public void training(JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD){
        List<WeightedObservedPoint> pointList = readMetasSamPairRDD.values()
                .filter(pairRec -> pairRec.isProperPaired())
                .mapToPair(pairRec -> {
                    return new Tuple2<>(Math.abs(pairRec.getFirstRecord().getInferredInsertSize()), 1);
                    //return new Tuple2<>(ProfilingUtils.computeInsertSize(pairRec.getFirstRecord(), pairRec.getSecondRecord()), 1);
                })
                .reduceByKey((a, b) -> a+b)
                .map(tup -> {
                    //System.out.println("WeightedObservedPoint: weight: 1.0" + " |ins: " + tup._1 + " |count: " + tup._2);
                    return new WeightedObservedPoint(1.0, tup._1, tup._2);
                })
                .collect();
        GaussianCurveFitter.ParameterGuesser guesser = new GaussianCurveFitter.ParameterGuesser(pointList);

        this.setMeanInsertSize((int) guesser.guess()[1]);
        this.setInserSizeSD((int) guesser.guess()[2]);

        this.doTraining = true;
    }

    /**
     * Filtering paired-end reads with only one mapped end. If the unmapped end is located outside
     * the referencce gene, the function will return true.
     *
     * Situation of retaining: (only end1 is mapped on reference gene, end2 is unmapped)
     *                   end1  =====-------------------===== end2              paired-end read
     * --|-----------------------------------------|--------------             reference gene region
     *   ^boundary                                 ^boundary
     *
     * Situation of filtering: (only end1 is mapped on reference gene, end2 is unmapped)
     *             end1  =====---------------===== end2                        paired-end read
     * --|-----------------------------------------|--------------             reference gene region
     *   ^boundary                                 ^boundary
     *
     *
     * @param samRecord The instance of SAMRecord in pair-end sequencing mode which is not properly
     *                  mapped as pair.
     * @return True if the mate read is probably mapped to the sequence region outside around reference marker.
     */
    private boolean satisfyInsertSizeThreshold(SAMRecord samRecord){
        if (samRecord.getReadNegativeStrandFlag()){
            return samRecord.getAlignmentStart() < (this.meanInsertSize - samRecord.getReadLength() + this.insTolerance);
        } else {
            int geneLen = this.referenceInfoMatrix.getGeneLength(samRecord.getReferenceName());
            if (geneLen > 0) {
                return (geneLen - samRecord.getAlignmentStart()) < (this.meanInsertSize + this.insTolerance);
            } else {
                return false;
            }
        }
    }

    /**
     * More precise method for insert size filtering. As the insert size of reads from paired-end
     * sequencing is not "certain"， we will use the mean insert size of all reads pair in the sample
     * and 2-sigma rule as the criterion.
     *
     * @param samRecord SAMRecord instance of the single mapped end that is to be check.
     * @return True if the unmapped end is located outside reference gene.
     */
    private boolean insertSizeRangeCheck(SAMRecord samRecord){
        if (samRecord.getReadNegativeStrandFlag()){
            return samRecord.getAlignmentStart() < (this.meanInsertSize - samRecord.getReadLength() + 2 * this.insertSizeSD);
        } else {
            int geneLen = this.referenceInfoMatrix.getGeneLength(samRecord.getReferenceName());
            if (geneLen > 0) {
                return (geneLen - samRecord.getAlignmentStart()) < (this.meanInsertSize + 2 * this.insertSizeSD);
            } else {
                return false;
            }
        }
    }
}
