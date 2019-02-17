package org.bgi.flexlab.metas.profiling.filter;

import org.apache.commons.math3.fitting.GaussianCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
import org.apache.spark.api.java.JavaPairRDD;
import org.bgi.flexlab.metas.io.referenceio.ReferenceInformation;
import org.bgi.flexlab.metas.io.samio.MetasSamPairRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamRecord;
import org.bgi.flexlab.metas.profiling.ProfilingUtils;
import scala.Tuple2;

import java.util.List;

/**
 * ClassName: MetasSamRecordInsertSizeFilter
 * Description: Filter for insert size of paired-end sequencing data. The insert size values of
 * paired-end data are not fixed which can be caused by experimental factors, environmental factors
 * or many other factors.
 *
 * Note: The filter is merely suitable for pair-end sequencing mode.
 *
 * TODO: 根据实际测序数据分析，ins的分布并不是标准的正态分布模式，所以fitter需要重新考虑。
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSamRecordInsertSizeFilter implements MetasSamRecordFilter {

    private int meanInsertSize;

    private int inserSizeSD = 0;

    private int insTolerance = 100;

    private boolean doTraining = false;

    private ReferenceInformation referenceInformation;

    public MetasSamRecordInsertSizeFilter(int insertSize, ReferenceInformation refInfo){
        this.meanInsertSize = insertSize;
        this.referenceInformation = refInfo;
    }

    @Override
    public boolean filter(MetasSamRecord record){
        if (this.doTraining){
            return;
        } else {
            return this.satisfyInsertSizeThreshold(record);
        }
        return false;
    }

    public void setMeanInsertSize(int meanIns){
        this.meanInsertSize = meanIns;
    }

    public void setInserSizeSD(int inserSizeSD) {
        this.inserSizeSD = inserSizeSD;
    }

    /**
     * Collect insert size data of all properly-aligned read pair (pair-end sequencing mode) and construct
     * the (insert_size, fragment_count) data points (fragment_count means read-pair count). And use
     * GaussianCurveFitter in org.apache.common.math3.fitting package to fit the data points, gain the
     * mean insert size and standard deviation.
     *
     * @param readMetasSamPairRDD
     */
    public void training(JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD){
        List<WeightedObservedPoint> pointList = readMetasSamPairRDD.values()
                .filter(pairRec -> pairRec.isProperPaired())
                .mapToPair(pairRec -> new Tuple2<>(ProfilingUtils.computeInsertSize(pairRec.getFirstRecord(), pairRec.getSecondRecord()), 1))
                .reduceByKey((a, b) -> a+b)
                .map(tup -> new WeightedObservedPoint(1.0, tup._1, tup._2))
                .collect();
        GaussianCurveFitter.ParameterGuesser guesser = new GaussianCurveFitter.ParameterGuesser(pointList);

        this.setMeanInsertSize((int) guesser.guess()[1]);
        this.setInserSizeSD((int) guesser.guess()[2]);

        this.doTraining = true;
    }

    /**
     *
     * @param samRecord The instance of MetasSamRecord in pair-end sequencing mode which is not properly
     *                  mapped as pair.
     * @return true if the mate read is probably mapped to the sequence region outside around reference marker.
     */
    private Boolean satisfyInsertSizeThreshold(MetasSamRecord samRecord){

        if (samRecord.getReadNegativeStrandFlag()){
            return samRecord.getAlignmentStart() < (this.meanInsertSize - samRecord.getReadLength() + this.insTolerance);
        } else {
            return (this.referenceInformation.getReferenceLength(samRecord.getReferenceName())-samRecord.getAlignmentStart())
                    < (this.meanInsertSize + this.insTolerance);
        }
    }
}
