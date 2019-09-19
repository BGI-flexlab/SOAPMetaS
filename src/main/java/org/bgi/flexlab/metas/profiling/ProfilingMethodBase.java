package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMRecord;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;

import java.io.Serializable;
import java.util.HashMap;


/**
 * ClassName: ProfilingMethodBase
 * Description: 基本的profiling功能抽象。
 *
 * @author heshixu@genomics.cn
 */

public abstract class ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;

    protected ProfilingAnalysisMode profilingAnalysisMode;
    protected ProfilingAnalysisLevel profilingAnalysisLevel;
    protected SequencingMode sequencingMode;


    public ProfilingMethodBase(MetasOptions options, JavaSparkContext jsc){
        this.profilingAnalysisMode = options.getProfilingAnalysisMode();
        this.profilingAnalysisLevel = options.getProfilingAnalysisLevel();
        this.sequencingMode = options.getSequencingMode();
    }

    /**
     * The key step of profiling process, including two transformations flatMapToPair() and reduceBykey(),
     * and the input RDD has type of JavaPairRDD<String, MetasSAMPairRecord> while the output type is
     * JavaPairRDD<String, ProfilingResultRecord>.
     *
     * The PairFlatMapFunction in flatMapToPair() is for computing the reads count of each marker, the
     * result will be recalibrated by GCBias(Default)Model. Note that the count value represents merely one
     * single read.
     *
     * The Function2 in reduceByKey() is for merging the read count result from flatMapToPair(). Note that
     * some pipeline, MetaPhlAn2 for example, use all counts of one marker to do optimization, so
     * reduceByKey should be replaced by groupByKey()?
     *
     * @return JavaPairRDD<String, ProfilingResultRecord> A new RDD containing profiling result.
     * ProfilingResultRecord contains cluster name (marker gene, species name or read group id), read count
     * of cluster, recalibrated abundance, and name list of all mapped reads(ProfilingAnalysisLevel.ESTIMATE).
     */
    public abstract JavaPairRDD<String, ProfilingResultRecord> runProfiling(
            JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD, Partitioner partitioner);

    public abstract JavaPairRDD<String, ProfilingResultRecord> runProfiling(
            JavaRDD<SAMRecord> samRecordJavaRDD, JavaSparkContext ctx);

    public void destroyContent(){
        return;
    }

    public abstract void setSampleIDbySampleName (HashMap<String, Integer> sampleIDbySampleName);
}
