package org.bgi.flexlab.metas.profiling;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.io.profilingio.ProfilingResultRecord;
import org.bgi.flexlab.metas.io.referenceio.ReferenceInfomation;
import org.bgi.flexlab.metas.io.samio.MetasSamPairRecord;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;


/**
 * ClassName: ProfilingMethodBase
 * Description: 基本的profiling功能抽象。
 *
 * @author: heshixu@genomics.cn
 */

public abstract class ProfilingMethodBase {

    protected ProfilingAnalysisMode profilingAnalysisMode;
    protected ProfilingAnalysisLevel profilingAnalysisLevel;
    protected SequencingMode sequencingMode;
    protected ReferenceInfomation referenceInfomation;

    protected int standardReadLength;
    protected int insertSize = Integer.MAX_VALUE;

    public ProfilingMethodBase(MetasOptions options){
        this.profilingAnalysisMode = options.getProfilingAnalysisMode();
        this.profilingAnalysisLevel = options.getProfilingAnalysisLevel();
        this.sequencingMode = options.getSequencingMode();

        this.referenceInfomation = new ReferenceInfomation(options.getReferenceMatrixFilePath());
        this.profilingAnalysisLevel = options.getProfilingAnalysisLevel();
        this.standardReadLength = options.getReadLength();

        if (this.sequencingMode.equals(SequencingMode.PAIREND)){
            this.insertSize = options.getInsertSize();
        }
    }

    /**
     * The key step of profiling process, including two transformations flatMapToPair() and reduceBykey(),
     * and the input RDD has type of JavaPairRDD<String, MetasSamPairRecord> while the output type is
     * JavaPairRDD<String, ProfilingResultRecord>.
     *
     * The PairFlatMapFunction in flatMapToPair() is for computing the reads count of each marker, the
     * result will be corrected by BiasCorrectionModel. Note that the count value represents merely one
     * single read.
     *
     * The Function2 in reduceByKey() is for merging the read count result from flatMapToPair(). Note that
     * some pipeline, MetaPhlAn2 for example, use all counts of one marker to do optimization, so
     * reduceByKey should be replaced by groupByKey()?
     *
     * @return JavaPairRDD<String, ProfilingResultRecord> A new RDD containing profiling result.
     * ProfilingResultRecord contains cluster name (marker gene, species name or read group id), read count
     * of cluster, corrected abundance, and name list of all mapped reads(ProfilingAnalysisLevel.ESTIMATE).
     */
    public abstract JavaRDD<ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD);

}
