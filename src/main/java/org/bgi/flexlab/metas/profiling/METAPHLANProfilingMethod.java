package org.bgi.flexlab.metas.profiling;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;

/**
 * ClassName: METAPHLANProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class METAPHLANProfilingMethod extends ProfilingMethodBase {

    public METAPHLANProfilingMethod(MetasOptions options){
        super(options);
    }

    /**
     * TODO: 需要完善基于METAPHLAN策略的丰度计算方法。
     *
     * @param readMetasSamPairRDD
     * @return
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD, Partitioner partitioner){
        return null;
    }

}
