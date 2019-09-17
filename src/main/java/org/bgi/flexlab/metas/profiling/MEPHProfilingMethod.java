package org.bgi.flexlab.metas.profiling;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;

import java.io.Serializable;

/**
 * ClassName: MEPHProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class MEPHProfilingMethod extends ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;

    public MEPHProfilingMethod(MetasOptions options, JavaSparkContext jsc){
        super(options, jsc);
    }

    /**
     * TODO: 需要完善基于METAPHLAN策略的丰度计算方法。
     *
     * @param readMetasSamPairRDD
     * @return
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD, Partitioner partitioner){
        return null;
    }

}
