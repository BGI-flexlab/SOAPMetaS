package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;

/**
 * ClassName: ProfilingOutputFormat
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingOutputFormat<K, V> extends ProfilingOutputFormatBase<K, V> {

    @Override
    protected String generateFileNameFromKeyValue(K key, V value, String appName) {
        // outputProfilingFile = this.outputDir + "/" + this.appName + "-Profiling-SAMPLE_" + smTag + ".abundance";
        return appName + "-Profiling-SAMPLE_" + getSmTag(value) + ".abundance";
    }

    private String getSmTag(Object rec){
        if (rec instanceof ProfilingResultRecord) {
            return ((ProfilingResultRecord) rec).getSmTag();
        } else {
            return "NOSMTAG";
        }
    }
}
