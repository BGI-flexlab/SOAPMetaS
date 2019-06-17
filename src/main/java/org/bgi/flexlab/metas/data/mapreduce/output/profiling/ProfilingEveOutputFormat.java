package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.bgi.flexlab.metas.data.structure.profiling.ProfilingEveResultRecord;

/**
 * ClassName: ProfilingEveOutputFormat
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingEveOutputFormat<K, V> extends ProfilingOutputFormatBase<K, V> {

    @Override
    protected String generateFileNameFromKeyValue(K key, V value, String appName) {
        // outputProfilingFile = this.outputDir + "/" + this.appName + "-Profiling-SAMPLE_" + smTag + ".abundance.evaluation";
        return appName + "-Profiling-SAMPLE_" + getSmTag(value) + ".abundance.evaluation";
    }

    private String getSmTag(Object rec){
        if (rec instanceof ProfilingEveResultRecord) {
            return ((ProfilingEveResultRecord) rec).getSmTag();
        } else {
            return "NOSMTAG";
        }
    }
}
