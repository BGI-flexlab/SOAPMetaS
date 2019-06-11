package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingEveResultRecord;

import java.io.IOException;

/**
 * ClassName: ProfilingEveOutputFormat
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingEveOutputFormat extends MetasAbunOutputFormat<String, ProfilingEveResultRecord> {

    private TextOutputFormat<String, ProfilingEveResultRecord> textOutForm = null;

    @Override
    protected String generateFileNameForKeyValue(String key, ProfilingEveResultRecord value, String outDir, String appName) {
        return outDir + "/" + appName + "-Profiling-SAMPLE_" + value.getSmTag() + ".abundance.evaluation";
    }

    @Override
    protected RecordWriter<String, ProfilingEveResultRecord> getBaseRecordWriter(TaskAttemptContext taskAttemptContext, String outputFile) throws IOException, InterruptedException {
        if (textOutForm == null) {
            textOutForm = new TextOutputFormat<String, ProfilingEveResultRecord>() {
                @Override
                public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
                    return new Path(outputFile);
                }
            };
        }

        return textOutForm.getRecordWriter(taskAttemptContext);
    }
}
