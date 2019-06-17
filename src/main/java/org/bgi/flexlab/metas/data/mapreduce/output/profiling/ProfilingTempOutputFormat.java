package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;

import java.io.IOException;

/**
 * ClassName: ProfilingTempOutputFormat
 * Description:
 *
 * @author: heshixu@genomics.cn
 */
@Deprecated
public class ProfilingTempOutputFormat extends ProfilingTempOutputFormatBase<String, ProfilingResultRecord> {

    private TextOutputFormat<String, ProfilingResultRecord> textOutForm = null;

    @Override
    protected String generateFileNameForKeyValue(String key, ProfilingResultRecord value, String outDir, String appName) {
        return outDir + "/" + appName + "-Profiling-SAMPLE_" + value.getSmTag() + ".abundance";
    }

    @Override
    protected RecordWriter<String, ProfilingResultRecord> getBaseRecordWriter(TaskAttemptContext taskAttemptContext, String outputFile) throws IOException, InterruptedException {
        if (textOutForm == null) {
            textOutForm = new TextOutputFormat<String, ProfilingResultRecord>() {
                @Override
                public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
                    return new Path(outputFile);
                }
            };
        }

        return textOutForm.getRecordWriter(taskAttemptContext);
    }
}
