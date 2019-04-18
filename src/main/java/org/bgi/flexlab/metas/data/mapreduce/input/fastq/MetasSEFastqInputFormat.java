package org.bgi.flexlab.metas.data.mapreduce.input.fastq;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * ClassName: MetasSEFastqInputFormat
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSEFastqInputFormat extends FileInputFormat<Text, Text> {
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(
                context.getConfiguration()).getCodec(file);
        return codec == null;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext tac)
            throws IOException, InterruptedException {
        final RecordReader<Text, Text> reader = new MetasSEFastqReader();
        reader.initialize(genericSplit, tac);
        return reader;
    }

}
