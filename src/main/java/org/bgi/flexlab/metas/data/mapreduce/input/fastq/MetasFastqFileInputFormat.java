package org.bgi.flexlab.metas.data.mapreduce.input.fastq;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * The class is based on org.bgi.flexlab.gaealib.input.fastq.FqText.
 *
 * *Changes:
 *  + Rename class as "MetasFastqFileInputFormat".
 *  + Change the type of "key" from type Text to type IntWritable.
 */

public class MetasFastqFileInputFormat extends FileInputFormat<Text, Text> {
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(
                context.getConfiguration()).getCodec(file);
        return codec == null;
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(
            org.apache.hadoop.mapred.InputSplit genericSplit, JobConf job,
            Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes();

        return new MetasGZFastqReader(job, (FileSplit) genericSplit, recordDelimiterBytes);
    }

}
