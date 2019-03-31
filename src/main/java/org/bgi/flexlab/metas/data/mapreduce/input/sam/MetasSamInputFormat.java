package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * ClassName: MetasSamInputFormat
 * Description: The class is based on org.seqdoop.hadoop_bam.SAMInputFormat. The main script body is
 *  *  copied from the original script. Here we changed SAMRecordWritable to MetasSamRecordWritable.
 *
 * @author heshixu@genomics.cn
 */

public class MetasSamInputFormat extends FileInputFormat<Text, MetasSamRecordWritable> {

    @Override public RecordReader<Text, MetasSamRecordWritable>
    createRecordReader(InputSplit split, TaskAttemptContext ctx)
            throws InterruptedException, IOException
    {
        final RecordReader<Text, MetasSamRecordWritable> rr = new MetasSamRecordReader();
        rr.initialize(split, ctx);
        return rr;
    }

    @Override public boolean isSplitable(JobContext job, Path path) {
        return super.isSplitable(job, path);
    }
}
