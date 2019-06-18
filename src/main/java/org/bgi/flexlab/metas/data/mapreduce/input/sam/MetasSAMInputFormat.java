package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

/**
 * ClassName: MetasSAMInputFormat
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSAMInputFormat extends FileInputFormat<Text, SAMRecordWritable> {

    @Override
    public RecordReader<Text,SAMRecordWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx)
            throws InterruptedException, IOException {
        final RecordReader<Text, SAMRecordWritable> rr = new MetasSAMRecordReader();
        rr.initialize(split, ctx);
        return rr;
    }

    @Override
    public boolean isSplitable(JobContext job, Path path){
        return super.isSplitable(job, path);
    }
}
