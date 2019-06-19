package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecordWritable;

import java.io.IOException;

/**
 * ClassName: MetasSAMWFInputFormat
 * Description: Read SAM file as a whole.
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSAMWFInputFormat extends FileInputFormat<Text, MetasSAMPairRecordWritable> {

    @Override
    public RecordReader<Text, MetasSAMPairRecordWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx)
            throws InterruptedException, IOException {
        final RecordReader<Text, MetasSAMPairRecordWritable> rr = new MetasSAMWFRecordReader();
        rr.initialize(split, ctx);
        return rr;
    }

    @Override
    public boolean isSplitable(JobContext job, Path path){
        return false;
    }
}
