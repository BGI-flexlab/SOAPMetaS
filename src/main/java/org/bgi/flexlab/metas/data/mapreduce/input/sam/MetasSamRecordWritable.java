package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import htsjdk.samtools.BAMRecordCodec;

import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory;
import org.seqdoop.hadoop_bam.util.DataInputWrapper;
import org.seqdoop.hadoop_bam.util.DataOutputWrapper;

/**
 * ClassName: MetasSamRecordWritable
 * Description: The class is based on org.seqdoop.hadoop_bam.SAMRecordWritable. The main script body is
 *  copied from the original script. Here we change SAMRecord to MetasSamRecord.
 *
 * @author heshixu@genomics.cn
 */

public class MetasSamRecordWritable implements Writable {
    private static final BAMRecordCodec lazyCodec =
            new BAMRecordCodec(null, new LazyBAMRecordFactory());

    private MetasSamRecord record;

    public MetasSamRecord get()            { return record; }
    public void      set(MetasSamRecord r) { record = r; }

    @Override public void write(DataOutput out) throws IOException {
        // In theory, it shouldn't matter whether we give a header to
        // BAMRecordCodec or not, since the representation of an alignment in BAM
        // doesn't depend on the header data at all. Only its interpretation
        // does, and a simple read/write codec shouldn't really have anything to
        // say about that. (But in practice, it already does matter for decode(),
        // which is why LazyBAMRecordFactory exists.)
        final BAMRecordCodec codec = new BAMRecordCodec(record.getHeader());
        codec.setOutputStream(new DataOutputWrapper(out));
        codec.encode(record);
    }
    @Override public void readFields(DataInput in) throws IOException {
        lazyCodec.setInputStream(new DataInputWrapper(in));
        record = (MetasSamRecord) lazyCodec.decode();
    }

    @Override
    public String toString() {
        return record.getSAMString().trim(); // remove trailing newline
    }
}
