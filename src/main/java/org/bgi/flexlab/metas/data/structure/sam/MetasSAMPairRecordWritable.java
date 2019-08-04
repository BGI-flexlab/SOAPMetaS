package org.bgi.flexlab.metas.data.structure.sam;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.io.Writable;
import org.seqdoop.hadoop_bam.LazyBAMRecordFactory;
import org.seqdoop.hadoop_bam.util.DataInputWrapper;
import org.seqdoop.hadoop_bam.util.DataOutputWrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ClassName: MetasSAMPairRecordWritable
 * Description: Based on org.seqdoop.hadoop_bam.SAMRecordWritable
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSAMPairRecordWritable implements Writable {
    private static final BAMRecordCodec lazyCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory());

    private MetasSAMPairRecord pairRecord = null;

    public MetasSAMPairRecord get() {
        return pairRecord;
    }

    public void set(MetasSAMPairRecord r) {
        pairRecord = r;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // In theory, it shouldn't matter whether we give a header to
        // BAMRecordCodec or not, since the representation of an alignment in BAM
        // doesn't depend on the header data at all. Only its interpretation
        // does, and a simple read/write codec shouldn't really have anything to
        // say about that. (But in practice, it already does matter for decode(),
        // which is why LazyBAMRecordFactory exists.)
        SAMRecord rec1 = pairRecord.getFirstRecord();
        final BAMRecordCodec codec = new BAMRecordCodec(rec1.getHeader());
        codec.setOutputStream(new DataOutputWrapper(out));
        codec.encode(rec1);
        codec.encode(pairRecord.getSecondRecord());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lazyCodec.setInputStream(new DataInputWrapper(in));

        SAMRecord rec1 = lazyCodec.decode();
        pairRecord.setFirstRecord(rec1);

        if (rec1.getReadPairedFlag()){
            pairRecord.setSecondRecord(lazyCodec.decode());
            pairRecord.setPaired(true);
        } else {
            pairRecord.setPaired(false);
        }
    }

    @Override
    public String toString() {
        if (pairRecord == null) {
            return "NULL_Pair_Record";
        }
        return pairRecord.toString(); // remove trailing newline
    }
}
