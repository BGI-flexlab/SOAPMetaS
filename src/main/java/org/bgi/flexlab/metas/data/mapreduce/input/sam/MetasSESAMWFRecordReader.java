package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import htsjdk.samtools.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecordWritable;
import org.bgi.flexlab.metas.data.structure.sam.SAMMultiSampleList;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * ClassName: MetasSESAMWFRecordReader
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSESAMWFRecordReader extends RecordReader<Text, MetasSAMPairRecordWritable> {

    protected static final Log LOG = LogFactory.getLog(MetasSESAMWFRecordReader.class.getName());

    private Text key = new Text();
    private MetasSAMPairRecordWritable recordWr = new MetasSAMPairRecordWritable();

    private FSDataInputStream input;
    private SAMRecordIterator iterator;
    private long start, end;
    private boolean isInitialized = false;

    private WorkaroundingStream waInput;

    private int sampleID;

    private SAMRecord lastRecord = null;

    @Override
    public void initialize(InputSplit spl, TaskAttemptContext ctx) throws IOException {
        // This method should only be called once (see Hadoop API). However,
        // there seems to be disagreement between implementations that call
        // initialize() and Hadoop-BAM's own code that relies on
        // {@link SAMInputFormat} to call initialize() when the reader is
        // created. Therefore we add this check for the time being.
        if(isInitialized)
            close();
        isInitialized = true;

        final FileSplit split = (FileSplit) spl;

        this.start =         split.getStart();
        this.end   = start + split.getLength();

        final Configuration conf = ctx.getConfiguration();

        final ValidationStringency stringency =
                SAMHeaderReader.getValidationStringency(conf);

        final Path file = split.getPath();
        final FileSystem fs = file.getFileSystem(conf);

        LOG.info("[SOAPMetas::" + MetasSESAMWFRecordReader.class.getName() + "] Current split file: "  +
                file.getName() + " File position: " + this.start + " Split length: " + split.getLength());

        String samSampleListPath = conf.get("metas.data.mapreduce.input.samsamplelist");
        //LOG.trace("[SOAPMetas::" + MetasSESAMWFRecordReader.class.getName() + "] SAM sample list configure metas.data.mapreduce.input.samsamplelist is " + samSampleListPath);

        if (samSampleListPath != null && !samSampleListPath.equals("")) {
            SAMMultiSampleList samMultiSampleList = new SAMMultiSampleList(samSampleListPath,
                    true, true, false);
            sampleID = samMultiSampleList.getSampleID(file.getName());
        } else {
            LOG.error("[SOAPMetas::" + MetasSESAMWFRecordReader.class.getName() + "] Please provide multisample information list, or the partitioning may be uncontrollable.");
            sampleID = 0;
        }

        LOG.info("[SOAPMetas::" + MetasSESAMWFRecordReader.class.getName() + "] Start read split " + file.getName());

        input = fs.open(file);

        // SAMFileReader likes to make our life difficult, so complexity ensues.
        // The basic problem is that SAMFileReader buffers its input internally,
        // which causes two issues.
        //
        // Issue #1 is that SAMFileReader requires that its input begins with a
        // SAM header. This is not fine for reading from the middle of a file.
        // Because of the buffering, if we have the reader read the header from
        // the beginning of the file and then seek to where we want to read
        // records from, it'll have buffered some records from immediately after
        // the header, which is no good. Thus we need to read the header
        // separately and then use a custom stream that wraps the input stream,
        // inserting the header at the beginning of it. (Note the spurious
        // re-encoding of the header so that the reader can decode it.)
        //
        // Issue #2 is handling the boundary between two input splits. The best
        // way seems to be the classic "in later splits, skip the first line, and
        // in every split finish reading a partial line at the end of the split",
        // but that latter part is a bit complicated here. Due to the buffering,
        // we can easily overshoot: as soon as the stream moves past the end of
        // the split, SAMFileReader has buffered some records past the end. The
        // basic fix here is to have our custom stream count the number of bytes
        // read and to stop after the split size. Unfortunately this prevents us
        // from reading the last partial line, so our stream actually allows
        // reading to the next newline after the actual end.

        final SAMFileHeader header = createSamReader(input, stringency).getFileHeader();

        waInput = new WorkaroundingStream(input, header);

        final boolean firstSplit = this.start == 0;

        if (firstSplit) {
            // Skip the header because we already have it, and adjust the start
            // to match.
            final int headerLength = waInput.getRemainingHeaderLength();
            input.seek(headerLength);
            this.start += headerLength;
        } else
            input.seek(--this.start);

        // Creating the iterator causes reading from the stream, so make sure
        // to start counting this early.
        waInput.setLength(this.end - this.start);

        iterator = createSamReader(waInput, stringency).iterator();

        if (!firstSplit) {
            // Skip the first line, it'll be handled with the previous split.
            try {
                if (iterator.hasNext())
                    iterator.next();
            } catch (SAMFormatException e) {
                LOG.error("[SOAPMetas::" + MetasSESAMWFRecordReader.class.getName() + "] SAM format is not correct. File: " + file.getName());
            }
        }
    }

    private SamReader createSamReader(InputStream in, ValidationStringency stringency) {
        SamReaderFactory readerFactory = SamReaderFactory.makeDefault()
                .setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
                .setUseAsyncIo(false);
        if (stringency != null) {
            readerFactory.validationStringency(stringency);
        }
        return readerFactory.open(SamInputResource.of(in));
    }

    @Override
    public void close() throws IOException {
        iterator.close();
    }

    @Override
    public float getProgress() throws IOException {
        final long pos = input.getPos();
        if (pos >= end)
            return 1;
        else
            return (float)(pos - start) / (end - start);
    }

    @Override
    public Text getCurrentKey(){
        return key;
    }

    @Override
    public MetasSAMPairRecordWritable getCurrentValue(){
        return recordWr;
    }

    @Override
    public boolean nextKeyValue(){
        if (!iterator.hasNext()) {
            if (this.lastRecord != null) {
                key.set(Integer.toString(sampleID));
                recordWr.set(new MetasSAMPairRecord(this.lastRecord, null));

                this.lastRecord = null;
                return true;
            }
            return false;
        }

        SAMRecord rec1 = null;

        SAMRecord tempRec = null;

        int count = 0;

        String lastReadName;

        if (this.lastRecord == null){
            this.lastRecord = iterator.next();
            while (this.lastRecord.getReadUnmappedFlag()){
                this.lastRecord = iterator.next();
            }
        }

        rec1 = this.lastRecord;
        count++;
        lastReadName = this.lastRecord.getReadName();
        this.lastRecord = null;

        while (iterator.hasNext()) {

            tempRec = iterator.next();
            if (tempRec.getReadUnmappedFlag()){
                continue;
            }

            if (tempRec.getReadName().equals(lastReadName)) {
                count++;
            } else {
                this.lastRecord = tempRec;
                break;
            }
        }

        //if (!r.getReadUnmappedFlag()) {
        //    record.set(r);
        //} else {
        //    record.set(null);
        //}

        //LOG.trace("[SOAPMetas::" + MetasSESAMWFRecordReader.class.getName() + "] Record element: key: " + key.toString() +
        //        " || value: " + r.toString());

        tempRec = null;

        MetasSAMPairRecord pairRecord = null;

        if (count == 1){
            pairRecord = new MetasSAMPairRecord(rec1, null);
        }

        key.set(Integer.toString(sampleID));
        recordWr.set(pairRecord);

        rec1 = null;

        return true;
    }
}
