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
import org.bgi.flexlab.metas.SOAPMetas;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecordWritable;
import org.bgi.flexlab.metas.data.structure.sam.SAMMultiSampleList;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

import java.io.*;

/**
 * ClassName: MetasSAMWFRecordReader
 * Description: Read multiple SAM records having the same read Name and Construct MetasSAMPairRecord.
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSAMWFRecordReader extends RecordReader<Text, MetasSAMPairRecordWritable> {

    protected static final Log LOG = LogFactory.getLog(MetasSAMWFRecordReader.class.getName());

    private Text key = new Text();
    private MetasSAMPairRecordWritable recordWr = new MetasSAMPairRecordWritable();

    private FSDataInputStream input;
    private SAMRecordIterator iterator;
    private long start, end;
    private boolean isInitialized = false;

    private WorkaroundingStream waInput;

    private int sampleID;
    private String splitFileName;

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
        this.splitFileName = file.getName();

        LOG.info("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] Current split file: "  +
                file.getName() + " File position: " + this.start + " Split length: " + split.getLength());

        //添加
        String samSampleListPath = conf.get("metas.data.mapreduce.input.samsamplelist");
        //LOG.trace("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] SAM sample list configure metas.data.mapreduce.input.samsamplelist is " + samSampleListPath);

        if (samSampleListPath != null && !samSampleListPath.equals("")) {
            SAMMultiSampleList samMultiSampleList = new SAMMultiSampleList(samSampleListPath,
                    true, true, false);
            sampleID = samMultiSampleList.getSampleID(file.getName());
            //LOG.trace("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] Current sample ID is: " + sampleID);
        } else {
            LOG.error("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] Please provide multisample information list, or the partitioning may be uncontrollable.");
            sampleID = 0;
        }

        LOG.info("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] Start read SAM of sample " + sampleID + " , split " + this.splitFileName);

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

        final SAMFileHeader header = createSamReader(input, stringency).getFileHeader(); // customized header test conceal

        waInput = new WorkaroundingStream(input, header); // customized header test conceal

        final boolean firstSplit = this.start == 0;


        if (firstSplit) {
            // Skip the header because we already have it, and adjust the start
            // to match.
            final int headerLength = waInput.getRemainingHeaderLength();
            ////final SAMFileHeader splitHeader = createSamReader(input, stringency).getFileHeader(); customized header test
            ////final int headerLength = getHeaderLength(splitHeader); customized header test
            input.seek(headerLength);
            this.start += headerLength;
        } else
            input.seek(--this.start);

        //if (!firstSplit) { customized header test
        //    input.seek(--this.start); customized header test
        //} customized header test

        //final SAMFileHeader header = SOAPMetas.getHeader(new Path("file:///hwfssz1/BIGDATA_COMPUTING/huangzhibo/workitems/SOAPMeta/SRS014287_header.sam"), conf); customized header test
        //waInput = new WorkaroundingStream(input, header); customized header test
        // Creating the iterator causes reading from the stream, so make sure
        // to start counting this early.
        waInput.setLength(this.end - this.start);

        iterator = createSamReader(waInput, stringency).iterator();

        if (!firstSplit) {
            // Skip the first line, it'll be handled with the previous split.
            try {
                if (iterator.hasNext())
                    iterator.next();
                    //LOG.trace("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] SAM file skipped first line: " + iterator.next());
            } catch (SAMFormatException e) {
                LOG.error("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] SAM format is not correct. File: " + this.splitFileName);
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

                MetasSAMPairRecord pairRecord = new MetasSAMPairRecord(this.lastRecord, null);
                pairRecord.setPaired(true);
                recordWr.set(pairRecord);

                pairRecord = null;
                this.lastRecord = null;
                return true;
            }
            LOG.info("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] Finish reading SAM of sample " + sampleID + " , split " + this.splitFileName);
            return false;
        }

        SAMRecord rec1 = null;
        SAMRecord rec2 = null;

        int count1 = 0;
        int count2 = 0;

        SAMRecord tempRec = null;

        String lastReadName;

        if (this.lastRecord == null){
            this.lastRecord = iterator.next();
            while (this.lastRecord.getReadUnmappedFlag()){
                this.lastRecord = iterator.next();
            }
        }

        if (this.lastRecord.getFirstOfPairFlag()){
            count1++;
            rec1 = this.lastRecord;
        } else {
            count2++;
            rec2 = this.lastRecord;
        }

        lastReadName = this.lastRecord.getReadName();
        this.lastRecord = null;

        while (iterator.hasNext()) {

            tempRec = iterator.next();
            if (tempRec.getReadUnmappedFlag()){
                continue;
            }

            //LOG.trace("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] SAM file readin line: " + tempRec);

            if (tempRec.getReadName().equals(lastReadName)) {
                if (tempRec.getFirstOfPairFlag()){
                    count1++;
                    rec1 = tempRec;
                } else {
                    count2++;
                    rec2 = tempRec;
                }
            } else {
                this.lastRecord = tempRec;
                break;
            }
        }

        tempRec = null;
        //if (!r.getReadUnmappedFlag()) {
        //    record.set(r);
        //} else {
        //    record.set(null);
        //}

        //LOG.trace("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] Record element: key: " + key.toString() +
        //        " || value: " + r.toString());

        key.set(Integer.toString(sampleID));

        MetasSAMPairRecord pairRecord = null;
        if (count1 == 1){
            if (count2 == 1){
                pairRecord = new MetasSAMPairRecord(rec1, rec2);

                if (rec1.getProperPairFlag()) {
                    pairRecord.setProperPaired(true);
                }
            } else {
                pairRecord = new MetasSAMPairRecord(rec1, null);
            }
            pairRecord.setPaired(true);
        } else {
            if (count2 == 1){
                pairRecord = new MetasSAMPairRecord(rec2, null);
                pairRecord.setPaired(true);
            }
        }
        recordWr.set(pairRecord);

        rec1 = null;
        rec2 = null;

        //LOG.info("[SOAPMetas::" + MetasSAMWFRecordReader.class.getName() + "] Current recordWr: " + recordWr.toString() + " | Current pairRecord: " + pairRecord.toString());

        return true;
    }

    public int getHeaderLength(SAMFileHeader header){
        String text = header.getTextHeader();
        if (text == null) {
            StringWriter writer = new StringWriter();
            new SAMTextHeaderCodec().encode(writer, header);
            text = writer.toString();
        }
        byte[] b;
        try {
            b = text.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            b = null;
            assert false;
        }
        return b.length;
    }
}

// See the long comment in SAMRecordReader.initialize() for what this does.
class WorkaroundingStream extends InputStream {
    private final InputStream stream, headerStream;
    private boolean headerRemaining;
    private long length;
    private int headerLength;

    private boolean lookingForEOL = false,
            foundEOL      = false,
            strippingAts  = false; // HACK, see read(byte[], int, int).

    public WorkaroundingStream(InputStream stream, SAMFileHeader header) {
        this.stream = stream;

        String text = header.getTextHeader();
        if (text == null) {
            StringWriter writer = new StringWriter();
            new SAMTextHeaderCodec().encode(writer, header);
            text = writer.toString();
        }
        byte[] b;
        try {
            b = text.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            b = null;
            assert false;
        }
        headerRemaining = true;
        headerLength    = b.length;
        headerStream    = new ByteArrayInputStream(b);

        this.length = Long.MAX_VALUE;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public int getRemainingHeaderLength() {
        return headerLength;
    }

    private byte[] readBuf = new byte[1];
    @Override public int read() throws IOException {
        for (;;) switch (read(readBuf)) {
            case  0: continue;
            case  1: return readBuf[0];
            case -1: return -1;
        }
    }

    @Override public int read(byte[] buf, int off, int len) throws IOException {
        if (!headerRemaining)
            return streamRead(buf, off, len);

        int h;
        if (strippingAts)
            h = 0;
        else {
            h = headerStream.read(buf, off, len);
            if (h == -1) {
                // This should only happen when there was no header at all, in
                // which case Picard doesn't throw an error until trying to read
                // a record, for some reason. (Perhaps an oversight.) Thus we
                // need to handle that case here.
                assert (headerLength == 0);
                h = 0;
            } else if (h < headerLength) {
                headerLength -= h;
                return h;
            }
            strippingAts = true;
            headerStream.close();
        }

        final int newOff = off + h;
        int s = streamRead(buf, newOff, len - h);

        if (s <= 0)
            return strippingAts ? s : h;

        // HACK HACK HACK.
        //
        // We gave all of the header, which means that SAMFileReader is still
        // trying to read more header lines. If we're in a split that isn't at
        // the start of the SAM file, we could be in the middle of a line and
        // thus see @ characters at the start of our data. Then SAMFileReader
        // would try to understand those as header lines and the end result is
        // that it throws an error, since they aren't actually header lines,
        // they're just part of a SAM record.
        //
        // So, if we're done with the header, strip all @ characters we see. Thus
        // SAMFileReader will stop reading the header there and won't throw an
        // exception until we use its SAMRecordIterator, at which point we can
        // catch it, because we know to expect it.
        //
        // headerRemaining remains true while it's possible that there are still
        // @ characters coming.

        int i = newOff-1;
        while (buf[++i] == '@' && --s > 0);

        if (i != newOff)
            System.arraycopy(buf, i, buf, newOff, s);

        headerRemaining = s == 0;
        return h + s;
    }
    private int streamRead(byte[] buf, int off, int len) throws IOException {
        if (len > length) {
            if (foundEOL)
                return 0;
            lookingForEOL = true;
        }
        int n = stream.read(buf, off, len);
        if (n > 0) {
            n = tryFindEOL(buf, off, n);
            length -= n;
        }
        return n;
    }
    private int tryFindEOL(byte[] buf, int off, int len) {
        assert !foundEOL;

        if (!lookingForEOL || len < length)
            return len;

        // Find the first EOL between length and len.

        // len >= length so length fits in an int.
        int i = Math.max(0, (int)length - 1);

        for (; i < len; ++i) {
            if (buf[off + i] == '\n') {
                foundEOL = true;
                return i + 1;
            }
        }
        return len;
    }

    @Override public void close() throws IOException {
        stream.close();
    }

    @Override public int available() throws IOException {
        return headerRemaining ? headerStream.available() : stream.available();
    }
}
