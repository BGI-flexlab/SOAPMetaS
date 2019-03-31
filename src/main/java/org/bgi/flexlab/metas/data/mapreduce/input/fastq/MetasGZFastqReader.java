package org.bgi.flexlab.metas.data.mapreduce.input.fastq;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.metas.data.structure.fastq.FastqMultiSampleList;
import org.bgi.flexlab.metas.data.structure.fastq.FastqSampleList;

import java.io.IOException;

/**
 * The Class is based on org.bgi.flexlab.gaealib.input.fastq.GZFastqReader.
 *
 * *Changes:
 *  + Rename class as "MetasGZFastqReader"
 *  + Modify the assignment of sampleID in constructor. The default samapleID without sampleList
 *  file is the hashcode of file split's path.
 *  + Change the type of "key" from Text to IntWritable.
 *  + Add readGroupID field.
 *  + The default readGroupID without sampleList file is the file path name(file.getName()).
 *  + Add readGrouID to the value for each record.
 *
 */

public class MetasGZFastqReader implements RecordReader<Text, Text> {
	protected static final Log LOG = LogFactory.getLog(MetasGZFastqReader.class.getName());

	protected CompressionCodecFactory compressionCodecs = null;
	protected long start;
	protected long pos;
	protected long end;
	protected LineReader in;
	protected int maxLineLength;
	protected byte[] recordDelimiterBytes;
	protected String firstLine = "";
	protected int sampleID;
	protected String readGroupID;
	protected int recordCount = 0;
	protected long fileLength;

	public void getFirstFastqLine() throws IOException {
		Text tmpline = new Text();
		int size;
		while ((size = in.readLine(tmpline, maxLineLength, Math.max(
				(int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength))) != 0) {
			start += size;
			if (tmpline.toString().startsWith("@")) {
				firstLine = tmpline.toString();
				break;
			}
		}
	}

	public MetasGZFastqReader(Configuration jobConf, FileSplit fileSplit, byte[] recordDelimiter) throws IOException {

		final Path file = fileSplit.getPath();
		compressionCodecs = new CompressionCodecFactory(jobConf);

		if (compressionCodecs.getCodec(file) != null) {
			LOG.error(this.getClass().toString() + " Fastq File Codec Error. Please decompress input file.");
			pos = end;
		}

		this.maxLineLength = jobConf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();

		FileSystem fs = file.getFileSystem(jobConf);
		fileLength = fs.getFileStatus(file).getLen();

		if (null != recordDelimiter) {
			this.recordDelimiterBytes = recordDelimiter;
		}

		// System.err.println("split:" + split.getPath().toString());
		String sampleListPath = jobConf.get("metas.data.mapreduce.input.fqmultisamplelist");
		if (sampleListPath != null && !sampleListPath.equals("")) {
			FastqMultiSampleList fastqMultiSampleList;
			fastqMultiSampleList = new FastqMultiSampleList(sampleListPath, true, false);
			FastqSampleList slist = fastqMultiSampleList.getSampleList(fileSplit.getPath().toString());
			if (slist != null) {
				sampleID = slist.getSampleID();
				readGroupID = slist.getRgID();
			} else {
				LOG.error("Please provide multisample information for " + file.toString() +
						" . Pr the partitioning may be uncontrollable.");
				sampleID = fastqMultiSampleList.getSampleCount() + 1;
				readGroupID = file.getName().replaceFirst("((\\.fq)|(\\.fastq))$", "");
			}
		} else {
			LOG.error("Please provide multisample information list, or the partitioning may be uncontrollable.");
			sampleID = 1;
			readGroupID = file.getName().replaceFirst("((\\.fq)|(\\.fastq))$", "");
		}

		// open the file and seek to the start of the split
		FSDataInputStream fileIn = fs.open(fileSplit.getPath());
		boolean skipFirstLine = false;

		if (start != 0) {
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}
		if (null == this.recordDelimiterBytes) {
			in = new LineReader(fileIn, jobConf);
		} else {
			in = new LineReader(fileIn, jobConf, this.recordDelimiterBytes);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		getFirstFastqLine();
		this.pos = start;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	public Text createKey() {
		return new Text();
	}

	public Text createValue() {
		return new Text();
	}

	public synchronized long getPos() throws IOException {
		return pos;
	}

	public boolean next(Text key, Text value) throws IOException {
		if (key == null) {
			key = new Text();
		}
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		boolean iswrongFq = false;

		while (pos < end) {
			Text tmp = new Text();
			String[] st = new String[4];
			int startIndex = 0;

			if (!firstLine.equals("")) {
				st[0] = firstLine;
				startIndex = 1;
				firstLine = "";
			}

			for (int i = startIndex; i < 4; i++) {
				newSize = in.readLine(tmp, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));

				if (newSize == 0) {
					iswrongFq = true;
					break;
				}
				pos += newSize;
				st[i] = tmp.toString();
			}

			if(st[0].charAt(0)=='@' && st[1].charAt(0)=='@'){
				// st数组元素前移一位
				System.arraycopy(st, 1, st, 0, 3);
				newSize = in.readLine(tmp, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));

				if (newSize == 0) {
					iswrongFq = true;
					break;
				}
				pos += newSize;
				st[3] = tmp.toString();
			}

			if (!iswrongFq) {
				int index = st[0].lastIndexOf("/");
				if (index < 0) {
					String[] splitTmp = StringUtils.split(st[0], " ");
					char ch;
					if(splitTmp.length == 1) {
						ch = '1';
						st[0] = splitTmp[0] + "/" + ch;
					}else {
						ch = splitTmp[1].charAt(0);
						if (ch != '1' && ch != '2')
							throw new RuntimeException("error fq format at reads:"
								+ st[0]);

						st[0] = splitTmp[0] + "__" + splitTmp[1].substring(1) + "/" + ch;
					}
					index = st[0].lastIndexOf("/");
				}
				String tempkey = st[0].substring(1, index).trim();
				char keyIndex = st[0].charAt(index + 1);

				// key: sampleID#readName. example: <Text>
				// value: mateIndex(1 or 2)##sampleID	pos	filelength##readGroupID##sequence	quality
				//key.set(sampleID + "#" +tempkey);
				//value.set(keyIndex + "##" + sampleID + "\t" + pos + "\t" + fileLength + "##" + readGroupID + "##" + st[1] + "\t" + st[3]);

				// new key: sampleID	readName
				// new value: mateIndex(1 or 2)##readGroupID##sequence	quality
				key.set(sampleID + "\t" + tempkey);
				value.set(keyIndex + "##" + readGroupID + "##" + st[1] + "\t" + st[3]);

				recordCount++;
			} else {
				LOG.warn("wrong fastq reads:blank line among fq file or end of file!");
			}
			break;
		}
		return !(newSize == 0 || iswrongFq);
	}
}
