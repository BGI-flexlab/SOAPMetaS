package org.bgi.flexlab.metas.data.structure.fastq;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;

/**
 * Description:
 *  The class is based on org.bgi.flexlab.gaealib.input.fastq.FastqMultiSampleList.
 *
 * *Changes:
 *  +
 *
 * Note:
 * 1.Input multisample information list file format:
 * ReadGroupID(SAM RG:Z:)	sample(SMTag)	read1_path	read2_path
 * ERR0000001 HG00001 /path/to/read_1.fq [/path/to/read_2.fq]
 *
 */

public class FastqMultiSampleList implements Serializable {

	public static final long serialVersionUID = 1L;

	protected static final Log LOG = LogFactory.getLog(FastqMultiSampleList.class.getName());

	private ArrayList<FastqSampleList> sampleList = null;
	private int sampleCount = 0;
	private StringBuilder filePath;

	public FastqMultiSampleList(String listFile, boolean isLocal, boolean recordSample, boolean recordPath) throws IOException {
		File file = new File(listFile);
		FileInputStream fis = new FileInputStream(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

		if (recordSample){
			sampleList = new ArrayList<>(20);
		}
		if (recordPath){
			filePath = new StringBuilder(64);
		}
		
		String line;
		String[] items;
		while((line = reader.readLine()) != null) {
			if(line.length() == 0) {
				continue;
			}

			items = StringUtils.split(line, '\t');

			FastqSampleList slist;

			if (items.length == 4){
				if (isLocal){
					if (!items[2].startsWith("file://")){
						items[2] = "file://" + items[2];
					}
					if (!items[3].startsWith("file://")){
						items[3] = "file://" + items[3];
					}
				}
				slist = new FastqSampleList();
				slist.setSampleList(items[0], items[1], items[2], items[3], sampleCount);
				this.sampleCount++;
			} else if (items.length == 3){
				if(isLocal){
					if (!items[2].startsWith("file://")){
						items[2] = "file://" + items[2];
					}
				}
				slist = new FastqSampleList();
				slist.setSampleList(items[0], items[1], items[2], null, sampleCount);
				this.sampleCount++;
			} else {
				slist = null;
				continue;
			}

			if (recordPath) {
				filePath.append(slist.getFastqPathString());
				LOG.trace("[SOAPMetas::" + FastqMultiSampleList.class.getName() + "] Save record path: " + slist.getFastqPathString());
			}

			if (recordSample) {
				sampleList.add(slist);
				LOG.trace("[SOAPMetas::" + FastqMultiSampleList.class.getName() + "] Save sample: " + slist.toString());
			}

		}

		if (recordSample) {
			sampleList.trimToSize();
		}

		reader.close();
		fis.close();
	}
	
	public ArrayList<FastqSampleList> getSampleList() {
		return sampleList;
	}

	public int getSampleCount() {
		return sampleCount;
	}
	public String getAllFastqPath(){
		if (filePath.length() > 0) {
			return filePath.deleteCharAt(filePath.length() - 1).toString();
		} else {
			return "";
		}
	}
}
