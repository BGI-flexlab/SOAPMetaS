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
 * ReadGroupID(SAM RG:Z:)	read1_path	read2_path
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
			sampleList = new ArrayList<>();
		}
		if (recordPath){
			filePath = new StringBuilder();
		}
		
		String line;
		while((line = reader.readLine()) != null) {
			//LOG.trace("[SOAPMetas::" + FastqMultiSampleList.class.getName() + "] Sample line: " + line);
			if(line.length() == 0) {
				continue;
			}

			String[] items = StringUtils.split(line, '\t');

			FastqSampleList slist;

			if (items.length == 3){
				if (isLocal){
					if (!items[1].startsWith("file://")){
						items[1] = "file://" + items[1];
					}
					if (!items[2].startsWith("file://")){
						items[2] = "file://" + items[2];
					}
				}
				slist = new FastqSampleList();
				slist.setSampleList(items[0], items[1], items[2], sampleCount);
				this.sampleCount++;
			} else if (items.length == 2){
				if(isLocal){
					if (!items[1].startsWith("file://")){
						items[1] = "file://" + items[1];
					}
				}
				slist = new FastqSampleList();
				slist.setSampleList(items[0], items[1], null, sampleCount);
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

		if (recordPath) {
			filePath.trimToSize();
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
