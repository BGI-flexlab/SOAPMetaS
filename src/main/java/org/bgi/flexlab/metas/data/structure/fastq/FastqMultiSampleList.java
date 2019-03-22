package org.bgi.flexlab.metas.data.structure.fastq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

public class FastqMultiSampleList {

	private ArrayList<FastqSampleList> fastqSampleList = new ArrayList<>();
	private int sampleCount = 0;
	private StringBuilder filePath;

	public FastqMultiSampleList(String list, boolean isLocal, boolean recordPath) throws IOException {
		File file = new File(list);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		
		String line;
		while((line = reader.readLine()) != null) {
			if(line.length() == 0) {
				continue;
			}

			String[] items = line.split("\t");

			FastqSampleList slist = new FastqSampleList();;

			if (items.length == 3){
				if (isLocal){
					if (!items[1].startsWith("file://")){
						items[1] = "file://" + items[1];
					}
					if (!items[2].startsWith("file://")){
						items[2] = "file://" + items[2];
					}
				}
				slist.setSampleList(items[0], items[1], items[2], sampleCount);
				this.sampleCount++;
			} else if (items.length == 2){
				if(isLocal){
					if (!items[1].startsWith("file://")){
						items[1] = "file://" + items[1];
					}
				}
				slist.setSampleList(items[0], items[1], null, sampleCount);
				this.sampleCount++;
			} else {
				slist = null;
				continue;
			}

			if (recordPath) {
				filePath.append(slist.getFastqPathString());
			}
		}
		filePath.trimToSize();
		reader.close();
	}
	
	public FastqSampleList getSampleList(String fqName) {
		for(FastqSampleList slist : fastqSampleList) {
			if(slist.getFastq1().contains(fqName) || slist.getFastq2().contains(fqName)) {
				return slist;
			}
		}
		return null;
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
