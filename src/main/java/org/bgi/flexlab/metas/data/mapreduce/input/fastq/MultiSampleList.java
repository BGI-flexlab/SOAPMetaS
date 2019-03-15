package org.bgi.flexlab.metas.data.mapreduce.input.fastq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class MultiSampleList {
	private ArrayList<SampleList> sampleList = new ArrayList<SampleList>();
	
	public MultiSampleList(String list, boolean keepFile) throws IOException {
		File file = new File(list);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		
		String line;
		while((line = reader.readLine()) != null) {
			if(line.length() == 0) {
				continue;
			}
			
			SampleList slist = new SampleList();
			
			if(slist.setSampleList(line, keepFile)) {
				//System.err.println("fq1:" + slist.getFastq1());
				//System.err.println("fq2:" + slist.getFastq2());
				sampleList.add(slist);
			}
		}
		reader.close();
	}
	
	public SampleList getID(String fqName) {
		for(SampleList slist : sampleList) {
			if(fqName.contains(slist.getFastq1()) || fqName.contains(slist.getFastq2())) {
				return slist;
			}
		}
		return null;
	}
	
	public ArrayList<SampleList> getSampleList() {
		return sampleList;
	}
}
