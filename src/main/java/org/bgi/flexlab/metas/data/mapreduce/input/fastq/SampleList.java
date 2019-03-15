package org.bgi.flexlab.metas.data.mapreduce.input.fastq;

public class SampleList {
	private int id;
	
	private String readsGroup;
	
	private String fastq1;
	
	private String fastq2;
	
	private String adapter1;
	
	private String adapter2;
	
	private void setDefault() {
		fastq1 = null;
		fastq2 = null;
		adapter1 = null;
		adapter2 = null;
	}
	
	public SampleList() {
		setDefault();
	}
	
	public boolean setSampleList( String line, boolean keepFile) {
		String splitArray[] = line.split("\t");
		
		if(splitArray.length < 6) {
			return false;
		}
		
		id = Integer.parseInt(splitArray[0]);
		readsGroup = splitArray[1];
		
		if(!keepFile) {
			for(int i = 2; i < 6; i++) {
				if(splitArray[i].startsWith("file:///")) {
					splitArray[i] = splitArray[i].substring(7);
				} else {
					if(splitArray[i].startsWith("file:/")) {
						splitArray[i] = splitArray[i].substring(5);
					}
				}
			}
		}
		
		fastq1 = splitArray[2];
		fastq2 = splitArray[3];
		adapter1 = splitArray[4];
		adapter2 = splitArray[5];
		return true;
	}

	/**
	 * @return the readsGroup
	 */
	public String getReadsGroup() {
		return readsGroup;
	}

	/**
	 * @param readsGroup the readsGroup to set
	 */
	public void setReadsGroup(String readsGroup) {
		this.readsGroup = readsGroup;
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the fastq1
	 */
	public String getFastq1() {
		return fastq1;
	}

	/**
	 * @param fastq1 the fastq1 to set
	 */
	public void setFastq1(String fastq1) {
		this.fastq1 = fastq1;
	}

	/**
	 * @return the fastq2
	 */
	public String getFastq2() {
		return fastq2;
	}

	/**
	 * @param fastq2 the fastq2 to set
	 */
	public void setFastq2(String fastq2) {
		this.fastq2 = fastq2;
	}

	/**
	 * @return the adapter1
	 */
	public String getAdapter1() {
		return adapter1;
	}

	/**
	 * @param adapter1 the adapter1 to set
	 */
	public void setAdapter1(String adapter1) {
		this.adapter1 = adapter1;
	}

	/**
	 * @return the adapter2
	 */
	public String getAdapter2() {
		return adapter2;
	}

	/**
	 * @param adapter2 the adapter2 to set
	 */
	public void setAdapter2(String adapter2) {
		this.adapter2 = adapter2;
	}
}
