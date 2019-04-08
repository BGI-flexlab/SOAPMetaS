package org.bgi.flexlab.metas.data.structure.fastq;

/**
 * The Class is based on org.bgi.flexlab.gaealib.input.fastq.FastqSampleList
 *
 * *Changes:
 *  + Simplify the data structure
 *  + Add method "getPathString()" to get fastq
 */

public class FastqSampleList {
	private int sampleID = 0;

	private String rgID;

	private String fastq1;
	private String fastq2;

	
	public FastqSampleList() {
		rgID = "OMISSION";
		fastq1 = null;
		fastq2 = null;
	}
	
	public void setSampleList (String rgID, String fastq1, String fastq2, int sampleID) {
		this.rgID = rgID;
		this.fastq1 = fastq1;
		this.fastq2 = fastq2;
		this.sampleID = sampleID;
	}

	/**
	 * @param id the id to set
	 */
	public void setSampleID(int id) {
		this.sampleID = id;
	}

	/**
	 * @return the id
	 */
	public int getSampleID() {
		return sampleID;
	}

	/**
	 * @param fastq1 the fastq1 to set
	 */
	public void setFastq1(String fastq1) {
		this.fastq1 = fastq1;
	}

	/**
	 * @return the fastq1
	 */
	public String getFastq1() {
		return fastq1;
	}

	/**
	 * @param fastq2 the fastq2 to set
	 */
	public void setFastq2(String fastq2) {
		this.fastq2 = fastq2;
	}

	/**
	 * @return the fastq2
	 */
	public String getFastq2() {
		return fastq2;
	}


	public void setRgID(String rgID) {
		this.rgID = rgID;
	}

	public String getRgID() {
		return rgID;
	}

	public String getFastqPathString(){
		if (fastq2 == null){
			return fastq1 + ",";
		}
		return fastq1 + "," + fastq2 + ",";
	}

	@Override
	public String toString() {
		return this.rgID + "::" + this.fastq1 + "::"+ this.fastq2;
	}
}
