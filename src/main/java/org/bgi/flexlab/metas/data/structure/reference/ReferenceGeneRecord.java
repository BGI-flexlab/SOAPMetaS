package org.bgi.flexlab.metas.data.structure.reference;


import htsjdk.samtools.util.StringUtil;

/**
 * ClassName: ReferenceGeneRecord
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ReferenceGeneRecord {

    private String markerName;
    private int geneLength;
    private double gcContent;

    private ReferenceSpeciesRecord speciesRecord = null;

    public ReferenceGeneRecord(String name, int geneLength, double gcContent){
        this.construct(name, geneLength, gcContent);
    }

    public ReferenceGeneRecord(String name, int geneLength, double gcContent, ReferenceSpeciesRecord species){
        this.construct(name, geneLength, gcContent);
        this.speciesRecord = species;
    }

    private void construct(String name, int geneLength, double gcContent){
        this.markerName = name;
        this.geneLength = geneLength;
        this.gcContent = gcContent;
    }

    public void setSpeciesRecord(ReferenceSpeciesRecord speciesRecord) {
        this.speciesRecord = speciesRecord;
    }

    public String getSpeciesName(){
        if (this.speciesRecord == null){
            return null;
        }
        return this.speciesRecord.getSpeciesName();
    }

    public double getSpeciesGenomeGC(){
        if (this.speciesRecord == null){
            return 0;
        }
        return this.speciesRecord.getGenomeGCContent();
    }

    public int getGeneLength() {
        return geneLength;
    }

    public double getGcContent() {
        return gcContent;
    }

    //public String getGeneSequence(){
    //    return StringUtil.bytesToString(bases);
    //}
}
