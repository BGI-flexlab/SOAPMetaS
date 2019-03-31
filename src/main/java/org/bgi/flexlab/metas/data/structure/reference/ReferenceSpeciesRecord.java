package org.bgi.flexlab.metas.data.structure.reference;

/**
 * ClassName: ReferenceSpeciesRecord
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class ReferenceSpeciesRecord {

    /**
     * TODO: 后续考虑实际分析流程是否需要保存序列信息，如果不需要的话，那就只保留GC content信息即可。
     */

    private String speciesName;
    //private int genomeLength;
    private double genomeGCContent;

    private String allCladeString;

    public ReferenceSpeciesRecord(String name, double gc){
        this.construct(name, gc);
        this.allCladeString = this.speciesName;
    }

    public ReferenceSpeciesRecord(String name, double gc, String allCladeString){
        this.construct(name, gc);
        this.allCladeString = allCladeString;
    }

    private void construct(String name, double gc){
        this.speciesName = name;
        this.genomeGCContent = gc;
    }

    //public int getGenomeLength() {
    //    return this.genomeLength;
    //}

    public String getSpeciesName() {
        return this.speciesName;
    }

    public String getAllCladeString() {
        return this.allCladeString;
    }

    /**
     * @return GC content ratio of reference nucleotide sequence.
     */
    public double getGenomeGCContent(){
        return this.genomeGCContent;
    }



}
