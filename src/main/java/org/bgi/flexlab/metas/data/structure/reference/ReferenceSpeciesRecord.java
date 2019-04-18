package org.bgi.flexlab.metas.data.structure.reference;

import java.io.Serializable;

/**
 * ClassName: ReferenceSpeciesRecord
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class ReferenceSpeciesRecord implements Serializable {

    public static final long serialVersionUID = 1L;

    /**
     * TODO: 后续考虑实际分析流程是否需要保存序列信息，如果不需要的话，那就只保留GC content信息即可。
     */

    private String speciesName;
    private int genomeLength;
    private double genomeGCContent;

    private String allCladeString;

    public ReferenceSpeciesRecord(String name, int genoLen, double gc){
        this.construct(name, genoLen, gc);
        this.allCladeString = this.speciesName;
    }

    public ReferenceSpeciesRecord(String name, int genoLen, double gc, String allCladeString){
        this.construct(name, genoLen, gc);
        this.allCladeString = allCladeString;
    }

    private void construct(String name, int genoLen, double gc){
        this.speciesName = name;
        this.genomeGCContent = gc;
        this.genomeLength = genoLen;
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

    public int getGenomeLength() {
        return this.genomeLength;
    }
}
