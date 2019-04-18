package org.bgi.flexlab.metas.data.structure.reference;

import java.io.Serializable;

/**
 * ClassName: ReferenceGeneRecord
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ReferenceGeneRecord implements Serializable {

    public static final long serialVersionUID = 1L;

    private String markerName;
    private int geneLength;
    private double gcContent;
    private String speciesName = null;

    public ReferenceGeneRecord(String name, int geneLength, String species){
        this.construct(name, geneLength, species, 0);
        this.speciesName = species;
    }

    public ReferenceGeneRecord(String name, int geneLength, String species, double gcContent){
        this.construct(name, geneLength, species, gcContent);

    }

    private void construct(String name, int geneLength, String species, double gcContent){
        this.markerName = name;
        this.geneLength = geneLength;
        this.speciesName = species;
        this.gcContent = gcContent;
    }


    public String getMarkerName() {
        return markerName;
    }

    public String getSpeciesName(){
        if (this.speciesName == null){
            return "Unknown";
        }
        return this.speciesName;
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
