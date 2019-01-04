package org.bgi.flexlab.metas.io.referenceio;

/**
 * ClassName: ReferenceRecord
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ReferenceRecord {

    private String referenceName;
    private int referenceLength;
    private String speciesName;
    private String allClade;
    private String referenceSequence;
    private Double referenceGCContent;

    public int getReferenceLength() {
        return this.referenceLength;
    }

    public String getReferenceName() {
        return this.referenceName;
    }

    public String getSpeciesName() {
        return this.speciesName;
    }

    public String getAllClade() {
        return this.allClade;
    }

    public String getReferenceSequence(){
        return this.referenceSequence;
    }

    public void setReferenceLength(int markerLength) {
        this.referenceLength = markerLength;
    }

    public void setAllClade(String allClade) {
        this.allClade = allClade;
    }

    public void setReferenceName(String markerName) {
        this.referenceName = markerName;
    }

    public void setSpeciesName(String speciesName) {
        this.speciesName = speciesName;
    }

    public void setReferenceSequence(String markerSequence) {
        this.referenceSequence = markerSequence;
    }

    /**
     * TODO: 需要参考hsjdk中 SequenceUtil的GC计算方法，其中碱基序列需要考虑如何采取Byte的形式进行存储。
     *
     * @return GC content ratio of reference nucleotide sequence.
     */
    public Double getReferenceGCContent(){
        return this.referenceGCContent;
    }

    public void setReferenceGCContent(Double gcContent){
        this.referenceGCContent = gcContent;
    }
}
