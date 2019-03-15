package org.bgi.flexlab.metas.data.structure.reference;

import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.SequenceUtil;

/**
 * ClassName: ReferenceRecord
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ReferenceRecord {

    /**
     * TODO: 后续考虑实际分析流程是否需要保存序列信息，如果不需要的话，那就只保留GC content信息即可。
     */

    private ReferenceSequence referenceSequence;
    private String speciesName;
    private String allClade;
    private Double referenceGCContent = null;

    public int getReferenceLength() {
        return this.referenceSequence.length();
    }

    public String getReferenceName() {
        return this.referenceSequence.getName();
    }

    public String getSpeciesName() {
        return this.speciesName;
    }

    public String getAllClade() {
        return this.allClade;
    }

    public String getReferenceSequence(){
        return this.referenceSequence.getBaseString();
    }

    public void setAllClade(String allClade) {
        this.allClade = allClade;
    }

    public void setSpeciesName(String speciesName) {
        this.speciesName = speciesName;
    }

    /**
     * @return GC content ratio of reference nucleotide sequence.
     */
    public Double getReferenceGCContent(){
        return SequenceUtil.calculateGc(this.referenceSequence.getBases());
    }

}
