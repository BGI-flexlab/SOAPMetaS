package org.bgi.flexlab.metas.data.structure.sam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.SequenceUtil;


/**
 * ClassName: MetasSamRecord
 * Description:
 * @author: heshixu@genomics.cn
 */

public class MetasSamRecord extends SAMRecord {

    public static final long serialVersionUID = 2L;
    //private Double identity;
    private String truncatedReadName = null;

    public MetasSamRecord(SAMFileHeader header) {
        super(header);
    }

    public String getTruncatedReadName(){
        return this.getReadName().replaceFirst("/[12]$", "");
    }

    public Double getGCContent(){
        return SequenceUtil.calculateGc(this.getReadBases());
    }

    @Override
    public String getPairedReadName() {
        final StringBuilder builder = new StringBuilder(64);
        builder.append(getTruncatedReadName());
        if (getReadPairedFlag()) {
            if (getFirstOfPairFlag()) {
                builder.append("/1");
            } else {
                builder.append("/2");
            }
        }
        return builder.toString();
    }


    ///**
    // * Set the identity value of the SamRecord.
    // *
    // * @param value Identity value of Double type.
    // */
    //public void setIdentity(final Double value){
    //    this.identity = value;
    //}
    ///**
    // * Get the identity value of the SamRecord.
    // *
    // * @return Identity value of Double type.
    // */
    //public Double getIdentity(){
    //    if (this.identity == null){
    //        setIdentity(computeIdentity());
    //    }
    //    return this.identity;
    //}
    ///**
    // * The method for computing identity. As no reliable method is decided, so the value is set to 0.0 .
    // * The designed method needs "optional tag" in .sam file which is ignored by SamRecord parser, and
    // * the filtering has little effect.
    // * @return
    // */
    //public Double computeIdentity(){
    //    Double identityTemp = 0.0;
    //    return identityTemp;
    //}

}
