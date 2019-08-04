package org.bgi.flexlab.metas.data.structure.sam;

import htsjdk.samtools.SAMRecord;

import java.io.Serializable;

/**
 * ClassName: MetasSAMPairRecord
 * Description: 由于存在paired-end测序的数据，所以需要构建针对pair信息的特殊数据类型，方便对pair信息的封装。为了方便
 * 流程的整合，针对single-end数据也进行同样的处理，并提供了判断pair 状态的适当接口。
 *
 * @author heshixu@genomics.cn
 */

public class MetasSAMPairRecord implements Serializable {

    public static final long serialVersionUID = 1L;

    private SAMRecord record1 = null;
    private SAMRecord record2 = null;

    //private boolean exact1 = true;
    //private boolean exact2 = true;

    private boolean paired = false;
    private boolean properPaired = false;

    //public boolean isExact1() {
    //    return exact1;
    //}
    //public boolean isExact2() {
    //    return exact2;
    //}
    //public void setMultiAlign1() {
    //    this.exact1 = false;
    //}
    //public void setMultiAlign2() {
    //    this.exact2 = false;
    //}

    public MetasSAMPairRecord(){}

    public MetasSAMPairRecord(SAMRecord record1, SAMRecord record2) {
        this.setFirstRecord(record1);
        this.setSecondRecord(record2);
    }

    public void setFirstRecord(SAMRecord record) {
        this.record1 = record;
    }

    public void setSecondRecord(SAMRecord record) {
        this.record2 = record;
    }

    public void setProperPaired(boolean properPaired){
        this.properPaired = properPaired;
    }

    public boolean isProperPaired(){
        return this.properPaired;
    }

    public void setPaired(boolean paired){
        this.paired = paired;
    }

    public boolean isPaired(){
        return this.paired;
    }

    public SAMRecord getFirstRecord() {
        return this.record1;
    }

    public SAMRecord getSecondRecord() {
        return this.record2;
    }

    @Override
    public String toString() {
        StringBuilder name = new StringBuilder(64).append("rec1: ");
        if (record1 == null) {
            name.append("null");
        } else {
            name.append(record1.toString());
        }

        name.append(" rec2: ");
        if (record2 == null) {
            name.append("null");
        } else {
            name.append(record2.toString());
        }

        return name.toString();
    }
}
