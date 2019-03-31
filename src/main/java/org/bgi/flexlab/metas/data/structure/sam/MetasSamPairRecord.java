package org.bgi.flexlab.metas.data.structure.sam;

/**
 * ClassName: MetasSamPairRecord
 * Description: 由于存在paired-end测序的数据，所以需要构建针对pair信息的特殊数据类型，方便对pair信息的封装。为了方便
 * 流程的整合，针对single-end数据也进行同样的处理，并提供了判断pair 状态的适当接口。
 *
 * @author heshixu@genomics.cn
 */

public class MetasSamPairRecord {

    private MetasSamRecord record1 = null;
    private MetasSamRecord record2 = null;

    //private boolean exact1 = true;
    //private boolean exact2 = true;

    private boolean pairedMode = false;
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

    public MetasSamPairRecord(MetasSamRecord record1, MetasSamRecord record2){
        this.record1 = record1;
        this.record2 = record2;
    }

    public void setProperPaired(boolean properPaired){
        this.properPaired = properPaired;
    }

    public boolean isProperPaired(){
        return this.properPaired;
    }

    public void setPairedMode(boolean pairedMode){
        this.pairedMode = pairedMode;
    }

    public boolean isPairedMode(){
        return this.pairedMode;
    }

    public MetasSamRecord getFirstRecord() {
        return this.record1;
    }

    public MetasSamRecord getSecondRecord() {
        return this.record2;
    }

    public MetasSamRecord getRecord(){
        if(isPairedMode()){
            return null;
        } else{
            return this.getFirstRecord();
        }
    }
}
