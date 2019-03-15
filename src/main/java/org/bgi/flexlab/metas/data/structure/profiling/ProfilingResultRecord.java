package org.bgi.flexlab.metas.data.structure.profiling;

import java.io.Serializable;

/**
 * ClassName: ProfilingResultRecord
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ProfilingResultRecord implements Serializable {

    protected String clusterName;
    protected Integer rawReadCount;
    protected Double correctedReadCount;
    protected Double abundance; // correctedReadCount divided by marker length
    //protected Double relativeAbun;
    //protected Byte readNameString; //后续需要考虑采用更合适的方式来存储read name字符串

    public void setClusterName(String name){
        this.clusterName = name;
    }

    public String getClusterName(){
        return this.clusterName;
    }

    public void setRawReadCount(Integer count){
        this.rawReadCount = count;
    }

    public Integer getRawReadCount(){
        return this.rawReadCount;
    }

    public void setAbundance(Double abundance) {
        this.abundance = abundance;
    }

    public Double getAbundance(){
        return this.abundance;
    }

    public void setCorrectedReadCount(Double correctedReadCount) {
        this.correctedReadCount = correctedReadCount;
    }

    public Double getCorrectedReadCount() {
        return this.correctedReadCount;
    }

    //public void setRelativeAbun(Double relativeAbun){
    //    this.relativeAbun = relativeAbun;
    //}

    //public Double getRelativeAbun(){
    //    return this.relativeAbun;
    //}

}
