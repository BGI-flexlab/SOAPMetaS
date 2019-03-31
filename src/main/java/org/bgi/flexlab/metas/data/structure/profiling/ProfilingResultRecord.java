package org.bgi.flexlab.metas.data.structure.profiling;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * ClassName: ProfilingResultRecord
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class ProfilingResultRecord implements Serializable {

    public static final long serialVersionUID = 1L;

    private String readGroupID;
    private String clusterName;
    private Integer rawReadCount;
    private Double correctedReadCount;
    private Double abundance; // correctedReadCount divided by marker length
    private byte[] readNameStringBytes; //后续需要考虑采用更合适的方式来存储read name字符串

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

    public void setReadGroupID(String readGroupID) {
        this.readGroupID = readGroupID;
    }

    public String getReadGroupID() {
        return readGroupID;
    }

    public void setReadNameString(byte[] readNameStringBytes) {
        this.readNameStringBytes = readNameStringBytes;
    }

    public String getReadNameString() {
        if (this.readNameStringBytes != null){
            return new String(this.readNameStringBytes, StandardCharsets.UTF_8);
        } else {
            return "";
        }
    }

}
