package org.bgi.flexlab.metas.data.structure.profiling;

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

    private String rgID;
    private String smTag;
    private String clusterName;
    private Integer rawReadCount;
    private Double recaliReadCount;
    private Double abundance; // recaliReadCount divided by marker length
    private Double relAbun; // Relative abundance.
    private byte[] readNameStringBytes; //后续需要考虑采用更合适的方式来存储read name字符串
    private int outType = 2;

    public ProfilingResultRecord(){}

    public ProfilingResultRecord(int outType){
        // 2: relative abundance only
        // 4: detailed relative abundance
        // 8: CAMI format
        this.outType = outType;
    }

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

    public void setRelAbun(Double relAbun) {
        this.relAbun = relAbun;
    }

    public Double getRelAbun() {
        return relAbun;
    }

    public void setrecaliReadCount(Double recaliReadCount) {
        this.recaliReadCount = recaliReadCount;
    }

    public Double getrecaliReadCount() {
        return this.recaliReadCount;
    }

    public void setRgID(String rgID) {
        this.rgID = rgID;
    }

    public String getRgID() {
        return rgID;
    }

    public void setSmTag(String smTag) {
        this.smTag = smTag;
    }

    public String getSmTag() {
        return smTag;
    }

    public void setReadNameString(String readNameStringBytes) {
        this.readNameStringBytes = readNameStringBytes.getBytes(StandardCharsets.UTF_8);
    }

    public String getReadNameString() {
        if (this.readNameStringBytes != null){
            return new String(this.readNameStringBytes, StandardCharsets.UTF_8);
        } else {
            return "";
        }
    }

    @Override
    public String toString() {
        //return this.getClusterName() + '\t' + this.getRelAbun();
        // Return type:
        // 2: relative abundance only
        // 4: detailed relative abundance
        // 8: CAMI format
        StringBuilder builder = new StringBuilder(64);
        if ((outType & 2) > 0) {
            return builder.append(this.getClusterName()).append('\t')
                    .append(String.format("%.2f", this.getRelAbun()))
                    .toString();
        } else if ((outType & 4) > 0){
            return builder.append(this.getClusterName())
                    .append('\t').append(this.getRawReadCount()).append('\t')
                    .append(this.getrecaliReadCount()).append('\t')
                    .append(this.getAbundance()).append('\t')
                    .append(String.format("%.2f", this.getRelAbun()))
                    .toString();
        } else {
            return builder.append(this.getClusterName()).append('\t')
                    .append(String.format("%.2f", this.getRelAbun()))
                    .toString();
        }

    }

    public String getInfo(){
        return new StringBuilder(64).append("RG:").append(rgID).append("SM:").append(smTag)
                .append(" | ClusterName: ").append(clusterName)
                .append(" | RawReadCount/recaliReadCount: ").append(rawReadCount).append('/').append(recaliReadCount)
                .append(" | Abundance: ").append(abundance).toString();
    }

}
