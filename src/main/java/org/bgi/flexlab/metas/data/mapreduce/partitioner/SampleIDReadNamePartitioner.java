package org.bgi.flexlab.metas.data.mapreduce.partitioner;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.util.Utils;

/**
 * ClassName: SampleIDReadNamePartitioner
 * Description: Customized partitioner for PairRDD of which the key format is
 *  r"sampleID\treadRecordCount", note that sampleID must be integer.
 *
 * @author heshixu@genomics.cn
 */

public class SampleIDReadNamePartitioner extends Partitioner {
    private int numPartitions;
    private int samplePartNum;

    public SampleIDReadNamePartitioner(int size, int samplePartNum){
        this.numPartitions = size;
        this.samplePartNum = Math.max(samplePartNum, 1);
    }

    @Override
    public int numPartitions() {
        return this.numPartitions;
    }

    /**
     * PairRDD to be partitioned:
     *
     * Note: sampleID begins from 0.
     *
     * @param key String. r"sampleID\t(readName/clusterName)"
     * @return
     */
    @Override
    public int getPartition(Object key) {
        if (key == null){
            return this.numPartitions - 1;
        }
        String[] keyItem = StringUtils.split(key.toString(), '\t');
        if (keyItem.length != 2){
            return this.numPartitions - 1;
        }
        int sampleIDPartRange = Integer.parseInt(keyItem[0]) * this.samplePartNum;
        int subPart = Utils.nonNegativeMod(keyItem[1].hashCode(), this.samplePartNum);

        return Math.min(sampleIDPartRange + subPart, sampleIDPartRange);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SampleIDReadNamePartitioner){
            return ((SampleIDReadNamePartitioner) obj).numPartitions == this.numPartitions;
        } else {
            return false;
        }
    }
}
