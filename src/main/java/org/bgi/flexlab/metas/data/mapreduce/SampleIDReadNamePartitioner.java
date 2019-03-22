package org.bgi.flexlab.metas.data.mapreduce;

import org.apache.spark.Partitioner;
import org.apache.spark.util.Utils;

/**
 * ClassName: SampleIDReadNamePartitioner
 * Description: Customized partitioner for PairRDD of which the key format is
 *  r"sampleID\treadRecordCount", note that sampleID must be integer.
 *
 * @author: heshixu@genomics.cn
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
     * new key: sampleID	readName
     *
     * Note: sampleID begins from 0.
     *
     * @param key String. r"sampleID\treadRecordCount"
     * @return
     */
    @Override
    public int getPartition(Object key) {
        if (key == null){
            return this.numPartitions - 1;
        }
        String[] keyItem = key.toString().split("\t");
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
