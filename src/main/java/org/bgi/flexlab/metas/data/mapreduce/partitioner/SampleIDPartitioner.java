package org.bgi.flexlab.metas.data.mapreduce.partitioner;

import org.apache.spark.Partitioner;

/**
 * ClassName: SampleIDPartitioner
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class SampleIDPartitioner extends Partitioner {
    private int numPartitions;
    private int samplePartNum;

    public SampleIDPartitioner(int sampleCount){
        this.numPartitions = sampleCount;
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
     * @param key String. r"sampleID"
     * @return
     */
    @Override
    public int getPartition(Object key) {
        if (key instanceof String){
            return Math.min(Integer.parseInt((String) key), this.numPartitions - 1);
        } else {
            return this.numPartitions - 1;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SampleIDPartitioner){
            return ((SampleIDPartitioner) obj).numPartitions == this.numPartitions;
        } else {
            return false;
        }
    }
}
