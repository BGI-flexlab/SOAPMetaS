package org.bgi.flexlab.metas.data.mapreduce.partitioner;

import org.apache.spark.Partitioner;

/**
 * ClassName: FastqOffsetPartitioner
 * Description: Customized partitioner for multi-sample fastq RDD.
 *
 * Note:
 * 1. RDD content:
 *  key: readName. example: <Text>
 * 	value: mateIndex(1 or 2)##sampleID	readRecordCount##readGroupID##sequence	quality
 *
 * @author heshixu@genomics.cn
 */

public class FastqOffsetPartitioner extends Partitioner {
    private int numPartitions;
    private int samplePartNum;

    public FastqOffsetPartitioner(int size, int samplePartNum){
        this.numPartitions = size;
        this.samplePartNum = Math.max(samplePartNum, 1);
    }

    @Override
    public int numPartitions() {
        return this.numPartitions;
    }

    /**
     * PairRDD to be partitioned:
     *  key: sampleID	pos filelength
     *  value: readGroupID##readName   seq1    qual1    seq2    qual2
     *
     * Note: sampleID begins from 0.
     *
     * @param key String. r"sampleID\treadRecordCount"
     * @return
     */
    @Override
    public int getPartition(Object key) {
        if (key instanceof String) {
            String[] paras = ((String) key).split("\t");
            int sampleIDPartRange = Integer.parseInt(paras[0]) * this.samplePartNum;
            long pos = Math.abs(Long.parseLong(paras[1]));
            long filelength = Math.abs(Long.parseLong(paras[2]));

            if (pos >= filelength){
                pos = filelength - 1L;
            }

            long scale = Math.abs(pos * (long) this.samplePartNum);

            // ArrayIndexOutOfRange related errors may happen.
            return Math.min(sampleIDPartRange + (int) Math.floorDiv(scale, filelength), sampleIDPartRange);
        } else{
            return this.numPartitions-1;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FastqOffsetPartitioner){
            return ((FastqOffsetPartitioner) obj).numPartitions == this.numPartitions;
        } else {
            return false;
        }
    }
}
