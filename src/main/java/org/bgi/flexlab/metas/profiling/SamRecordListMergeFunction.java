package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.util.SequencingMode;

import java.io.Serializable;

/**
 * ClassName: SamRecordListMergeFunction
 * Description: samrecord 列表 到 sampairrecord 的转换方法。
 *
 * @author heshixu@genomics.cn
 */

public class SamRecordListMergeFunction implements Serializable, Function<Iterable<SAMRecord>, MetasSAMPairRecord> {

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(SamRecordListMergeFunction.class);

    private SequencingMode sequencingMode;

    /**
     * Constructor.
     *
     * @param seqMode Sequencing mode, include Paired-end sequencing mode and single-end sequencing mode.
     */
    public SamRecordListMergeFunction(SequencingMode seqMode){
        this.sequencingMode = seqMode;
    }

    @Override
    public MetasSAMPairRecord call(Iterable<SAMRecord> metasSamRecords) {

        if (this.sequencingMode.equals(SequencingMode.PAIREDEND)){
            return pairedListToSamPair(metasSamRecords);
        } else if (this.sequencingMode.equals(SequencingMode.SINGLEEND)){
            return singleListToSamPair(metasSamRecords);
        }
        return null;
    }



    /**
     * Generator of MetasSAMPairRecord for single-end sequencing data, the generated instance will have
     * only one SAMRecord stored in "record1" field. If one read has multiple alignment, all info about
     * the read will be abandoned.
     *
     * @param metasSamRecords Iterator of MetasSamRecords created by groupByKey() operation of RDD.
     * @return MetasSAMPairRecord if the read is exactly mapped to one reference, else null.
     */
    private MetasSAMPairRecord singleListToSamPair(Iterable<SAMRecord> metasSamRecords){

        MetasSAMPairRecord pairRecord = null;

        SAMRecord tempRecord = null;

        for (SAMRecord record: metasSamRecords){
            if (record.isSecondaryAlignment()){
                tempRecord = null;
                break;
            }
            tempRecord = record;
        }

        if (tempRecord != null){
            pairRecord = new MetasSAMPairRecord(tempRecord, null);
            //pairRecord.setPaired(false);
        }

        //LOG.trace("[SOAPMetas::" + SamRecordListMergeFunction.class.getName() + "] Single-end SAMPair: " + pairRecord.toString());

        return pairRecord;
    }

    /**
     * TODO: 注意cOMG原流程中insertsize+100的100是为了容错，insertsize本身包含end+gap+end的区域。
     * Generator of MetasSAMPairRecord for paired-end sequencing data, both "record1" and "record2" fields
     * will be set. There are several status of the paired records:
     * + Both reads are unmapped. This kind of pairs will have been filtered out in the first operation of RDD.
     * + One of the paired is unmapped. If the other read is exactly mapped to one reference, the insert size
     *    and "FirstOfPair" flag will be considered.
     * + One of the paired is unmapped. If the other one is multiple-mapped record, return null.
     * + Each one of the pair is exactly mapped to two different references, At species analysis level, the
     *    species origin of the marker will be considered. At markers level, both are treated as the last
     *    two status.
     * + Bath are properly mapped to the same single reference. This is the standard status.
     * + One is exact alignment, while the other is secondary alignment. If one of the multiple reference is the
     *    same as the exact one, treat them as the proper pair. If none, treated the multiple read as unmapped.
     * + Both are multiply mapped, abandoned.
     *
     * @param metasSamRecords Iterator of MetasSamRecords created by groupByKey() operation of RDD, all
     *                        the records from the iterator have the same record name (read id) without
     *                        "/1/2" suffix.
     * @return MetasSAMPairRecord if the read is exactly mapped to one reference, else null.
     */

    private MetasSAMPairRecord pairedListToSamPair(Iterable<SAMRecord> metasSamRecords){
        MetasSAMPairRecord pairRecord = null;

        int count1 = 0;
        int count2 = 0;

        SAMRecord tempRec1 = null;
        SAMRecord tempRec2 = null;

        for (SAMRecord record: metasSamRecords){
            if (record.getFirstOfPairFlag()){
                tempRec1 = record;
                count1++;
            }

            if (record.getSecondOfPairFlag()){
                tempRec2 = record;
                count2++;
            }
        }

        if (count1 == 1){
            if (count2 == 1){
                pairRecord = new MetasSAMPairRecord(tempRec1, tempRec2);
                pairRecord.setPaired(true);

                if (tempRec1.getReferenceName().equals(tempRec2.getReferenceName())) {
                    pairRecord.setProperPaired(true);
                }
            } else {
                pairRecord = new MetasSAMPairRecord(tempRec1, null);
                //pairRecord.setPaired(false);
            }
        } else {
            if (count2 == 1){
                pairRecord = new MetasSAMPairRecord(tempRec2, null);
                //pairRecord.setPaired(false);
            }
        }

        //LOG.trace("[SOAPMetas::" + SamRecordListMergeFunction.class.getName() + "] Paired-end SAMPair: " + pairRecord.toString());
        return pairRecord;
    }
}
