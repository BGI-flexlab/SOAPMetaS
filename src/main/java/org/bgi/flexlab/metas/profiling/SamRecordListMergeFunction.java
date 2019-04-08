package org.bgi.flexlab.metas.profiling;

import org.apache.spark.api.java.function.Function;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;
import org.bgi.flexlab.metas.util.SequencingMode;

/**
 * ClassName: SamRecordListMergeFunction
 * Description: samrecord 列表 到 sampairrecord 的转换方法。
 *
 * @author heshixu@genomics.cn
 */

public class SamRecordListMergeFunction implements Function<Iterable<MetasSamRecord>, MetasSamPairRecord> {

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
    public MetasSamPairRecord call(Iterable<MetasSamRecord> metasSamRecords) throws Exception {
        if (this.sequencingMode.equals(SequencingMode.PAIREDEND)){
            return pairedListToSamPair(metasSamRecords);
        } else if (this.sequencingMode.equals(SequencingMode.SINGLEEND)){
            return singleListToSamPair(metasSamRecords);
        }
        return null;
    }



    /**
     * Generator of MetasSamPairRecord for single-end sequencing data, the generated instance will have
     * only one MetasSamRecord stored in "record1" field. If one read has multiple mapping, all info about
     * the read will be abandoned.
     *
     * @param metasSamRecords Iterator of MetasSamRecords created by groupByKey() operation of RDD.
     * @return MetasSamPairRecord if the read is exactly mapped to one reference, else null.
     */
    private MetasSamPairRecord singleListToSamPair(Iterable<MetasSamRecord> metasSamRecords){

        MetasSamPairRecord pairRecord = null;

        MetasSamRecord tempRecord = null;

        for (MetasSamRecord record: metasSamRecords){
            if (record.isSecondaryAlignment()){
                tempRecord = null;
                break;
            }
            tempRecord = record;
        }

        if (tempRecord != null){
            pairRecord = new MetasSamPairRecord(tempRecord, null);
            pairRecord.setPairedMode(false);
        }

        return pairRecord;
    }

    /**
     * TODO: 注意cOMG原流程中insertsize+100的100是为了容错，insertsize本身包含end+gap+end的区域。
     * Generator of MetasSamPairRecord for paired-end sequencing data, both "record1" and "record2" fields
     * will be set. There are several status of the paired records:
     * + Both reads are unmapped. This kind of pairs will have been filtered out in the first operation of RDD.
     * + One of the paired is unmapped. If the other read is exactly mapped to one reference, the insert size
     *    and "FirstOfPair" flag will be considered.
     * + One of the paired is unmapped. If the other one is multiple-mapping, return null.
     * + Each one of the pair is exactly mapped to two different references, At species analysis level, the
     *    species origin of the marker will be considered. At markers level, both are treated as the last
     *    two status.
     * + Bath are properly mapped to the same single reference. This is the standard status.
     * + One is exactly mapped, while the other is multiply mapped. If one of the multiple reference is the
     *    same as the exact one, treat them as the proper pair. If none, treated the multiple read as unmapped.
     * + Both are multiply mapped, abandoned.
     *
     * @param metasSamRecords Iterator of MetasSamRecords created by groupByKey() operation of RDD, all
     *                        the records from the iterator have the same record name (read id) without
     *                        "/1/2" suffix.
     * @return MetasSamPairRecord if the read is exactly mapped to one reference, else null.
     */

    private MetasSamPairRecord pairedListToSamPair(Iterable<MetasSamRecord> metasSamRecords){
        MetasSamPairRecord pairRecord = null;

        boolean exact1 = false;
        boolean exact2 = false;

        MetasSamRecord tempRec1 = null;
        MetasSamRecord tempRec2 = null;

        for (MetasSamRecord record: metasSamRecords){
            if (record.getFirstOfPairFlag()){
                if (record.isSecondaryAlignment()){
                    exact1 = false;
                    continue;
                }
                tempRec1 = record;
                exact1 = true;
                continue;
            }

            if (record.getSecondOfPairFlag()){
                if (record.isSecondaryAlignment()){
                    exact2 = false;
                    continue;
                }
                tempRec2 = record;
                exact2 = true;
                continue;
            }
        }

        if (exact1 && exact2){

            pairRecord = new MetasSamPairRecord(tempRec1, tempRec2);
            pairRecord.setPairedMode(true);

            if (tempRec1.getReferenceName().equals(tempRec2.getReferenceName())){
                pairRecord.setProperPaired(true);
            }

        } else if (exact1) {
            pairRecord = new MetasSamPairRecord(tempRec1, null);
            pairRecord.setPairedMode(true);

        } else if (exact2) {
            pairRecord = new MetasSamPairRecord(null, tempRec2);
            pairRecord.setPairedMode(true);
        }

        return pairRecord;
    }
}
