package org.bgi.flexlab.metas.profiling;

import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.io.samio.MetasSamPairRecord;
import org.bgi.flexlab.metas.io.samio.MetasSamRecord;
import org.bgi.flexlab.metas.util.SequencingMode;

/**
 * ClassName: ProfilingUtils
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public final class ProfilingUtils {

    private MetasOptions options;
    private SequencingMode sequencingMode;

    public ProfilingUtils (MetasOptions options){
        this.options = options;

        this.sequencingMode = this.options.getSequencingMode();
    }

    /**
     * samrecord 列表 到 sampairrecord 的转换方法。
     * @param metasSamRecords
     * @return
     */
    public MetasSamPairRecord readSamListToSamPair(Iterable<MetasSamRecord> metasSamRecords){
        try {
            if (this.sequencingMode.equals(SequencingMode.SINGLEEND)) {
                return singleListToSamPair(metasSamRecords);
            } else if (this.sequencingMode.equals(SequencingMode.PAIREND)){
                return pairedListToSamPair(metasSamRecords);
            }
        } catch (final Exception e) {
            e.printStackTrace();
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
            pairRecord = new MetasSamPairRecord(tempRecord);
            pairRecord.setPairedMode(false);
        }

        return pairRecord;
    }

    /**
     * TODO: 关于insertsize的过滤需要确认是在此步完成还是在后续步骤，因为需要用到marker length数据。以及，需要确认insert size的具体功能需求。
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
            pairRecord = new MetasSamPairRecord(tempRec1);
            pairRecord.setPairedMode(true);

        } else if (exact2) {
            pairRecord = new MetasSamPairRecord(tempRec2);
            pairRecord.setPairedMode(true);
        }

        return pairRecord;
    }


    public String samRecordNameModifier(MetasSamRecord record){
        try {
            if(this.sequencingMode.equals(SequencingMode.SINGLEEND)){
                return record.getReadName();
            } else {
                return record.getReadName().replaceFirst("/[12]$", "");
            }
        } catch (final NullPointerException e){
            e.printStackTrace();
        }
        return null;
    }

    public Double computeRelativeAbundance(Double abundance, Double totalAbundance){
        return abundance/totalAbundance;
    }


    /**
     * Compute SAMRecord insert size
     *
     * The method is copied from org.bgi.flexlab.gaea.util.GaeaSamPairUtil
     */
    public static int computeInsertSize(final MetasSamRecord firstEnd,
                                        final MetasSamRecord secondEnd) {
        if (firstEnd.getReadUnmappedFlag() || secondEnd.getReadUnmappedFlag()) {
            return 0;
        }
        if (!firstEnd.getReferenceName().equals(secondEnd.getReferenceName())) {
            return 0;
        }

        final int firstEnd5PrimePosition = firstEnd.getReadNegativeStrandFlag() ? firstEnd
                .getAlignmentEnd() : firstEnd.getAlignmentStart();
        final int secondEnd5PrimePosition = secondEnd
                .getReadNegativeStrandFlag() ? secondEnd.getAlignmentEnd()
                : secondEnd.getAlignmentStart();

        final int adjustment = (secondEnd5PrimePosition >= firstEnd5PrimePosition) ? +1
                : -1;
        return secondEnd5PrimePosition - firstEnd5PrimePosition + adjustment;
    }
}
