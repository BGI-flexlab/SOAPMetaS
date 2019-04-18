package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMRecord;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.util.SequencingMode;

/**
 * ClassName: ProfilingUtils
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public final class ProfilingUtils {

    private MetasOptions options;
    private SequencingMode sequencingMode;

    public ProfilingUtils (MetasOptions options){
        this.options = options;

        this.sequencingMode = this.options.getSequencingMode();
    }


    public String samRecordNameModifier(SAMRecord record){
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
    public static int computeInsertSize(final SAMRecord firstEnd,
                                        final SAMRecord secondEnd) {
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

    ///**
    // * Function2 for reduceByKey of MetasSamPairRecordRDD in method ProfilingProcessMS.profiling.
    // *
    // * Deprecated.
    // */
    //public static class SamPairRecordMergeFunction implements
    //        Function2<MetasSamPairRecord, MetasSamPairRecord, MetasSamPairRecord> {
//
    //    private SequencingMode seqMode;
//
    //    public SamPairRecordMergeFunction(SequencingMode mode){
    //        this.seqMode = mode;
    //    }
//
    //    @Override
    //    public MetasSamPairRecord call(MetasSamPairRecord metasSamPairRecord, MetasSamPairRecord metasSamPairRecord2) throws Exception {
    //        if (this.seqMode.equals(SequencingMode.PAIREDEND)){
    //            this.pairedModeMerge(metasSamPairRecord, metasSamPairRecord2);
    //        } else {
    //            this.singleModeMerge(metasSamPairRecord, metasSamPairRecord2);
    //        }
    //    }
//
    //    private MetasSamPairRecord singleModeMerge(MetasSamPairRecord metasSamPairRecord,
    //                                               MetasSamPairRecord metasSamPairRecord2){
    //        return null;
    //    }
//
    //    private MetasSamPairRecord pairedModeMerge(MetasSamPairRecord pair1,
    //                                               MetasSamPairRecord pair2){
    //        SAMRecord pair2Rec1 = pair2.getFirstRecord();
    //        SAMRecord pair2Rec2 = pair2.getSecondRecord();
//
    //        if (pair2.isExact1()){
//
    //        }
    //        return null;
    //    }
    //}
}
