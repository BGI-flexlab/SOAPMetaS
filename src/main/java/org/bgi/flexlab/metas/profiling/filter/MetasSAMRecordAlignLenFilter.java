package org.bgi.flexlab.metas.profiling.filter;

import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.function.Function;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import scala.Serializable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: MetasSAMRecordAlignLenFilter
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSAMRecordAlignLenFilter implements Function<MetasSAMPairRecord, MetasSAMPairRecord>, Serializable {

    private static final long serialVersionUID = 1L;

    private int minAlignLength;

    private final HashSet<String> CIGAR_Match_Mismatch_Indel = new HashSet<>(Arrays.asList("M", "=", "X"));

    private Pattern cigarPattern = Pattern.compile("(\\d+)(\\D+)");
    //private Pattern mdTagPattern = Pattern.compile("\\d+");

    public MetasSAMRecordAlignLenFilter(){
        this.minAlignLength = 30;
    }

    public MetasSAMRecordAlignLenFilter(int minAlignLen){
        this.minAlignLength = minAlignLen;
    }

    /**
     * @param record SAMRecord to be evaluated.
     * @return
     */
    public boolean filter(SAMRecord record){
        if (record == null){
            return false;
        }
        double len = calculateAlignLen(record.getCigarString());
        return len < this.minAlignLength;
    }


    @Override
    public MetasSAMPairRecord call(MetasSAMPairRecord inputRec){
        MetasSAMPairRecord pairRec = inputRec;
        if (this.filter(pairRec.getFirstRecord())){
            pairRec.setFirstRecord(null);
            pairRec.setProperPaired(false);
        }
        if (this.filter(pairRec.getSecondRecord())) {
            pairRec.setSecondRecord(null);
            pairRec.setProperPaired(false);
        }
        return pairRec;
    }

    /**
     *
     * Calculate length of the alignment between the read and the marker genes (different from read length).
     *
     * @param cigar The CIGAR as string.
     * //@param mdTag The value string of "MD:Z:" tag in optional field of SAM file, without the "MD:Z:" tag itself.
     * @return double Identity value.
     */
    public double calculateAlignLen(String cigar){

        int cigarAllCount = 0;

        // identify the total number of match/mismatch.
        Matcher cigarMatcher = this.cigarPattern.matcher(cigar);
        while(cigarMatcher.find()){
            if (CIGAR_Match_Mismatch_Indel.contains(cigarMatcher.group(2))){
                cigarAllCount += Integer.parseInt(cigarMatcher.group(1));
            }
        }

        return cigarAllCount;
    }
}
