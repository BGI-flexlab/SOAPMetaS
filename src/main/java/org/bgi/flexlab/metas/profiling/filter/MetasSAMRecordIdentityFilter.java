package org.bgi.flexlab.metas.profiling.filter;

import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: MetasSAMRecordIdentityFilter
 * Description: Filter SAM by the identity of alignment result.
 *
 * @author heshixu@genomics.cn
 */

public class MetasSAMRecordIdentityFilter
        implements MetasSAMRecordFilter, Function<Tuple2<String, SAMRecord>, Boolean> {

    private double minimumIdentity;

    public MetasSAMRecordIdentityFilter(){
        this.minimumIdentity = 80;
    }

    public MetasSAMRecordIdentityFilter(double minIdentity){
        this.minimumIdentity = minIdentity;
    }

    /**
     * @param record SAMRecord to be evaluated.
     * @return
     */
    @Override
    public boolean filter(SAMRecord record){
        if (record == null){
            return false;
        }
        double identity = calculateIdentity(record.getCigarString(), record.getStringAttribute("MD"));
        return identity < this.minimumIdentity;
    }


    @Override
    public Boolean call(Tuple2<String, SAMRecord> tuple){
        if (this.filter(tuple._2)){
            return false;
        }
        return true;
    }

    /**
     * The method is referred to calculate_percent_identity method in HUMAnN2:nucleotide_search module.
     *
     * Note that there are several methods for identity computing, referred to <https://mp.weixin.qq.com/s/eAbrhOvYH5PTHQnDpMHSig>.
     * So the details could be re-considered.
     *
     * @param cigar The CIGAR as string.
     * @param mdTag The value string of "MD:Z:" tag in optional field of SAM file, without the "MD:Z:" tag itself.
     * @return double Identity value.
     */
    public double calculateIdentity(String cigar, String mdTag){

        final HashSet<String> CIGAR_Match_Mismatch_Indel = new HashSet<>(Arrays.asList("M", "=", "X", "I", "D"));

        int cigarAllCount = 0;
        int mdTagAllCount = 0;

        double identity = 0.0;

        // identify the total number of match/mismatch/indel.
        Matcher cigarMatcher = Pattern.compile("(\\d+)(\\D+)").matcher(cigar);
        while(cigarMatcher.find()){
            if (CIGAR_Match_Mismatch_Indel.contains(cigarMatcher.group(2))){
                cigarAllCount += Integer.parseInt(cigarMatcher.group(1));
            }
        }

        // sum the md field numbers to get the total number of matches.
        Matcher mdTagMatcher =Pattern.compile("(\\d+)\\D+").matcher(mdTag);
        while(mdTagMatcher.find()){
            mdTagAllCount += Integer.parseInt(mdTagMatcher.group(1));
        }

        if (cigarAllCount > 0){
            identity = 100.0 * ( mdTagAllCount / (cigarAllCount * 1.0) );
        }

        return identity;
    }
}
