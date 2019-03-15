package org.bgi.flexlab.metas.profiling.filter;

import org.apache.spark.api.java.function.Function;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: MetasSamRecordIdentityFilter
 * Description: Filter SAM by the identity of mapping result.
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSamRecordIdentityFilter implements MetasSamRecordFilter, Function<MetasSamRecord, Boolean> {

    private double minimumIdentity = 0;

    /**
     * @param record MetasSamRecord to be evaluated.
     * @return
     */
    @Override
    public boolean filter(MetasSamRecord record){
        double identity = calculateIdentity(record.getCigarString(), record.getStringAttribute("MD"));
        return identity < this.minimumIdentity;
    }


    @Override
    public Boolean call(MetasSamRecord record){
        if (this.filter(record)){
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
