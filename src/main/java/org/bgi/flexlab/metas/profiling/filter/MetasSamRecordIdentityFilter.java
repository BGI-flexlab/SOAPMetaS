package org.bgi.flexlab.metas.profiling.filter;

import org.bgi.flexlab.metas.io.samio.MetasSamRecord;

/**
 * ClassName: MetasSamRecordIdentityFilter
 * Description: Filter SAM by the identity of mapping result.
 *
 * @author: heshixu@genomics.cn
 */

public class MetasSamRecordIdentityFilter implements MetasSamRecordFilter {

    private float minimumIdentity = 0;

    public MetasSamRecordIdentityFilter(final float minimumIdentity){
        this.minimumIdentity = minimumIdentity;
    }


    /**
     * TODO: Identity computing method. 可以参考humann2的模块neucleotide.calculate_percent_identity
     *
     * @param record MetasSamRecord to be evaluated.
     * @return
     */
    @Override
    public boolean filter(MetasSamRecord record){
        float identity = 0;
        return identity < this.minimumIdentity;
    }

}
