package org.bgi.flexlab.metas.profiling.filter;

import org.apache.spark.api.java.function.Function;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.io.samio.MetasSamRecord;

/**
 * ClassName: MetasSamRecordRDDFilter
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

filter的创建，会不会因为每个RDD都需要创建一遍，导致性能受到影响（即使可以忽略)???

public class RawMetasSamRecordFilter implements Function<MetasSamRecord, Boolean> {

    private MetasSamRecordIdentityFilter identityFilter;

    public RawMetasSamRecordFilter(final double minIdentity){
        this.identityFilter = new MetasSamRecordIdentityFilter(minIdentity);
    }

    @Override
    public Boolean call(MetasSamRecord record) throws Exception{
        if (record.getReadUnmappedFlag()){
            return false;
        }
        if (identityFilter.filter(record)){
            return false;
        }
        return true;
    }
}
