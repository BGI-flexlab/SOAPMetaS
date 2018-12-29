package org.bgi.flexlab.metas.profiling.filter;

import org.bgi.flexlab.metas.io.samio.MetasSamRecord;

/**
 * Basic APi for filtering MetasSamRecord instance.
 */
public interface MetasSamRecordFilter {

    /**
     * Whether the MetasSamRecord should be filtered out. "true" means the record should be filtered out.
     *
     * @param record MetasSamRecord to be evaluated.
     * @return true if matches the filter, otherwise false.
     */
    public boolean filter(MetasSamRecord record);

}
