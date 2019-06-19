package org.bgi.flexlab.metas.profiling.filter;

import htsjdk.samtools.SAMRecord;

/**
 * Basic APi for filtering SAMRecord instance.
 */
public interface MetasSAMRecordFilter {

    /**
     * Whether the SAMRecord should be filtered out. "true" means the record should be filtered out.
     *
     * @param record SAMRecord to be evaluated.
     * @return true if matches the filter, otherwise false.
     */
    public boolean filter(SAMRecord record);

}
