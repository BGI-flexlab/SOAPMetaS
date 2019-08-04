package org.bgi.flexlab.metas.data.structure.profiling;

import java.nio.charset.StandardCharsets;

/**
 * ClassName: ProfilingEveResultRecord
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

@Deprecated
public class ProfilingEveResultRecord extends ProfilingResultRecord{

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(256);
        return builder.append(this.getClusterName())
                .append('\t').append(this.getRawReadCount()).append('\t')
                .append(this.getrecaliReadCount()).append('\t')
                .append(this.getRelAbun()).append('\t')
                .append(this.getReadNameString()).toString();
    }
}
