package org.bgi.flexlab.metas.io.referenceio;

import java.util.HashMap;

/**
 * ClassName: ReferenceInfomation
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class ReferenceInfomation {

    private HashMap<String, ReferenceRecord> referenceRecordHashMap;

    /**
     * TODO: 需要完善markers的文件读取和矩阵构建方法。另外要考虑HashMap类型是否适用，有没有更高效的data structure，
     *
     * @param referenceMatrixFilePath
     */
    public ReferenceInfomation(String referenceMatrixFilePath){

    }

    public int getReferenceLength(String referenceName) {
        return referenceRecordHashMap.get(referenceName).getReferenceLength();
    }

    public Double getReferenceGCContent(String referenceName) {
        return referenceRecordHashMap.get(referenceName).getReferenceGCContent();
    }

    public String getReferenceSpeciesName(String referenceName) {
        return referenceRecordHashMap.get(referenceName).getSpeciesName();
    }
}
