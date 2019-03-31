package org.bgi.flexlab.metas.data.structure.sam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: SAMMultiSampleList
 * Description:
 *
 * Note:
 * 1. multiple sample SAM file list format: (all path must be absolute path)
 *  SampleTag samfilePath1_1
 *  SampleTag samfilePath1_2
 *  SampleTag samfilePath1_3
 *  ...
 *  SampleTag2 samfilePath2
 *  ...
 *
 * @author heshixu@genomics.cn
 */

public class SAMMultiSampleList {

    protected static final Log LOG = LogFactory.getLog(SAMMultiSampleList.class.getName());
    private Map<String, Integer> samPathIDMap = new HashMap<>(100);
    private int sampleCount = 0;
    private StringBuilder filePath;

    public SAMMultiSampleList(String list, boolean isLocal, boolean recordPath) throws IOException {
        HashMap<String, Integer> tagSamCount = new HashMap<>(100);

        File file = new File(list);
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line;
        while((line = reader.readLine()) != null) {
            String[] items = line.split("\t");

            if (items.length != 2 ) {
                continue;
            }
            if (isLocal){
                if (items[1].startsWith("/")){
                    items[1] = "file://" + items[1];
                } else {
                    assert items[1].startsWith("file://");
                }
            }

            if (!tagSamCount.containsKey(items[0])){
                tagSamCount.put(items[0], sampleCount);
                sampleCount++;
            }

            samPathIDMap.put(items[1], tagSamCount.get(items[0]));

            if (recordPath) {
                filePath.append(items[1]).append(",");
            }
        }

        filePath.trimToSize();
        reader.close();
    }

    public int getSampleID(String filePath){
        if (samPathIDMap.containsKey(filePath)){
            return samPathIDMap.get(filePath);
        } else {
            LOG.error("[" + this.getClass().getName() + "] :: SAM file " + filePath + " has no sampleID, set default ID: " + sampleCount);
            return sampleCount;
        }
    }

    public int getSampleCount() {
        return sampleCount;
    }
    public String getAllSAMFilePath(){
        if (filePath.length() > 0) {
            return filePath.deleteCharAt(filePath.length() - 1).toString();
        } else {
            return "";
        }
    }
}
