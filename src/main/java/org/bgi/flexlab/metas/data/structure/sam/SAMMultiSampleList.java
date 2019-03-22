package org.bgi.flexlab.metas.data.structure.sam;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * ClassName: SAMMultiSampleList
 * Description:
 *
 * Note:
 * 1. multiple sample SAM file list format: (all path must be absolute path)
 *  SampleTag samfilePath1_1
 *  SampleTag samfilePath1_2
 *  SampleTag2 samfilePath2
 *  ...
 *
 * @author: heshixu@genomics.cn
 */

public class SAMMultiSampleList {
    private HashMap<String, Integer> samPathIDMap = new HashMap<>();
    private int sampleCount = 0;
    private StringBuilder filePath;

    public SAMMultiSampleList(String list, boolean isLocal, boolean recordPath) throws IOException {
        HashMap<String, Integer> tagSamCount = new HashMap<>();

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
        return samPathIDMap.getOrDefault(filePath, sampleCount);
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
