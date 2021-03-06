package org.bgi.flexlab.metas.data.structure.sam;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;

/**
 * ClassName: SAMMultiSampleList
 * Description:
 *
 * Note:
 * 1. multiple sample SAM file list format: (all path must be absolute path)
 *  ReadGroupID sample(SMTag) samfilePath1_1
 *  ReadGroupID sample(SMTag) samfilePath1_2
 *  ReadGroupID sample(SMTag) samfilePath1_3
 *  ...
 *  ReadGroupID2 sample2(SMTag) samfilePath2
 *  ...
 *
 * @author heshixu@genomics.cn
 */

public class SAMMultiSampleList implements Serializable {

    public static final long serialVersionUID = 1L;

    protected static final Log LOG = LogFactory.getLog(SAMMultiSampleList.class.getName());

    private Map<String, Integer> samPathIDMap = null;
    HashMap<String, Integer> sampleIDbySampleName;
    private int sampleCount = 0;
    private StringBuilder filePath;

    public SAMMultiSampleList(String list, boolean isLocal, boolean recordSample, boolean recordPath) throws IOException {
        sampleIDbySampleName = new HashMap<>();

        if (recordSample){
            samPathIDMap = new HashMap<>(100);
        }
        if (recordPath){
            filePath = new StringBuilder(128);
        }

        File file = new File(list);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

        String line;
        String sampleTag;
        while((line = reader.readLine()) != null) {
            //LOG.trace("[SOAPMetas::" + SAMMultiSampleList.class.getName() + "] Current sample line: " + line);
            String[] items = StringUtils.split(line, '\t');

            if (items.length != 3 ) {
                continue;
            }

            sampleTag = items[1];
            if (isLocal){
                if (items[2].startsWith("/")){
                    items[2] = "file://" + items[2];
                }
            }

            if (!sampleIDbySampleName.containsKey(sampleTag)){
                sampleIDbySampleName.put(sampleTag, sampleCount);
                sampleCount++;
            }

            if (recordSample) {
                samPathIDMap.put(items[2], sampleIDbySampleName.get(sampleTag));
            }

            //LOG.trace("[SOAPMetas::" + SAMMultiSampleList.class.getName() + "] Current sample_line info: " +
            //        "RGID: " + items[0] + " || sampleID: " + tagSamCount.get(items[0]) + " || path: " + items[1]);

            if (recordPath) {
                this.filePath.append(items[2]);
                this.filePath.append(',');
                //LOG.trace("[SOAPMetas::" + SAMMultiSampleList.class.getName() + "] Save SAM record file: " + items[1]);
            }
        }

        reader.close();
    }

    public SAMMultiSampleList(List<String> list, boolean isLocal, boolean recordSample, boolean recordPath) throws IOException {
        sampleIDbySampleName = new HashMap<>();

        if (recordSample){
            samPathIDMap = new HashMap<>(100);
        }
        if (recordPath){
            filePath = new StringBuilder(128);
        }

        String sampleTag;
        for (String line : list) {
            //LOG.trace("[SOAPMetas::" + SAMMultiSampleList.class.getName() + "] Current sample line: " + line);
            String[] items = StringUtils.split(line, '\t');

            if (items.length != 3) {
                continue;
            }

            sampleTag = items[1];
            if (isLocal) {
                if (items[2].startsWith("/")) {
                    items[2] = "file://" + items[2];
                }
            }

            if (!sampleIDbySampleName.containsKey(sampleTag)) {
                sampleIDbySampleName.put(sampleTag, sampleCount);
                sampleCount++;
            }

            if (recordSample) {
                samPathIDMap.put(items[2], sampleIDbySampleName.get(sampleTag));
            }

            //LOG.trace("[SOAPMetas::" + SAMMultiSampleList.class.getName() + "] Current sample_line info: " +
            //        "RGID: " + items[0] + " || sampleID: " + tagSamCount.get(items[0]) + " || path: " + items[1]);

            if (recordPath) {
                this.filePath.append(items[2]);
                this.filePath.append(',');
                //LOG.trace("[SOAPMetas::" + SAMMultiSampleList.class.getName() + "] Save SAM record file: " + items[1]);
            }
        }
    }

    public int getSampleID(String filePath){
        Iterator<String> keyIter = samPathIDMap.keySet().iterator();
        String key;
        while (keyIter.hasNext()){
            key = keyIter.next();
            if (key.contains(filePath)){
                return this.samPathIDMap.get(key);
            }
        }
        LOG.error("[SOAPMetas::" + SAMMultiSampleList.class.getName() + "] SAM file " + filePath + " has no sampleID, set default ID: " + sampleCount);
        return sampleCount;
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

    public HashMap<String, Integer> getSampleIDbySampleName() {
        return sampleIDbySampleName;
    }
}
