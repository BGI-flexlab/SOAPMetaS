package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SAMFileHeaderFactory {
    private static final Log LOG = LogFactory.getLog(SAMFileHeaderFactory.class);

    private SAMSequenceDictionary dict;

    /**
     * Reference matrix file format:
     * geneID	geneName	geneLength	species[   geneGC[	genus	phylum]]
     *
     * TODO 应该使用hadoop方式读取文件，否则若集群没有挂载共享存储...
     *
     * @param matrixFilePath reference matrix file
     */
    public SAMFileHeader createHeader(String matrixFilePath, Set<String> samples){
        createDict(matrixFilePath);
        SAMFileHeader header = new SAMFileHeader(dict);
        addReadGroupRecord(header, samples);
        return header;
    }

    public SAMSequenceDictionary createDict(String matrixFilePath){

        List<SAMSequenceRecord> samSequenceRecordList = new ArrayList<>();
        try (FileInputStream matrixFR = new FileInputStream(new File(matrixFilePath))) {

            BufferedReader matrixBR = new BufferedReader(new InputStreamReader(matrixFR));

            String currentLine;
            while ((currentLine = matrixBR.readLine()) != null) {
                String[] lineSplit = currentLine.split("\t",4);
                int len = Integer.parseInt(lineSplit[2]);
                samSequenceRecordList.add(new SAMSequenceRecord(lineSplit[1], len));
            }
        } catch (NumberFormatException e){
            LOG.error("[SOAPMetas::" + SAMFileHeaderFactory.class.getName() + "] Wrong number format of gene length or gene GC content.");
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + SAMFileHeaderFactory.class.getName() + "] Can't load reference matrix file: " + matrixFilePath);
        }

        dict = new SAMSequenceDictionary(samSequenceRecordList);

        return dict;
    }


    //约定RG ID与RG SM相同
    private void addReadGroupRecord(SAMFileHeader header, Set<String> samples){
        for (String sampleName: samples){
            SAMReadGroupRecord readGroupRecord = new SAMReadGroupRecord(sampleName);
            readGroupRecord.setSample(sampleName);
            header.addReadGroup(readGroupRecord);
        }
    }

    private void addReadGroupRecord(SAMFileHeader header, String sampleList){

        try (FileInputStream matrixFR = new FileInputStream(new File(sampleList))) {

            BufferedReader matrixBR = new BufferedReader(new InputStreamReader(matrixFR));

            String currentLine;
            while ((currentLine = matrixBR.readLine()) != null) {
                String[] lineSplit = currentLine.split("\t",2);
                SAMReadGroupRecord readGroupRecord = new SAMReadGroupRecord(lineSplit[0]);
                readGroupRecord.setSample(lineSplit[0]);
                header.addReadGroup(readGroupRecord);
            }

        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + SAMFileHeaderFactory.class.getName() + "] Can't load sampleList file: " + sampleList);
        }
    }



    public void setDict(SAMSequenceDictionary dict) {
        this.dict = dict;
    }

    public SAMSequenceDictionary getDict() {
        return dict;
    }
}
