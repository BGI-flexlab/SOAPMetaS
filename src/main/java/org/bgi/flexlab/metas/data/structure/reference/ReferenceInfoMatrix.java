package org.bgi.flexlab.metas.data.structure.reference;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: ReferenceGeneTable
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class ReferenceGeneTable implements Serializable{

    public static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(ReferenceGeneTable.class);

    private final static int MARKER_NUMBER = 9879900;
    private final static int SPECIES_NUMBER = 200;

    private Map<String, ReferenceGeneRecord> markerRecordMap;
    private Map<String, ReferenceSpeciesRecord> refSpeciesRecordMap;

    /**
     * Reference matrix file format:
     * geneID	geneName	geneLength	geneGC	species[	genus	phylum]
     *
     * species file format:
     * s__Genusname_speciesname genomeLength    float
     *
     * @param referenceMatrixFilePath Information matrix of marker gene
     * @param speciesGCFilePath species GC content list
     */
    public ReferenceGeneTable(String referenceMatrixFilePath, String speciesGCFilePath){
        this.refSpeciesRecordMap = new HashMap<>(SPECIES_NUMBER);

        try (FileInputStream speciesFR = new FileInputStream(new File(speciesGCFilePath))){

            BufferedReader speciesBR = new BufferedReader(new InputStreamReader(speciesFR));
            String currentLine = null;

            while ((currentLine = speciesBR.readLine()) != null) {
                LOG.trace("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] Species GC file, current line: " + currentLine);
                String[] lineSplit = StringUtils.split(currentLine, '\t');
                ReferenceSpeciesRecord speciesRecord = new ReferenceSpeciesRecord(lineSplit[0], Integer.parseInt(lineSplit[1]),
                        Double.parseDouble(lineSplit[2]));
                this.refSpeciesRecordMap.put(lineSplit[1], speciesRecord);
            }

            speciesBR.close();

        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] Can't load species gc file: " + speciesGCFilePath);
        }

        this.readMatrixFile(referenceMatrixFilePath);
    }

    public ReferenceGeneTable(String referenceMatrixFilePath){
        this.readMatrixFile(referenceMatrixFilePath);
    }

    /**
     * Reference matrix file format:
     * geneID	geneName	geneLength	species[	genus	phylum  geneGC]
     *
     * TODO: 更高效的matrix存储方式，除了hashMap
     * TODO: 考虑是否可以采用 sparkcontext.textfile来读取并生成map
     *
     * @param matrixFilePath reference matrix file
     */
    private void readMatrixFile(String matrixFilePath){
        try (FileInputStream matrixFR = new FileInputStream(new File(matrixFilePath))) {
            this.markerRecordMap = new HashMap<>(MARKER_NUMBER); // Number of genes in IGC_9.9M_update.ref

            BufferedReader matrixBR = new BufferedReader(new InputStreamReader(matrixFR));
            String currentLine = null;

            while ((currentLine = matrixBR.readLine()) != null) {
                String[] lineSplit = StringUtils.split(currentLine, '\t');
                ReferenceGeneRecord geneRecord;
                if (lineSplit.length == 4) {
                    geneRecord = new ReferenceGeneRecord(null, Integer.parseInt(lineSplit[2]),
                            lineSplit[3]);
                } else if (lineSplit.length == 7){
                    geneRecord = new ReferenceGeneRecord(null, Integer.parseInt(lineSplit[2]),
                            lineSplit[3], Double.parseDouble(lineSplit[6]));
                } else {
                    continue;
                }
                this.markerRecordMap.put(lineSplit[1], geneRecord);
                LOG.trace("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] Marker gene matrix file. " +
                        "Current line: " + currentLine + " || Key of current line: " + lineSplit[1]);
            }

            matrixBR.close();

        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] Can't load reference matrix file: " + matrixFilePath);
        }
    }

    public int getGeneLength(String geneName) {
        return markerRecordMap.get(geneName).getGeneLength();
    }

    public double getGeneGCContent(String referenceName) {
        double gc = markerRecordMap.get(referenceName).getGcContent();
        if (gc == 0){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] Species of gene" + referenceName + " doesn't has gc content info. Return 0.");
        }
        return gc;
    }

    public String getGeneSpeciesName(String referenceName) {
        String name = "Unknown";
        try {
            ReferenceGeneRecord record = markerRecordMap.get(referenceName);
            name = record.getSpeciesName();
        } catch (NullPointerException e){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] Reference " + referenceName +
                    " may be omitted in reference matrix file.");
        }
        if (name.equals("Unknown")){
            LOG.warn("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] Gene " + referenceName +
                    " doesn't has species info. Return \"Unknown\" as species name.");
            return name.intern();
        }
        return name;
    }

    public int getSpeciesGenoLen(String referenceName){
        return this.refSpeciesRecordMap.get(referenceName).getGenomeLength();
    }

    public double getSpeciesGenoGC(String referenceName){
        return this.refSpeciesRecordMap.get(referenceName).getGenomeGCContent();
    }

}
