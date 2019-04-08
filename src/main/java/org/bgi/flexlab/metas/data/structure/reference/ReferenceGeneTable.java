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

public class ReferenceGeneTable {

    private static final Log LOG = LogFactory.getLog(ReferenceGeneTable.class);

    private Map<String, ReferenceGeneRecord> referenceRecordMap;

    /**
     * Reference matrix file format:
     * geneID	geneName	geneLength	geneGC	species[	genus	phylum]
     *
     * species gc file format:
     * s__Genusname_speciesname	float
     *
     * TODO: 需要完善markers的文件读取和矩阵构建方法。另外要考虑HashMap类型是否适用，有没有更高效的data structure
     *
     * @param referenceMatrixFilePath Information matrix of marker gene
     * @param speciesGCFilePath species GC content list
     */
    public ReferenceGeneTable(String referenceMatrixFilePath, String speciesGCFilePath){
        Map<String, ReferenceSpeciesRecord> speciesRecordMap = new HashMap<>(200);

        try (FileInputStream speciesFR = new FileInputStream(new File(speciesGCFilePath))){

            BufferedReader speciesBR = new BufferedReader(new InputStreamReader(speciesFR));
            String currentLine = null;

            assert speciesBR.ready(): "Species GC file is not readable.";

            while ((currentLine = speciesBR.readLine()) != null) {
                String[] lineSplit = StringUtils.split(currentLine, '\t');
                ReferenceSpeciesRecord speciesRecord = new ReferenceSpeciesRecord(lineSplit[0],
                        Double.parseDouble(lineSplit[1]));
                speciesRecordMap.put(lineSplit[1], speciesRecord);
            }

            speciesBR.close();

        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] " + ReferenceGeneTable.class.getName() + "] Can't load species gc file.");
        }

        this.readMatrixFile(referenceMatrixFilePath, speciesRecordMap);
    }

    public ReferenceGeneTable(String referenceMatrixFilePath){
        Map<String, ReferenceSpeciesRecord> speciesRecordMap = new HashMap<>(0);
        this.readMatrixFile(referenceMatrixFilePath, speciesRecordMap);
    }

    private void readMatrixFile(String matrixFilePath,
                                Map<String, ReferenceSpeciesRecord> speciesRecordMap){
        try (FileInputStream matrixFR = new FileInputStream(new File(matrixFilePath))) {
            this.referenceRecordMap = new HashMap<>(9879900); // Number of genes in IGC_9.9M_update.ref

            BufferedReader matrixBR = new BufferedReader(new InputStreamReader(matrixFR));
            String currentLine = null;

            assert matrixBR.ready(): "gene matrix file is not readable";

            while ((currentLine = matrixBR.readLine()) != null) {
                String[] lineSplit = StringUtils.split(currentLine, '\t');
                ReferenceGeneRecord geneRecord = new ReferenceGeneRecord(null,
                        Integer.parseInt(lineSplit[2]), Double.parseDouble(lineSplit[3]),
                        speciesRecordMap.getOrDefault(lineSplit[4], null));
                this.referenceRecordMap.put(lineSplit[1], geneRecord);
            }

            matrixBR.close();

        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] " + ReferenceGeneTable.class.getName() + "] Can't load reference matrix file.");
        }
    }

    public int getGeneLength(String geneName) {
        return referenceRecordMap.get(geneName).getGeneLength();
    }

    public double getGeneGCContent(String referenceName) {
        double gc = referenceRecordMap.get(referenceName).getGcContent();
        if (gc == 0){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] species of gene" + referenceName + ". Return 0.");
        }
        return gc;
    }

    public String getGeneSpeciesName(String referenceName) {
        String name = referenceRecordMap.get(referenceName).getSpeciesName();
        if (name == null){
            LOG.error("[SOAPMetas::" + ReferenceGeneTable.class.getName() + "] gene" + referenceName + ". Return Unknown.");
            return "Unknown";
        }
        return name;
    }

    public double getSpeciesGenoGC(String referenceName){
        return referenceRecordMap.get(referenceName).getSpeciesGenomeGC();
    }

}
