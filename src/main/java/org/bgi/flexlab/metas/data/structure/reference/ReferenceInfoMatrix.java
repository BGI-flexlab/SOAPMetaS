package org.bgi.flexlab.metas.data.structure.reference;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: ReferenceInfoMatrix
 * Description: Related marker genes and correspond species genome information are stored in the instance
 * of this class, which will then be utilized to calculate abundance.
 *
 * Note: The application will run normally if the marker gene information in reference matrix file is
 * replaced with "genome" information, and this is functionally similar to species-level analysis. However,
 * users must pay attention that the builtin GC bias recalibration model won't work as expected.
 *
 * @author heshixu@genomics.cn
 */

public class ReferenceInfoMatrix implements Serializable{

    public static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(ReferenceInfoMatrix.class);

    private final int MARKER_NUMBER = 10;
    private final int SPECIES_NUMBER = 5;

    private Map<String, ReferenceGeneRecord> markerRecordMap;
    private Map<String, ReferenceSpeciesRecord> refSpeciesRecordMap;

    /**
     * Reference matrix file format:
     * geneID	geneName	geneLength	species[	genus	phylum  geneGC]
     *
     * species file format:
     * s__Genusname_speciesname genomeLength    genomeGC
     *
     * @param referenceMatrixFilePath Information matrix of marker gene
     * @param speciesGCFilePath species GC content list
     */
    public ReferenceInfoMatrix(String referenceMatrixFilePath, String speciesGCFilePath){
        this.readMatrixFile(referenceMatrixFilePath);

        if (speciesGCFilePath != null) {
            this.refSpeciesRecordMap = new HashMap<>(SPECIES_NUMBER);

            try (FileInputStream speciesFR = new FileInputStream(new File(speciesGCFilePath))) {

                BufferedReader speciesBR = new BufferedReader(new InputStreamReader(speciesFR));

                String currentLine;
                while ((currentLine = speciesBR.readLine()) != null) {
                    LOG.trace("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Species GC file, current line: " + currentLine);
                    String[] lineSplit = StringUtils.split(currentLine, '\t');
                    ReferenceSpeciesRecord speciesRecord = new ReferenceSpeciesRecord(null, Integer.parseInt(lineSplit[1]),
                            Double.parseDouble(lineSplit[2]));
                    this.refSpeciesRecordMap.put(lineSplit[0], speciesRecord);
                }

                speciesBR.close();

            } catch (NumberFormatException e){
                LOG.error("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Wrong number format of species genome length or genome GC content.");
            } catch (IOException e) {
                LOG.error("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Can't load species gc file: " + speciesGCFilePath);
            }
        }
    }

    //public ReferenceInfoMatrix(String referenceMatrixFilePath){
    //    this.readMatrixFile(referenceMatrixFilePath);
    //}

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

            String currentLine;
            while ((currentLine = matrixBR.readLine()) != null) {
                String[] lineSplit = StringUtils.split(currentLine, '\t');
                ReferenceGeneRecord geneRecord;
                if (lineSplit.length == 4) {
                    geneRecord = new ReferenceGeneRecord(null, Integer.parseInt(lineSplit[2]),
                            lineSplit[3]);
                } else if (lineSplit.length == 7) {
                    geneRecord = new ReferenceGeneRecord(null, Integer.parseInt(lineSplit[2]),
                            lineSplit[3], Double.parseDouble(lineSplit[6]));
                } else {
                    continue;
                }
                this.markerRecordMap.put(lineSplit[1], geneRecord);
                LOG.trace("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Marker gene matrix file. " +
                        "Current line: " + currentLine + " || Key of current line: " + lineSplit[1]);
            }

            matrixBR.close();

        } catch (NumberFormatException e){
            LOG.error("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Wrong number format of gene length or gene GC content.");
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Can't load reference matrix file: " + matrixFilePath);
        }
    }

    /**
     * Get gene length of specific gene.
     *
     * @param geneName Marker name.
     * @return int Gene length. 0 if no length info.
     */
    public int getGeneLength(String geneName) {

        ReferenceGeneRecord geneRec = markerRecordMap.getOrDefault(geneName, null);

        if (geneRec == null){
            LOG.warn("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Gene" + geneName +
                    " doesn't has gene info. Return 0 as length.");
            return 0;
        }
        return geneRec.getGeneLength();
    }

    public double getGeneGCContent(String geneName) {
        ReferenceGeneRecord geneRec = markerRecordMap.get(geneName);

        if (geneRec == null){
            LOG.warn("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Gene" + geneName +
                    " doesn't has gene info. Return 0 as gc content.");
            return 0;
        }
        return geneRec.getGcContent();
    }

    public String getGeneSpeciesName(String referenceName) {
        String name = "Unknown";
        try {
            ReferenceGeneRecord record = markerRecordMap.get(referenceName);
            name = record.getSpeciesName();
        } catch (NullPointerException e){
            LOG.error("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Reference " + referenceName +
                    " may be omitted in reference matrix file.");
        }
        if (name.equals("Unknown")){
            LOG.warn("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Gene " + referenceName +
                    " doesn't has species info. Return \"Unknown\" as species name.");
            return name.intern();
        }
        return name;
    }

    public int getSpeciesGenoLen(String referenceName){
        ReferenceSpeciesRecord speciesRecord = this.refSpeciesRecordMap.getOrDefault(referenceName, null);
        if (speciesRecord == null){
            LOG.warn("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Species " + referenceName +
                    " doesn't has genome info. Return 1 as genome length.");
            return 0;
        }
        return speciesRecord.getGenomeLength();
    }

    public double getSpeciesGenoGC(String referenceName){
        ReferenceSpeciesRecord speciesRecord = this.refSpeciesRecordMap.getOrDefault(referenceName, null);
        if (speciesRecord == null){
            LOG.warn("[SOAPMetas::" + ReferenceInfoMatrix.class.getName() + "] Species " + referenceName +
                    " doesn't has genome info. Return 0 as GC content.");
            return 0;
        }
        return speciesRecord.getGenomeGCContent();
    }

}
