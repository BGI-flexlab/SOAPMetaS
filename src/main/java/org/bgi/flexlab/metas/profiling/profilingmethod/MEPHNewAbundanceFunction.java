package org.bgi.flexlab.metas.profiling.profilingmethod;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 * ClassName: MEPHNewAbundanceFunction
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MEPHNewAbundanceFunction implements PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<Integer, Double>>>, String, ProfilingResultRecord>, Serializable {

    public static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(MEPHNewAbundanceFunction.class);

    //private final Broadcast<Hash  Map<String, ArrayList<String>>> markers2extsBroad;
    //private final Broadcast<HashMap<String, Integer>> markers2lenBroad;
    //private Broadcast<HashMap<String, String>> markers2cladeBroad = null;
    private final Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad; // marker: cladename, len, extsList
    private final Broadcast<HashMap<String, Tuple2<String, String>>> cladeName2HighRankBroad; //taxon: fullname, fullTaxID
    private final Broadcast<ArrayList<Tuple3<ArrayList<String>, Integer, ArrayList<String>>>> taxonomyInformationBroad; //TODO: 后续改成在每个partition读取文件
    private boolean doDisqm = true;
    private double nonZeroPercent = 0.33;
    private double quantile = 0.1;
    private int minNucLen = 2000;
    private char taxaLevel = 'a';
    private ArrayList<String> kingdomList;
    private HashMap<Character, String> taxLevNameDict;
    private int inputNreads = 0;
    private boolean ignoreUnknown;
    private String outFormat;

    /**
     * Statistical type:
     * 2: avg_g
     * 4: avg_l
     * 8: tavg_g
     * 16: tavg_l
     * 32: wavg_g
     * 64: wavg_l
     * 128: med
     **/
    private final int statType;

    public MEPHNewAbundanceFunction(final Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad,
                                 final Broadcast<ArrayList<Tuple3<ArrayList<String>, Integer, ArrayList<String>>>> taxonomyInformationBroad,
                                 final Broadcast<HashMap<String, Tuple2<String, String>>> cladeName2HighRank, MetasOptions options){
        //this.markers2extsBroad = markers2exts;
        //this.markers2lenBroad = markers2len;
        this.markersInformationBroad = markersInformationBroad;
        this.taxonomyInformationBroad = taxonomyInformationBroad;
        this.cladeName2HighRankBroad = cladeName2HighRank;
        this.kingdomList = new ArrayList<>(Arrays.asList("k__Archaea", "k__Bacteria", "k__Eukaryota", "k__Viruses"));
        this.taxLevNameDict = new HashMap<>(8);
        this.taxLevNameDict.put('k', "superkindom");
        this.taxLevNameDict.put('p', "phylum");
        this.taxLevNameDict.put('c', "class");
        this.taxLevNameDict.put('o', "order");
        this.taxLevNameDict.put('f', "family");
        this.taxLevNameDict.put('g', "genus");
        this.taxLevNameDict.put('s', "species");

        this.doDisqm = options.isDoDisqm();
        this.statType = options.getStatType();
        this.inputNreads = options.getTotalNReads();

        this.ignoreUnknown = options.ignoreUnknown();
        this.outFormat = options.getOutputFormat();

        if (this.taxaLevel == 't') {
            this.taxaLevel = 'a'; // "t__" level is omitted implicitly in metaphlan2 2019, because "t__" clade will not be recorded into result dict.
        }
        if (this.outFormat.equals("CAMI")) {
            this.taxaLevel = 'a';
        }

        // MetaPhlAn2 2018 database:
        // markers2clades/exts/lens: 1035649
        // taxa2clades: 16904
        // allClades: 27085
    }

    /**
     * Input:
     *  key: sampleID"\t"rgID"\t"cladeName"\t"markerName
     *  value: tuple(rawNreads, recaliNreads)
     *
     * @param tuple2Iterator
     * @return
     * @throws Exception
     */
    @Override
    public Iterator<Tuple2<String, ProfilingResultRecord>> call(Iterator<Tuple2<String, Tuple2<Integer, Double>>> tuple2Iterator) throws Exception {

        if (!tuple2Iterator.hasNext()){
            return new ArrayList<Tuple2<String, ProfilingResultRecord>>(0).iterator();
        }

        String sampleID;
        String smTag;
        Tuple2<String, Tuple2<Integer, Double>> tupleTemp;
        String[] keyEleTemp;

        // First record, to obtain sampleID and smTag(RGID)
        tupleTemp = tuple2Iterator.next();
        keyEleTemp = tupleTemp._1.split("\t", 4);
        sampleID = keyEleTemp[0];
        smTag = keyEleTemp[1];

        HashMap<String, MEPHNewAbundanceFunction.CladeNode> allClades = new HashMap<>(27086); //cladename: CladeNode

        LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Start Initializing allClades for sample " + smTag);
        allCladesInitialize(allClades);
        LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Finish Initializing allClades and start adding reads for sample " + smTag);

        /*
        Add reads
         */
        String markerNameTemp;
        String cladeNameTemp;

        cladeNameTemp = keyEleTemp[2];
        markerNameTemp = keyEleTemp[3];
        putMarkerRCMap(allClades.get(cladeNameTemp), markerNameTemp, tupleTemp._2);

        // Following records
        while (tuple2Iterator.hasNext()){

            tupleTemp = tuple2Iterator.next();
            keyEleTemp = tupleTemp._1.split("\t", 4);
            cladeNameTemp = keyEleTemp[2];
            markerNameTemp = keyEleTemp[3];

            putMarkerRCMap(allClades.get(cladeNameTemp), markerNameTemp, tupleTemp._2);
        }

        LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Finish adding reads and start computing abundance for sample " + smTag);

        /*
        Compute abundance.
         */
        //double totalAbun = this.kingdomList.stream().mapToDouble(kingdom -> this.computeNodeAbundance(kingdom, cladeMarkers2nreads, allClades)).sum();
        double totalAbun = 0.0;
        int totalNreads = 0;
        double kingdomAbun;
        double kingdomNreads;
        for (String kingdom : kingdomList) {
            kingdomAbun = this.computeNodeAbundance(kingdom, allClades);
            totalAbun += kingdomAbun;
            kingdomNreads = this.computeNodeNreads(kingdom, allClades.get(kingdom));
            totalNreads += kingdomNreads;
            LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Kingdom " + kingdom + " total abundance: " + kingdomAbun + " , total mapped reads: " + kingdomNreads);
        }
        LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Total abundance: " + totalAbun + " | Total mapped reads: " + totalNreads );

        LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Finish computing abundance and start relative abunadnce computing for sample " + smTag);

        if (! (totalAbun > 0.0) ) {
            return new ArrayList<Tuple2<String, ProfilingResultRecord>>(0).iterator();
        }

        /*
        Relative Abundance
         */
        ArrayList<Tuple2<String, ProfilingResultRecord>> results = new ArrayList<>(28000); //key: sampleID; value: ProfilingResultRecord

        // 获取各个 node 的 abundace，此处包括获取 unclassified 的信息。 生成的 ProfilingResult 也要包含 genoLen 信息？
        // 注意对 get_full_name 的需求，可能要考虑存储各个 node 的全名称。 此外，此过程需要考虑是否保存某个分类层级，因为后续
        // 进行 relative abundance 计算de时候，如果每个层级都计算，结果可能会出错

        double mappedReadsPercent = totalNreads/(double) inputNreads;
        if (ignoreUnknown || mappedReadsPercent > 1.0) {
            mappedReadsPercent = 1.0;
        }
        cladeNameTemp = null;
        String rank;
        String taxPath;
        String unclSuffix = "_unclassified".intern();
        String fullNameTemp;
        char taxaLevTemp;

        //　MetaPhlAn2 2019 CAMI 模式的结果理论上应该是 a__ taxolev， 即包含所有分类层级
        if (this.outFormat.equals("CAMI")) {
            for (HashMap.Entry<String, MEPHNewAbundanceFunction.CladeNode> entry : allClades.entrySet()) {
                cladeNameTemp = entry.getKey();
                if (cladeNameTemp.startsWith("t")){
                    continue;
                }
                rank = this.taxLevNameDict.get(cladeNameTemp.charAt(0));
                taxPath = this.cladeName2HighRankBroad.value().get(cladeNameTemp)._2;
                MEPHNewAbundanceFunction.CladeNode node = entry.getValue();
                if (node.abundance == null || node.abundance == 0.0) {
                    continue;
                }
                results.add(new Tuple2<>(sampleID, profilingResultGenerator(
                        this.cladeName2HighRankBroad.value().get(cladeNameTemp)._1 + cladeNameTemp.substring(3),
                        smTag,
                        node.taxID,
                        rank,
                        taxPath,
                        node.rawRC,
                        node.recaliRC,
                        node.abundance,
                        node.abundance/totalAbun * 100 * mappedReadsPercent
                )));


                if (node.unclassifiedAbun > 0.0) {
                    fullNameTemp = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp.substring(3) + "|" + cladeNameTemp.substring(3) + unclSuffix;

                    // The two "nreads" are zero because the unclassifiedAbun is calculated by substraction of sumChildAbun from node.abundance, the correlated "nreads" are not calculated.
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(fullNameTemp, smTag, node.taxID, rank, taxPath, 0, 0, node.unclassifiedAbun, node.unclassifiedAbun/totalAbun * 100 * mappedReadsPercent)));
                }
                if (node.subclUnclassified && !cladeNameTemp.startsWith("s")) {
                    fullNameTemp = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp.substring(3) + "|" + cladeNameTemp.substring(3) + unclSuffix;
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(fullNameTemp, smTag, node.taxID, rank, taxPath, node.rawRC, node.recaliRC, node.abundance, node.abundance/totalAbun * 100 * mappedReadsPercent)));
                }
            }
        } else if (this.taxaLevel == 'a') {
            for (HashMap.Entry<String, MEPHNewAbundanceFunction.CladeNode> entry : allClades.entrySet()) {
                cladeNameTemp = entry.getKey();
                if (cladeNameTemp.startsWith("t")){
                    continue;
                }
                rank = this.taxLevNameDict.get(cladeNameTemp.charAt(0));
                taxPath = this.cladeName2HighRankBroad.value().get(cladeNameTemp)._2;
                MEPHNewAbundanceFunction.CladeNode node = entry.getValue();
                if (node.abundance == null || node.abundance == 0.0) {
                    continue;
                }
                results.add(new Tuple2<>(sampleID, profilingResultGenerator(
                        this.cladeName2HighRankBroad.value().get(cladeNameTemp)._1 + cladeNameTemp,
                        smTag,
                        node.taxID,
                        rank,
                        taxPath,
                        node.rawRC,
                        node.recaliRC,
                        node.abundance,
                        node.abundance/totalAbun * 100 * mappedReadsPercent
                )));


                if (node.unclassifiedAbun > 0.0) {
                    taxaLevTemp = node.children.keySet().iterator().next().substring(0, 3).charAt(0);
                    fullNameTemp = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp + "|";// + taxaLevTemp + cladeNameTemp.substring(3) + unclSuffix;
                    rank = this.taxLevNameDict.get(taxaLevTemp); //这一部分有bug？rank是最后一级，但taxpath和taxID却是上一级，会不会影响丰度评估？

                    // The two "nreads" are zero because the unclassifiedAbun is calculated by substraction of sumChildAbun from node.abundance, the correlated "nreads" are not calculated.
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(fullNameTemp, smTag, node.taxID, rank, taxPath, 0, 0, node.unclassifiedAbun, node.unclassifiedAbun/totalAbun * 100 * mappedReadsPercent)));
                }
                if (node.subclUnclassified && !cladeNameTemp.startsWith("s")) {
                    fullNameTemp = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp + "|";// + getNextTaxaLevel(cladeNameTemp) + cladeNameTemp.substring(1) + unclSuffix;
                    rank = this.taxLevNameDict.get(getNextTaxaLevel(cladeNameTemp));
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(fullNameTemp, smTag, node.taxID, rank, taxPath, node.rawRC, node.recaliRC, node.abundance, node.abundance/totalAbun * 100 * mappedReadsPercent)));
                }
            }
            if (!ignoreUnknown){
                results.add(new Tuple2<>(sampleID, profilingResultGenerator("UNKNOWN", smTag, "-1", "", "", inputNreads - totalNreads, 0, 0, (1-mappedReadsPercent) * 100)));
            }
        } else {
            double sumLevRelAbun = 0.0;
            double relAbun;
            for (HashMap.Entry<String, MEPHNewAbundanceFunction.CladeNode> entry : allClades.entrySet()) {
                cladeNameTemp = entry.getKey();
                if (cladeNameTemp.charAt(0) == taxaLevel) {
                    rank = this.taxLevNameDict.get(cladeNameTemp.charAt(0));
                    taxPath = this.cladeName2HighRankBroad.value().get(cladeNameTemp)._2;
                    MEPHNewAbundanceFunction.CladeNode node = entry.getValue();
                    relAbun = node.abundance/totalAbun * 100 * mappedReadsPercent;
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(
                            this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp,
                            smTag,
                            node.taxID,
                            rank,
                            taxPath,
                            node.rawRC,
                            node.recaliRC,
                            node.abundance,
                            relAbun
                    )));
                    sumLevRelAbun += relAbun;
                    if (node.unclassifiedAbun > 0.0) {
                        fullNameTemp = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp + "|" + node.children.keySet().iterator().next().substring(0, 3) + cladeNameTemp.substring(3) + unclSuffix;
                        relAbun = node.unclassifiedAbun/totalAbun * 100 * mappedReadsPercent;
                        results.add(new Tuple2<>(sampleID, profilingResultGenerator(fullNameTemp, smTag, "-1", "", "", 0, 0, node.unclassifiedAbun, relAbun)));
                        sumLevRelAbun += relAbun;
                    }
                }
            }
            results.add(new Tuple2<>(sampleID, profilingResultGenerator(taxaLevel + "__unclassified", smTag, "", "", "",0, 0, 0, 100 - sumLevRelAbun))); // level_unclassified
        }
        LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Finish relative abundance for sample " + smTag);


        return results.iterator();
    }

    private void putMarkerRCMap(MEPHNewAbundanceFunction.CladeNode node, String marker, Tuple2<Integer, Double> count) {
        MEPHNewAbundanceFunction.CladeNode cladeNodeTemp = node;
        while (cladeNodeTemp.children.size() == 1) {
            cladeNodeTemp = cladeNodeTemp.children.values().iterator().next();
        }
        cladeNodeTemp.markersRCMap.put(marker, count);
    }

    private void allCladesInitialize(HashMap<String, MEPHNewAbundanceFunction.CladeNode> allClades) {
        int cladeGenoLen;
        MEPHNewAbundanceFunction.CladeNode rootNode = new MEPHNewAbundanceFunction.CladeNode();
        MEPHNewAbundanceFunction.CladeNode fatherNode;
        MEPHNewAbundanceFunction.CladeNode temp;
        ArrayList<String> taxLevNameList;
        ArrayList<String> taxIDList;
        String taxLevName;
        String kingdom;
        String taxid;
        int count = 0;
        for (Tuple3<ArrayList<String>, Integer, ArrayList<String>> tuple: this.taxonomyInformationBroad.value()){
            fatherNode = rootNode;
            taxLevNameList = tuple._1();
            cladeGenoLen = tuple._2();
            taxIDList = tuple._3();

            kingdom = taxLevNameList.get(0);
            count = taxLevNameList.size();
            for (int i=0; i<count; i++){
                taxLevName = taxLevNameList.get(i);
                taxid = taxIDList.get(i);
                if (!fatherNode.children.containsKey(taxLevName)){
                    temp = new MEPHNewAbundanceFunction.CladeNode();
                    fatherNode.children.put(taxLevName, temp);
                    allClades.put(taxLevName, temp);
                    temp.fatherClade = fatherNode;
                    temp.kingdom = kingdom;
                    temp.taxID = taxid;
                }
                fatherNode = fatherNode.children.get(taxLevName);
                if (taxLevName.startsWith("t")) {
                    fatherNode.cladeGenomeLen = cladeGenoLen;
                }
                if (taxLevName.contains("_sp")) {
                    fatherNode.spInCladeName = true;
                }
            }
        }
        rootNode.children.values().stream().map(this::calculateNodeGenoLen);

        // markersInformation dict DataStructure: {marker: Tuple(cladename, len, extsList)}
        // Note: the markerRCMap must be initialized here for disqm mode, because the "nonZeroMarkerCount" relies on these "zero' value.
        for (HashMap.Entry<String, Tuple3<String, Integer, ArrayList<String>>> entry: this.markersInformationBroad.value().entrySet()) {
            putMarkerRCMap(allClades.get(entry.getValue()._1()), entry.getKey(), new Tuple2<>(0, 0.0));
        }
    }

    private double calculateNodeGenoLen(MEPHNewAbundanceFunction.CladeNode node) {
        if (node.children.size() < 1) {
            return node.cladeGenomeLen;
        }
        OptionalDouble avgLen = node.children.values().stream()
                .mapToDouble(this::calculateNodeGenoLen)
                .average();
        if (avgLen.isPresent()) {
            node.cladeGenomeLen = avgLen.getAsDouble();
        } else {
            node.cladeGenomeLen = 0.0;
        }
        return node.cladeGenomeLen;
    }

    private char getNextTaxaLevel(String cladeName){
        if (cladeName.startsWith("g")) {
            return 's';
        } else if (cladeName.startsWith("f")) {
            return 'g';
        } else if (cladeName.startsWith("o")) {
            return 'f';
        } else if (cladeName.startsWith("c")) {
            return 'o';
        } else if (cladeName.startsWith("p")) {
            return 'c';
        } else if (cladeName.startsWith("k")) {
            return 'p';
        } else {
            return 'k';
        }
    }

    // core algorithm of MetaPhlAn2
    private double computeNodeAbundance(String clade, HashMap<String, MEPHNewAbundanceFunction.CladeNode> allClades)
            throws NullPointerException {
        MEPHNewAbundanceFunction.CladeNode node = allClades.get(clade);
        //if (node.abundance != null) {
        //    return node.abundance;
        //}
        if (node == null) {
            LOG.error("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Null node for clade: " + clade);
            return 0.0;
            //throw new NullPointerException("Clade " + clade + " has no CladeNode record.");
        } else {
            if (node.abundance != null) {
                return node.abundance;
            }
        }

        double sumChildAbun = 0.0;
        try {
            //sumChildAbun = node.children.keySet().stream().mapToDouble(childClade -> this.computeNodeAbundance(childClade, cladeMarkers2nreads, allClades)).sum();
            for (String childClade : node.children.keySet()) {
                sumChildAbun += this.computeNodeAbundance(childClade, allClades);
                //sumChildAbun += allClades.get(childClade).abundance;
            }
        } catch (NullPointerException e) {
            LOG.error("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Current clade: "
                    + clade + " has null child or child has no abundance record. " + e.toString());
        }

        int markerLenSum = -1;
        double recaliRCSum = 0.0;
        int nonZeroCount = 0;

        ArrayList<Tuple3<Integer, Double, Integer>> discardLenCountList = new ArrayList<>(3); // marker_len, nreads, rawnreads
        ArrayList<Tuple3<Double, Integer, Double>> retainLenCountList = new ArrayList<>(10); // nreads/marker_len, marker_len, nreads

        HashMap<String, Tuple2<Integer, Double>> markersRCMap = node.markersRCMap;
        //if (markersRCMap != null) {
        boolean discard = false;
        String markerName = null;
        MEPHNewAbundanceFunction.CladeNode extSpeClade = null;
        int markerLen = 0;
        double recaliRC = 0.0;
        int rawRC = 0;
        //LOG.info("[SOAPMetas::" + MEPHNewAbundanceFunction.class.getName() + "] Current clade " + clade + " is under raw reads counting. ");
        for (HashMap.Entry<String, Tuple2<Integer, Double>> entry : markersRCMap.entrySet()) { // WARN: 这一步在 metaphlan 中进行了 sort, 会影响后续的 record 回收顺序, 从而影响最终丰度值
            markerName = entry.getKey();
            rawRC = entry.getValue()._1;
            recaliRC = entry.getValue()._2;
            markerLen = markersInformationBroad.value().get(markerName)._2();
            discard = false;

            if (doDisqm) {
                for (String extCladeName : markersInformationBroad.value().get(markerName)._3()) {
                    extSpeClade = allClades.get(extCladeName).fatherClade;
                    while (extSpeClade.children.size() == 1) {
                        extSpeClade = extSpeClade.children.values().iterator().next();
                    }
                    int mc = extSpeClade.markersRCMap.size();
                    int nonZeroMC = this.getNonZeroMarkerCount(extSpeClade);
                    if ((mc > 0) && (nonZeroMC / (double) mc > this.nonZeroPercent)) {
                        discard = true;
                        discardLenCountList.add(new Tuple3<>(markerLen, recaliRC, rawRC));
                        break;
                    }
                }
            }

            if (discard) continue;
            retainLenCountList.add(new Tuple3<>(recaliRC / markerLen, markerLen, recaliRC));
            node.recaliRC += recaliRC;
            node.rawRC += rawRC;
            if (recaliRC > 0) {
                nonZeroCount++;
            }
            markerLenSum += markerLen;
            recaliRCSum += entry.getValue()._2;
        }

        int nDiscard = discardLenCountList.size();
        int nRetain = retainLenCountList.size();
        if (doDisqm && nDiscard > 0){
            int nRipr = 10;
            if (isSingleTerminal(node)) {
                nRipr = 0;
            }
            if (node.kingdom.equals("k__Viruses")){
                nRipr = 0;
            }
            if (nRetain < nRipr) {
                int upBound = Math.min(nRipr - nRetain, nDiscard);
                double recaliRCTemp;
                for (int i=0; i<upBound; i++){
                    Tuple3<Integer, Double, Integer> item = discardLenCountList.get(i);
                    recaliRCTemp = item._2();
                    retainLenCountList.add(new Tuple3<>(recaliRCTemp/(double) item._1(), item._1(), recaliRCTemp));
                    node.recaliRC += recaliRCTemp;
                    node.rawRC += item._3();
                    markerLenSum += item._1();
                    recaliRCSum += recaliRCTemp;
                    if (recaliRCTemp > 0){
                        nonZeroCount++;
                    }
                }
            }
        }

        nRetain = retainLenCountList.size();
        double locAbun = 0.0;
        int lBoundIndex = (int) (this.quantile * nRetain);
        int rBoundIndex = nRetain - lBoundIndex - 1;
        if (markerLenSum == 0) markerLenSum = -1;

        MEPHNewAbundanceFunction.CladeNode fatherClade = node.fatherClade;
        if ( clade.startsWith("t") && ( fatherClade.children.size() > 1 || fatherClade.spInCladeName || node.kingdom.equals("k__Viruses") ) ){
            if (nRetain == 0 || nonZeroCount/(double) nRetain < 0.7) {
                node.abundance = 0.0;
                return 0.0;
            }
        }

        if (markerLenSum > 0) {
            if ((statType & 2) > 0 || (lBoundIndex == 0 && (statType & 40) > 0)){
                // avg_g
                locAbun = recaliRCSum / markerLenSum;
            } else if ((statType & 4) > 0 || (lBoundIndex == 0 && (statType & 80) > 0)) {
                // avg_l
                double sumv = 0;
                for (int i = 0; i < nRetain; i++) {
                    sumv += retainLenCountList.get(i)._1();
                }
                locAbun = sumv / nRetain;
            } else if ((statType & 8) > 0) {
                // tavg_g, default
                retainLenCountList.sort(Comparator.comparing(Tuple3::_1));
                int sumMLen = 0;
                double sumNRead = 0.0;
                for (int i=lBoundIndex; i <= rBoundIndex; i++) {
                    sumMLen += retainLenCountList.get(i)._2();
                    sumNRead += retainLenCountList.get(i)._3();
                }
                if (sumMLen > 0) {
                    locAbun = sumNRead / sumMLen;
                } else {
                    locAbun = 0.0;
                }
            } else if ((statType & 16) > 0) {
                // tavg_l
                retainLenCountList.sort(Comparator.comparing(Tuple3::_1));
                double sumV = 0.0;
                for (int i=lBoundIndex; i <= rBoundIndex; i++) {
                    sumV += retainLenCountList.get(i)._1();
                }
                locAbun = sumV / (rBoundIndex - lBoundIndex + 1);
            } else if ((statType & 32) > 0) {
                // wavg_g
                retainLenCountList.sort(Comparator.comparing(Tuple3::_3));
                double minCount = retainLenCountList.get(lBoundIndex)._3();
                double maxCount = retainLenCountList.get(rBoundIndex)._3();
                double sumCount = 0.0;
                for (int i = lBoundIndex; i <= rBoundIndex; i++) {
                    sumCount += retainLenCountList.get(i)._1();
                }
                locAbun = (minCount * lBoundIndex + maxCount * lBoundIndex + sumCount)/markerLenSum;
            } else if ((statType & 64) > 0) {
                // wavg_l
                retainLenCountList.sort(Comparator.comparing(Tuple3::_1));
                double minV = retainLenCountList.get(lBoundIndex)._1();
                double maxV = retainLenCountList.get(rBoundIndex)._1();
                double sumV = 0.0;
                for (int i=lBoundIndex; i <= rBoundIndex; i++) {
                    sumV += retainLenCountList.get(i)._1();
                }
                locAbun = (minV * lBoundIndex + maxV * lBoundIndex + sumV)/nRetain;
            } else if ((statType & 128) > 0) {
                // med
                retainLenCountList.sort(Comparator.comparing(Tuple3::_1));
                int range = rBoundIndex - lBoundIndex + 1;
                int midindex = lBoundIndex + range/2;
                if (range % 2 == 0) {
                    locAbun = (retainLenCountList.get(midindex)._1() + retainLenCountList.get(midindex - 1)._1()) / 2;
                } else {
                    locAbun = retainLenCountList.get(midindex)._1();
                }
            }
        }

        node.abundance = locAbun;
        if (node.children != null && node.children.size() > 0) {
            // self.children
            if (markerLenSum < this.minNucLen) {
                node.abundance = sumChildAbun;
            } else if (locAbun < sumChildAbun) {
                node.abundance = sumChildAbun;
            }
            // TODO: 这里的计算可能有一点小问题，如果 存在 只属于当前 node 而不属于其子节点的 marker，
            //  而且当前 node 的 locAbun 比它子节点的 sumChildAbun 要小，那么这一部分结果就直接被忽略掉了。
            //  但如果 locAbun 比 sumChildAbun 要大，这里就直接用 locAbun 和 sumChildAbun 的差值作为 unclAbun，
            //  事实上按照计算逻辑， locAbun 本身就应该是 unclAbun （没有 subclade 归属的部分）

            if (node.abundance > sumChildAbun) {
                node.unclassifiedAbun = node.abundance - sumChildAbun;
            }
        } else {
            // not self.children
            if (!clade.startsWith("s") && !clade.startsWith("t")) {
                node.subclUnclassified = true;
            }
        }

        if (clade.startsWith("s")) {
            node.nreads = (int) Math.floor(node.abundance * node.cladeGenomeLen);
        }
        return node.abundance;
    }

    private int computeNodeNreads(String clade, MEPHNewAbundanceFunction.CladeNode node) {
        if (node == null) {
            return 0;
        }
        if (clade.startsWith("s")) {
            return node.nreads;
        }
        int nreads = node.children.entrySet().stream().mapToInt(entry -> this.computeNodeNreads(entry.getKey(), entry.getValue())).sum();
        node.nreads = nreads;
        return nreads;
    }

    private int getNonZeroMarkerCount(MEPHNewAbundanceFunction.CladeNode cladeNode) {
        if (cladeNode.nonZeroMarkerCount != null) {// && cladeNode.nonZeroMarkerCount > 0) {
            return cladeNode.nonZeroMarkerCount;
        }
        int count = 0;
        for (Tuple2<Integer, Double> rc: cladeNode.markersRCMap.values()) {
            if (rc._2 > 0) {
                count++;
            }
        }
        cladeNode.nonZeroMarkerCount = count;
        return count;
    }

    private boolean isSingleTerminal(MEPHNewAbundanceFunction.CladeNode cladeNode) {
        HashMap<String, MEPHNewAbundanceFunction.CladeNode> childrenMap = cladeNode.children;
        int childNum = childrenMap.size();
        if (childNum == 0){
            return true;
        }
        if (childNum > 1){
            return false;
        }
        return isSingleTerminal(childrenMap.values().iterator().next());
    }

    private class CladeNode {
        String kingdom = "UNKNOWN"; // k__Archaea; k__Bacteria; k__Eukaryota;  k__Viruses
        String taxID = "-1";
        boolean spInCladeName = false;

        double cladeGenomeLen = 0.0; // Average genome length of sub-taxas of the clade.
        boolean subclUnclassified = false;

        Double abundance = null;
        double unclassifiedAbun = 0.0;

        Integer nonZeroMarkerCount = null; // count of markers2nreads items of which the "nreads" > 0
        int nreads = 0; // number of reads mapped to the clade

        int rawRC = 0; // raw reads count
        double recaliRC; // recalibrated reads count

        MEPHNewAbundanceFunction.CladeNode fatherClade = null;
        HashMap<String, Tuple2<Integer, Double>> markersRCMap = new HashMap<>(2); // dict of the reads count of each marker, MetaPhlAn2 markers2nreads
        HashMap<String, MEPHNewAbundanceFunction.CladeNode> children = new HashMap<>(2); // key: clade name, value: CladeNode
    }

    private ProfilingResultRecord profilingResultGenerator(String clusterName, String smTag,
                                                           String taxID, String rank, String taxPath,
                                                           int rawnread, double recalinread, double abundance,
                                                           double relAbun) {

        ProfilingResultRecord resultRecord;

        //if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
        //    resultRecord = new ProfilingEveResultRecord();
        //} else {
        //
        //}
        if (outFormat.equals("CAMI")) {
            resultRecord = new ProfilingResultRecord(8);
        } else if (outFormat.equals("DETAILED")) {
            resultRecord = new ProfilingResultRecord(4);
        } else {
            resultRecord = new ProfilingResultRecord(2);
        }

        resultRecord.setClusterName(clusterName);

        resultRecord.setSmTag(smTag);

        resultRecord.setRawReadCount(rawnread);
        resultRecord.setrecaliReadCount(recalinread);
        resultRecord.setAbundance(abundance);
        resultRecord.setRank(rank);
        resultRecord.setTermTaxID(taxID);
        resultRecord.setTaxPath(taxPath);
        resultRecord.setRelAbun(relAbun);

        return resultRecord;
    }
}
