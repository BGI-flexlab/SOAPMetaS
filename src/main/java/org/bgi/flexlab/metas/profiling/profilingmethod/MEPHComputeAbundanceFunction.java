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
 * ClassName: MEPHComputeAbundanceFunction
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MEPHComputeAbundanceFunction implements PairFlatMapFunction<Iterator<Tuple2<String, HashMap<String, Tuple2<Integer, Double>>>>, String, ProfilingResultRecord>, Serializable {

    public static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(MEPHProfilingMethod.class);

    //private final Broadcast<HashMap<String, ArrayList<String>>> markers2extsBroad;
    //private final Broadcast<HashMap<String, Integer>> markers2lenBroad;
    //private Broadcast<HashMap<String, String>> markers2cladeBroad = null;
    private final Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad;
    private final Broadcast<HashMap<String, String>> cladeName2HighRankBroad;
    private final Broadcast<ArrayList<Tuple2<ArrayList<String>, Integer>>> taxonomyInformationBroad; //TODO: 后续改成在每个partition读取文件
    private boolean doDisqm = true;
    private double quantile = 0.1;
    private int minNucLen = 2000;
    private String taxaLevel = "a__";
    private ArrayList<String> kingdomList;

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
    private final int statType = 8;

    public MEPHComputeAbundanceFunction(final Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad,
                                        final Broadcast<ArrayList<Tuple2<ArrayList<String>, Integer>>> taxonomyInformationBroad,
                                        final Broadcast<HashMap<String, String>> cladeName2HighRank){
        //this.markers2extsBroad = markers2exts;
        //this.markers2lenBroad = markers2len;
        this.markersInformationBroad = markersInformationBroad;
        this.taxonomyInformationBroad = taxonomyInformationBroad;
        this.cladeName2HighRankBroad = cladeName2HighRank;
        this.kingdomList = new ArrayList<>(Arrays.asList("k__Archaea", "k__Bacteria", "k__Eukaryota", "k__Viruses"));

        // markers2clades/exts/lens: 1035649
        // taxa2clades: 16904
        // allClades: 27085
    }

    /**
     * Input:
     *  key: sampleID"\t"rgID
     *  value: marker2nreads (including rawNreads and recaliNreads)
     *
     *
     *
     * @param tuple2Iterator
     * @return
     * @throws Exception
     */
    @Override
    public Iterator<Tuple2<String, ProfilingResultRecord>> call(Iterator<Tuple2<String, HashMap<String, Tuple2<Integer, Double>>>> tuple2Iterator) throws Exception {

        String sampleID;
        String smTag;

        if (!tuple2Iterator.hasNext()){
            return new ArrayList<Tuple2<String, ProfilingResultRecord>>(0).iterator();
        }

        HashMap<String, CladeNode> allClades = new HashMap<>(27086); //cladename: CladeNode
        allCladesInitialize(allClades);

        Tuple2<String, HashMap<String, Tuple2<Integer, Double>>> tupleTemp = tuple2Iterator.next();
        HashMap<String, Tuple2<Integer, Double>> marker2NreadsTemp;
        String cladeNameTemp;
        CladeNode cladeNodeTemp;
        int nonzeroMarkerCountTemp = 0;

        String[] keyEleTemp = tupleTemp._1.split("\t");
        sampleID = keyEleTemp[0];
        smTag = keyEleTemp[1];
        cladeNameTemp = keyEleTemp[2];
        keyEleTemp = null;
        marker2NreadsTemp = tupleTemp._2;

        HashMap<String, HashMap<String, Tuple2<Integer, Double>>> cladeMarkers2nreads = new HashMap<>(27086);

        cladeMarkers2nreads.put(cladeNameTemp, marker2NreadsTemp);
        cladeNodeTemp = allClades.get(cladeNameTemp);
        cladeNodeTemp.markerCount = marker2NreadsTemp.size();
        for (Tuple2<Integer, Double> countTuple: marker2NreadsTemp.values()) {
            if (countTuple._2 > 0) nonzeroMarkerCountTemp++;
        }
        cladeNodeTemp.nonZeroMarkerCount = nonzeroMarkerCountTemp;

        //tupleTemp = null;
        //cladeNameTemp = null;

        while (tuple2Iterator.hasNext()){
            nonzeroMarkerCountTemp = 0;
            tupleTemp = tuple2Iterator.next();
            cladeNameTemp = tupleTemp._1.split("\t")[2];
            cladeMarkers2nreads.put(cladeNameTemp, tupleTemp._2);
            cladeNodeTemp = allClades.get(cladeNameTemp);
            cladeNodeTemp.markerCount = marker2NreadsTemp.size();
            for (Tuple2<Integer, Double> countTuple: marker2NreadsTemp.values()) {
                if (countTuple._2 > 0) nonzeroMarkerCountTemp++;
            }
            cladeNodeTemp.nonZeroMarkerCount = nonzeroMarkerCountTemp;
        }
        // allClades的初始化能否放到compute函数中，在函数中第一个children循环采用外部的hashset来获取子节点名称。所以广播变量还需要提供一个外部的taxa_tree

        double totalAbun = 0.0;
        try {
            totalAbun = this.kingdomList.parallelStream().mapToDouble(kingdom -> this.computeNodeAbundance(kingdom, cladeMarkers2nreads, allClades)).sum();
            //for (String kingdom : kingdomList) {
            //    totalAbun += this.computeNodeAbundance(kingdom, cladeMarkers2nreads, allClades);
            //}
        } catch (NullPointerException e){
            LOG.error("[SOAPMetas::" + MEPHComputeAbundanceFunction.class.getName() + "] " + e.toString());
        }

        if (! (totalAbun > 0.0) ) {
            return new ArrayList<Tuple2<String, ProfilingResultRecord>>(0).iterator();
        }

        ArrayList<Tuple2<String, ProfilingResultRecord>> results = new ArrayList<>(28000); //key: sampleID; value: ProfilingResultRecord

        // 获取各个 node 的 abundace，此处包括获取 unclassified 的信息。 生成的 ProfilingResult 也要包含 genoLen 信息？
        // 注意对 get_full_name 的需求，可能要考虑存储各个 node 的全名称。 此外，此过程需要考虑是否保存某个分类层级，因为后续
        // 进行 relative abundance 计算de时候，如果每个层级都计算，结果可能会出错

        cladeNameTemp = null;
        if (taxaLevel.startsWith("a")) {
            for (HashMap.Entry<String, CladeNode> entry : allClades.entrySet()) {
                cladeNameTemp = entry.getKey();
                CladeNode node = entry.getValue();
                if (node.abundance == null || node.abundance == 0.0) {
                    continue;
                }
                results.add(new Tuple2<>(sampleID, profilingResultGenerator(this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp, smTag, node.rawnreads, node.recaliNreads, node.abundance, node.abundance/totalAbun)));

                String name;
                String unclSuffix = "_unclassified".intern();
                if (node.uncl_abundance > 0.0) {
                    name = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp + "|" + node.children.keySet().iterator().next().substring(0, 3) + cladeNameTemp.substring(3) + unclSuffix;

                    // The two "nreads" are zero because the uncl_abundance is calculated by substraction of sumAbun from node.abundance, the correlated "nreads" are not calculated.
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(name, smTag, 0, 0, node.uncl_abundance, node.uncl_abundance/totalAbun)));
                }
                if (node.subcl_uncl && !cladeNameTemp.startsWith("s")) {
                    name = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp + "|" + getNextTaxaLevel(cladeNameTemp) + cladeNameTemp.substring(1) + unclSuffix;
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(name, smTag, node.rawnreads, node.recaliNreads, node.abundance, node.abundance/totalAbun)));
                }
            }
        } else {
            double sumLevRelAbun = 0.0;
            double relAbun;
            for (HashMap.Entry<String, CladeNode> entry : allClades.entrySet()) {
                cladeNameTemp = entry.getKey();
                if (cladeNameTemp.startsWith(taxaLevel)) {
                    CladeNode node = entry.getValue();
                    results.add(new Tuple2<>(sampleID, profilingResultGenerator(this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp, smTag, node.rawnreads, node.recaliNreads, node.abundance, node.abundance/totalAbun)));

                    String name;
                    String unclSuffix = "_unclassified".intern();
                    if (node.uncl_abundance > 0.0) {
                        name = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp + "|" + node.children.keySet().iterator().next().substring(0, 3) + cladeNameTemp.substring(3) + unclSuffix;
                        relAbun = node.uncl_abundance/totalAbun;
                        results.add(new Tuple2<>(sampleID, profilingResultGenerator(name, smTag, 0, 0, node.uncl_abundance, relAbun)));
                        sumLevRelAbun += relAbun;
                    }
                    if (node.subcl_uncl && !cladeNameTemp.startsWith("s")) {
                        name = this.cladeName2HighRankBroad.value().get(cladeNameTemp) + cladeNameTemp + "|" + getNextTaxaLevel(cladeNameTemp) + cladeNameTemp.substring(1) + unclSuffix;
                        relAbun = node.abundance/totalAbun;
                        results.add(new Tuple2<>(sampleID, profilingResultGenerator(cladeNameTemp, smTag, node.rawnreads, node.recaliNreads, node.abundance, relAbun)));
                        sumLevRelAbun += relAbun;
                    }
                }
            }
            results.add(new Tuple2<>(sampleID, profilingResultGenerator(taxaLevel + "unclassified", smTag, 0, 0, 0, 1-sumLevRelAbun))); // level_unclassified
        }

        return results.iterator();
    }

    private void allCladesInitialize(HashMap<String, CladeNode> allClades) {
        int cladeGenoLen;
        CladeNode rootNode = new CladeNode();
        CladeNode fatherNode = rootNode;
        CladeNode temp;
        String kingdom = "UNKNOWN";
        for (Tuple2<ArrayList<String>, Integer> tuple: this.taxonomyInformationBroad.value()){
            cladeGenoLen = tuple._2;
            for (String taxLevName: tuple._1){
                if (taxLevName.startsWith("k")) {
                    kingdom = taxLevName;
                }
                if (!fatherNode.children.containsKey(taxLevName)){
                    temp = new CladeNode();
                    fatherNode.children.put(taxLevName, temp);
                    allClades.put(taxLevName, temp);
                    temp.fatherClade = fatherNode;
                    temp.kingdom = kingdom;
                }
                fatherNode = fatherNode.children.get(taxLevName);
                if (taxLevName.startsWith("t")) {
                    fatherNode.cladeGenomeLen = cladeGenoLen;
                }
                if (taxLevName.contains("_sp")) {
                    fatherNode.spInClade = true;
                }
            }
        }
        rootNode.children.entrySet().parallelStream().map(entry -> calculateNodeGenoLen(entry.getValue()));
    }

    private double calculateNodeGenoLen(CladeNode node) {
        if (node.children.size() < 1) {
            return node.cladeGenomeLen;
        }
        node.cladeGenomeLen = node.children.entrySet().parallelStream()
                .mapToDouble(entry -> calculateNodeGenoLen(entry.getValue()))
                .average().getAsDouble();
        return node.cladeGenomeLen;
    }

    private String getNextTaxaLevel(String cladeName){
        if (cladeName.startsWith("g")) {
            return "s";
        } else if (cladeName.startsWith("f")) {
            return "g";
        } else if (cladeName.startsWith("o")) {
            return "f";
        } else if (cladeName.startsWith("c")) {
            return "o";
        } else if (cladeName.startsWith("p")) {
            return "c";
        } else if (cladeName.startsWith("k")) {
            return "p";
        } else {
            return "k";
        }
    }

    // core algorithm of MetaPhlAn2
    private double computeNodeAbundance(String clade, HashMap<String, HashMap<String, Tuple2<Integer, Double>>> cladeMarkers2nreads, HashMap<String, CladeNode> allClades)
            throws NullPointerException {
        CladeNode node = allClades.get(clade);
        if (node == null) {
            throw new NullPointerException("Clade " + clade + " has no CladeNode record.");
        } else {
            if (node.abundance != null) {
                return node.abundance;
            }
        }

        double sumAbun = 0.0;
        int markerLenSum = -1;
        double nreadsSum = 0.0;
        double locAbun = 0.0;
        int nonZeroCount = 0;

        try {
            sumAbun = node.children.keySet().parallelStream().mapToDouble(childClade -> this.computeNodeAbundance(childClade, cladeMarkers2nreads, allClades)).sum();
            //for (String childClade : node.children.keySet()) {
            //    sumAbun += this.computeNodeAbundance(childClade, cladeMarkers2nreads, allClades);
            //    //sumAbun += allClades.get(childClade).abundance;
            //}
        } catch (NullPointerException e) {
            LOG.error("[SOAPMetas::" + MEPHComputeAbundanceFunction.class.getName() + "] Current clade: "
                    + clade + " has null child or child has no abundance record OR " + e.toString());
        }

        ArrayList<Tuple3<Integer, Double, Integer>> discardLenCountList = new ArrayList<>(3); // marker_len, nreads, rawnreads
        ArrayList<Tuple3<Double, Integer, Double>> retainLenCountList = new ArrayList<>(10); // nreads/marker_len, marker_len, nreads

        boolean discard;
        String markerName = null;
        CladeNode extClade = null;
        int markerLen = 0;
        double nread = 0.0;
        int rawnread = 0;
        for(HashMap.Entry<String, Tuple2<Integer, Double>> entry: cladeMarkers2nreads.get(clade).entrySet()){
            markerName = entry.getKey();
            rawnread = entry.getValue()._1;
            nread = entry.getValue()._2;
            markerLen = markersInformationBroad.value().get(markerName)._2();
            discard = false;

            if (doDisqm) {
                for (String extCladeName : markersInformationBroad.value().get(markerName)._3()) {
                    extClade = allClades.getOrDefault(extCladeName, null);
                    if (extClade == null) break;
                    extClade = extClade.fatherClade;
                    while (extClade.children.size() == 1) {
                        extClade = extClade.children.values().iterator().next();
                    }
                    int mc = extClade.markerCount;
                    if ((mc > 0) && (extClade.nonZeroMarkerCount / (double) mc > 0.33)) {
                        discard = true;
                        discardLenCountList.add(new Tuple3<>(markerLen, nread, rawnread));
                        break;
                    }
                }
            }

            if (discard) continue;
            retainLenCountList.add(new Tuple3<>(nread/markerLen, markerLen, nread));
            node.recaliNreads += nread;
            node.rawnreads += rawnread;
            if (nread > 0){
                nonZeroCount++;
            }
            markerLenSum += markerLen;
            nreadsSum += entry.getValue()._2;
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
                for (int i=0; i<upBound; i++){
                    Tuple3<Integer, Double, Integer> item = discardLenCountList.get(i);
                    retainLenCountList.add(new Tuple3<>(item._2()/item._1(), item._1(), item._2()));
                    node.recaliNreads += item._2();
                    node.rawnreads += item._3();
                    markerLenSum += item._1();
                    nreadsSum += item._2();
                    if (item._2() > 0){
                        nonZeroCount++;
                    }
                }
            }
        }
        nRetain = retainLenCountList.size();

        int lBoundIndex = (int) (this.quantile * nRetain);
        int rBoundIndex = nRetain - lBoundIndex - 1;

        CladeNode fatherClade = node.fatherClade;
        if (clade.startsWith("t") && fatherClade.children.size() > 1 || fatherClade.spInClade || (node.kingdom.equals("k__Viruses"))){
            if (nRetain == 0 || nonZeroCount/(double) nRetain < 0.7) {
                node.abundance = 0.0;
                return 0.0;
            }
        }

        if (markerLenSum >= 0) {
            if ((statType & 2) > 0 || !(lBoundIndex > 0) && (statType & 40) > 0) {
                // avg_g
                if (markerLenSum > 0) {
                    locAbun = nreadsSum / markerLenSum;
                } else {
                    locAbun = 0.0;
                }
            } else if ((statType & 4) > 0 || !(lBoundIndex > 0) && (statType & 80) > 0) {
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
                node.abundance = sumAbun;
            } else if (locAbun < sumAbun) {
                node.abundance = sumAbun;
            }
            // TODO: 这里的计算可能有一点小问题，如果 存在 只属于当前 node 而不属于其子节点的 marker，
            //  而且当前 node 的 locAbun 比它子节点的 sumAbun 要小，那么这一部分结果就直接被忽略掉了。
            //  但如果 locAbun 比 sumAbun 要大，这里就直接用 locAbun 和 sumAbun 的差值作为 unclAbun，
            //  事实上按照计算逻辑， locAbun 本身就应该是 unclAbun （没有 subclade 归属的部分）

            if (node.abundance > sumAbun) {
                node.uncl_abundance = node.abundance - sumAbun;
            }
        } else {
            // not self.children
            if (!clade.startsWith("s") && !clade.startsWith("t")) {
                node.subcl_uncl = true;
            }
        }

        return node.abundance;
    }

    private boolean isSingleTerminal(CladeNode cladeNode) {
        HashMap<String, CladeNode> childrenMap = cladeNode.children;
        if (childrenMap == null){
            return true;
        }
        Collection<MEPHComputeAbundanceFunction.CladeNode> children = childrenMap.values();
        if (children.size() > 1){
            return false;
        }
        return isSingleTerminal(children.iterator().next());
    }

    private class CladeNode {
        double cladeGenomeLen = 0.0; // Average genome length of sub-taxas of the clade.
        String kingdom = "UNKNOWN"; // k__Archaea; k__Bacteria; k__Eukaryota;  k__Viruses
        boolean spInClade = false;
        Double abundance = null;
        double uncl_abundance = 0.0;
        boolean subcl_uncl = false;
        int markerCount = 0; // markers2nreads items count
        int nonZeroMarkerCount = 0; // count of markers2nreads items of which the "nreads" > 0
        int rawnreads = 0;
        double recaliNreads;
        CladeNode fatherClade = null;
        HashMap<String, CladeNode> children = new HashMap<>(5); // key: clade name, value: CladeNode
    }

    private ProfilingResultRecord profilingResultGenerator(
            String clusterName, String smTag, int rawnread, double recalinread, double abundance, double relAbun) {

        ProfilingResultRecord resultRecord;

        //if (this.profilingAnalysisMode.equals(ProfilingAnalysisMode.EVALUATION)) {
        //    resultRecord = new ProfilingEveResultRecord();
        //} else {
        //
        //}
        resultRecord = new ProfilingResultRecord();

        resultRecord.setClusterName(clusterName);

        resultRecord.setSmTag(smTag);

        resultRecord.setRawReadCount(rawnread);
        resultRecord.setrecaliReadCount(recalinread);
        resultRecord.setAbundance(abundance);
        resultRecord.setRelAbun(relAbun);

        return resultRecord;
    }
}
