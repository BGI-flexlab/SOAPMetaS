package org.bgi.flexlab.metas.profiling.profilingmethod;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
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

    private final Broadcast<HashMap<String, ArrayList<String>>> markers2extsBroad;
    private final Broadcast<HashMap<String, Integer>> markers2lenBroad;
    private boolean doDisqm = true;
    private double quantile = 0.1;
    private int minNucLen = 2000;

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

    public MEPHComputeAbundanceFunction(final Broadcast<HashMap<String, ArrayList<String>>> markers2exts, final Broadcast<HashMap<String, Integer>> markers2len){
        this.markers2extsBroad = markers2exts;
        this.markers2lenBroad = markers2len;

        // markers2clades/exts/lens: 1035649
        // taxa2clades: 16904
        // all_clades: 27085
    }

    @Override
    public Iterator<Tuple2<String, ProfilingResultRecord>> call(Iterator<Tuple2<String, HashMap<String, Tuple2<Integer, Double>>>> tuple2Iterator) throws Exception {

        HashMap<String, HashMap<String, Tuple2<Integer, Double>>> cladeMarkers2nreads;
        HashMap<String, CladeNode> all_clades = new HashMap<>(27086);

        ArrayList<String> kingdomList = new ArrayList<>(5);

        try {
            for (String kingdom : kingdomList) {
                this.computeNodeAbundance(kingdom, cladeMarkers2nreads, all_clades);
            }
        } catch (NullPointerException e){
            LOG.error("[SOAPMetas::" + MEPHComputeAbundanceFunction.class.getName() + "] " + e.toString());
        }

        ArrayList<Tuple2<String, ProfilingResultRecord>> results = new ArrayList<>(28000);

        // 获取各个 node 的 abundace，此处包括获取 unclassified 的信息。 生成的 ProfilingResult 也要包含 genoLen 信息？
        // 注意对 get_full_name 的需求，可能要考虑存储各个 node 的全名称。 此外，此过程需要考虑是否保存某个分类层级，因为后续
        // 进行 relative abundance 计算de时候，如果每个层级都计算，结果可能会出错

        return null;
    }

    // core algorithm of MetaPhlAn2
    private double computeNodeAbundance(String clade, HashMap<String, HashMap<String, Tuple2<Integer, Double>>> cladeMarkers2nreads, HashMap<String, CladeNode> all_clades)
            throws NullPointerException {
        CladeNode node = all_clades.get(clade);
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
            for (String childClade : node.children.keySet()) {
                sumAbun += this.computeNodeAbundance(childClade, cladeMarkers2nreads, all_clades);
                //sumAbun += all_clades.get(childClade).abundance;
            }
        } catch (NullPointerException e) {
            LOG.error("[SOAPMetas::" + MEPHComputeAbundanceFunction.class.getName() + "] Current clade: "
                    + clade + " has null child or child has no abundance record OR " + e.toString());
        }

        ArrayList<Tuple2<Integer, Double>> discardLenCountList = new ArrayList<>(3);
        ArrayList<Tuple3<Double, Integer, Double>> retainLenCountList = new ArrayList<>(10); // nreads/marker_len, marker_len, nreads

        boolean discard;
        String markerName = null;
        CladeNode extClade = null;
        int markerLen = 0;
        double nread = 0.0;
        for(HashMap.Entry<String, Tuple2<Integer, Double>> entry: cladeMarkers2nreads.get(clade).entrySet()){
            markerName = entry.getKey();
            nread = entry.getValue()._2;
            markerLen = markers2lenBroad.value().get(markerName);
            discard = false;

            if (doDisqm) {
                for (String extCladeName : markers2extsBroad.value().get(markerName)) {
                    extClade = all_clades.getOrDefault(extCladeName, null);
                    if (extClade == null) break;
                    extClade = extClade.fatherClade;
                    while (extClade.children.size() == 1) {
                        extClade = extClade.children.values().iterator().next();
                    }
                    int mc = extClade.markerCount;
                    if ((mc > 0) && (extClade.nonZeroMarkerCount / (double) mc > 0.33)) {
                        discard = true;
                        discardLenCountList.add(new Tuple2<>(markerLen, nread));
                        break;
                    }
                }
            }

            if (discard) continue;
            retainLenCountList.add(new Tuple3<>(nread/markerLen, markerLen, nread));
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
            if (terminalCountCheck(node)) {
                nRipr = 0;
            }
            if ((node.kingdom & 32) > 0){
                nRipr = 0;
            }
            if (nRetain < nRipr) {
                int upBound = Math.min(nRipr - nRetain, nDiscard);
                for (int i=0; i<upBound; i++){
                    Tuple2<Integer, Double> item = discardLenCountList.get(i);
                    retainLenCountList.add(new Tuple3<>(item._2/item._1, item._1, item._2));
                    markerLenSum += item._1;
                    nreadsSum += item._2;
                    if (item._2 > 0){
                        nonZeroCount++;
                    }
                }
            }
        }
        nRetain = retainLenCountList.size();

        int lBoundIndex = (int) (this.quantile * nRetain);
        int rBoundIndex = nRetain - lBoundIndex - 1;

        CladeNode fatherClade = node.fatherClade;
        if (clade.startsWith("t") && fatherClade.children.size() > 1 || (fatherClade.kingdom & 33)>0){
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

    private boolean terminalCountCheck(CladeNode cladeNode) {
        boolean bol = true;
        HashMap<String, CladeNode> childrenMap = cladeNode.children;
        if (childrenMap == null){
            return bol;
        }
        Collection<MEPHComputeAbundanceFunction.CladeNode> children = childrenMap.values();
        if (children.size() > 1){
            return false;
        }
        for (CladeNode childNode: children){
            bol = bol && terminalCountCheck(childNode);
        }
        return bol;
    }

    private class CladeNode {
        int cladeGenomeLen = 0; // Average genome length of sub-taxas of the clade.
        int kingdom = 0; // 0: not define 1: _sp in name 2: un_classified 4: k__Archaea; 8: k__Bacteria; 16: k__Eukaryota; 32: k__Viruses
        Double abundance = null;
        double uncl_abundance = 0.0;
        boolean subcl_uncl = false;
        int markerCount = 0; // markers2nreads items count
        int nonZeroMarkerCount = 0; // count of markers2nreads items of which the "nreads" > 0
        int genoLen = 0;
        CladeNode fatherClade = null;
        HashMap<String, CladeNode> children = null; // key: clade name, value: CladeNode
    }
}
