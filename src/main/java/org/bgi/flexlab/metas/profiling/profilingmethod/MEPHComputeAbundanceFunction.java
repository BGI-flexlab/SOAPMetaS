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
    private final boolean doDisqm = true;
    private final double quantile = 0.1;

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

        return null;
    }

    private void computeNodeAbundance(String clade, HashMap<String, HashMap<String, Tuple2<Integer, Double>>> cladeMarkers2nreads, HashMap<String, CladeNode> all_clades)
            throws NullPointerException {
        CladeNode node = all_clades.get(clade);
        if (node == null) {
            throw new NullPointerException("Clade " + clade + " has no CladeNode record.");
        } else {
            if (node.abundance != null) {
                return;
            }
        }

        double sumAbun = 0.0;
        int markerLenSum = -1;
        double nreadsSum = 0.0;
        double locAbun = 0.0;
        int nonZeroCount = 0;

        try {
            for (String childClade : node.children.keySet()) {
                this.computeNodeAbundance(childClade, cladeMarkers2nreads, all_clades);
                sumAbun += all_clades.get(childClade).abundance;
            }
        } catch (NullPointerException e) {
            LOG.error("[SOAPMetas::" + MEPHComputeAbundanceFunction.class.getName() + "] Current clade: "
                    + clade + " has null child or child has no abundance record OR " + e.toString());
        }

        ArrayList<Tuple2<Integer, Double>> discardLenCountList = new ArrayList<>(3);
        ArrayList<Tuple3<Double, Integer, Double>> retainLenCountList = new ArrayList<>(10); // nreads/marker_len, marker_len, nreads
        ArrayList<Double> nreadsList = new ArrayList<>(10); // list of nreads;

        boolean discard;
        String markerName = null;
        CladeNode extClade = null;
        int markerLen = 0;
        double nread = 0.0;
        for(HashMap.Entry<String, Tuple2<Integer, Double>> entry: cladeMarkers2nreads.get(clade).entrySet()){
            markerName = entry.getKey();
            nread = entry.getValue()._2;
            markerLen = markers2lenBroad.value().get(markerName);

            if (doDisqm) {
                discard = false;
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

        int lBound = (int) (this.quantile * nRetain);
        int rBound = nRetain - lBound;

        CladeNode fatherClade = node.fatherClade;
        if (clade.startsWith("t") && fatherClade.children.size() > 1 || (fatherClade.kingdom & 33)>0){
            if (nRetain == 0 || nonZeroCount/(double) nRetain < 0.7) {
                node.abundance = 0.0;
                return;
            }
        }

        if (markerLenSum >= 0) {
            if ((statType & 2) > 0 || !(lBound > 0) && (statType & 40) > 0) {
                // avg_g
                if (markerLenSum > 0) {
                    locAbun = nreadsSum / markerLenSum;
                } else {
                    locAbun = 0.0;
                }
            } else if ((statType & 4) > 0 || !(lBound > 0) && (statType & 80) > 0) {
                double sumv = 0;
                for (int i = 0; i < nRetain; i++) {
                    sumv += retainLenCountList.get(i)._1();
                }
                locAbun = sumv / nRetain;
            } else if ((statType & 8) > 0) {
                retainLenCountList.sort(Comparator.comparing(Tuple3::_1));
                int sumMLen = 0;
                double sumNRead = 0.0;
                for (int i=lBound; i < rBound; i++) {
                    sumMLen += retainLenCountList.get(i)._2();
                    sumNRead += retainLenCountList.get(i)._3();
                }
                if (sumMLen > 0) {
                    locAbun = sumNRead / sumMLen;
                } else {
                    locAbun = 0.0;
                }
            } else if ((statType & 16) > 0) {

            }
        }

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
        CladeNode fatherClade = null;
        HashMap<String, CladeNode> children = null; // key: clade name, value: CladeNode
    }
}
