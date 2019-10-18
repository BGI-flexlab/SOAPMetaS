package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * ClassName: RelativeAbundanceFunction
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class RelativeAbundanceFunction implements Serializable, PairFlatMapFunction<Iterator<Tuple2<String, ProfilingResultRecord>>, String, ProfilingResultRecord> {

    private static final Logger LOG = LogManager.getLogger(RelativeAbundanceFunction.class);

    public static final long serialVersionUID = 1L;

    private boolean skip = false;

    public RelativeAbundanceFunction(boolean skip) {
        this.skip = skip;
    }

    @Override
    public Iterator<Tuple2<String, ProfilingResultRecord>> call(Iterator<Tuple2<String, ProfilingResultRecord>> tuple2Iterator) throws Exception {

        if (this.skip) {
            LOG.info("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] Skip relative abundance computing. (metaphlan pipeline)");
            return tuple2Iterator;
        }
        ArrayList<Tuple2<String, ProfilingResultRecord>> relativeProfilingList;
        Tuple2<String, ProfilingResultRecord> tup0;
        String smTag;
        String sampleID;
        double totalAbun = 0;

        if (tuple2Iterator.hasNext()) {
            tup0 = tuple2Iterator.next();
            smTag = tup0._2.getSmTag();
            totalAbun += tup0._2.getAbundance();
            //LOG.info("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] Abundance of cluster " + tup0._2.getClusterName() + " is " + tup0._2.getAbundance());
            sampleID = tup0._1;
            LOG.info("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] Relative abundance calculation for sampleID: " + sampleID + " sample TAG: " + smTag);

            relativeProfilingList = new ArrayList<>(128);
            relativeProfilingList.add(tup0);
        } else {
            LOG.info("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] Skip null partition.");
            return new ArrayList<Tuple2<String, ProfilingResultRecord>>(0).iterator();
        }

        while (tuple2Iterator.hasNext()){
            Tuple2<String, ProfilingResultRecord> tup = tuple2Iterator.next();

            //if (!tup._1.equals(sampleID)){
            //    LOG.warn("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] Partition error. ProfilingResultRecord of " + tup._2.getClusterName() +
            //            " in sample " + tup._1 + " is partitioned wrongly to sample " + sampleID);
            //    continue;
            //}

            totalAbun += tup._2.getAbundance();
            //LOG.info("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] Abundance of cluster " + tup0._2.getClusterName() + " is " + tup0._2.getAbundance());

            relativeProfilingList.add(tup);
        }

        LOG.info("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] Total abundance of sample " + smTag + " is " + totalAbun);

        for(Tuple2<String, ProfilingResultRecord> tup: relativeProfilingList){
            tup._2.setRelAbun(tup._2.getAbundance()/totalAbun * 100);
        }

        LOG.info("[SOAPMetas::" + RelativeAbundanceFunction.class.getName() + "] SampleTAG: " + smTag + " . SampleID recheck: " + relativeProfilingList.get(relativeProfilingList.size()-1)._1);
        return relativeProfilingList.iterator();
    }
}
