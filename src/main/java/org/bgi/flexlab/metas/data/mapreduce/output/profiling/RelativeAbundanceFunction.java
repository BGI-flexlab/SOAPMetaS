package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

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

    public static final long serialVersionUID = 1L;

    @Override
    public Iterator<Tuple2<String, ProfilingResultRecord>> call(Iterator<Tuple2<String, ProfilingResultRecord>> tuple2Iterator) throws Exception {
        ArrayList<Tuple2<String, ProfilingResultRecord>> relativeProfilingList = new ArrayList<>(128);

        double totalAbun = 0;

        while (tuple2Iterator.hasNext()){
            Tuple2<String, ProfilingResultRecord> tup = tuple2Iterator.next();
            totalAbun += tup._2.getAbundance();

            relativeProfilingList.add(tup);
        }

        for(Tuple2<String, ProfilingResultRecord> tup: relativeProfilingList){
            tup._2.setRelAbun(tup._2.getAbundance()/totalAbun);
        }
        return relativeProfilingList.iterator();
    }
}
