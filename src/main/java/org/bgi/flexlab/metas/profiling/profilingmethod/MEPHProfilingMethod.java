package org.bgi.flexlab.metas.profiling.profilingmethod;

import com.google.gson.stream.JsonReader;
import htsjdk.samtools.SAMRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDPartitioner;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSAMPairRecord;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelBase;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.util.*;

/**
 * ClassName: MEPHProfilingMethod
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class MEPHProfilingMethod extends ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(MEPHProfilingMethod.class);

    private boolean doGCRecalibration;
    private GCBiasModelBase gcBiasRecaliModel;

    //private Broadcast<HashMap<String, ArrayList<String>>> markers2extsBroad = null;
    //private Broadcast<HashMap<String, Integer>> markers2lenBroad = null;
    //private Broadcast<HashMap<String, String>> markers2cladeBroad = null;
    private Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad = null; // marker: cladename, len, extsList

    private Broadcast<ArrayList<Tuple2<ArrayList<String>, Integer>>> taxonomyInformationBroad = null;
    private Broadcast<HashMap<String, String>> cladeName2HighRankBroad = null; // "s__Species_name: k__xxx|p__xxx|c__xxx|o_xxx|f__xxx|g__xxx|"

    private final MetasOptions options;
    //private String metaphlanMpaDBFile;
    private String mpaMarkersListFile; // MetaPhlAn2 markers list file extracted from mpa_v20_m200.pkl "marker"
    private String mpaTaxonomyListFile; // MetaPhlAn2 taxonomy list file extracted from mpa_v20_m200.pkl "taxonomy"

    public MEPHProfilingMethod(MetasOptions options, JavaSparkContext jsc){
        super(options, jsc);
        this.options = options;

        this.mpaMarkersListFile = this.options.getMpaMarkersListFile();
        this.mpaTaxonomyListFile = this.options.getMpaTaxonomyListFile();
    }

    /**
     * TODO: 需要完善基于METAPHLAN策略的丰度计算方法。
     *
     * @param readMetasSamPairRDD
     * @return
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaPairRDD<String, MetasSAMPairRecord> readMetasSamPairRDD, Partitioner partitioner){
        return null;
    }

    /*
     Input:

     After mapToPair:
      key: sampleID"\t"rgID"\t"cladeName
      value: HashMap<markerName, tuple<1, gc_recali_value>>
      Partition: default

     After reduceByKey:
      key: sampleID"\t"rgID"\t"cladeName
      value: HashMap<markerName, tuple<count, gc_recali_count>>
      Partition: sampleID


     Output:
      Type: JavaPairRDD
      key: sampleID
      value: ProfilingResultRecord (clusterName includes taxonomy information)
     */
    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaRDD<SAMRecord> samRecordJavaRDD, JavaSparkContext ctx) {

        SampleIDPartitioner sampleIDPartitioner = new SampleIDPartitioner(this.sampleIDbySampleName.size() + 1);

        这个是干啥用的? Broadcast<HashMap<String, Integer>>  sampleNamesBroadcast = ctx.broadcast(this.sampleIDbySampleName);

        HashMap<String, Tuple3<String, Integer, ArrayList<String>>> markersInformation = new HashMap<>(1035700); // marker: cladename, len, extsList
        ArrayList<Tuple2<ArrayList<String>, Integer>> taxonomyInformation = new ArrayList<>(17000); // ([k__xxx, p__xxx, c__xxx, ..., s__xxx, t__xxx], genoLength)
        HashMap<String, String> cladeName2HighRank = new HashMap<>(130000); // "s__Species_name: k__xxx|p__xxx|c__xxx|o_xxx|f__xxx|g__xxx|"

        if (广播变量需要判断是否已经广播)
        this.readMarkerFile(this.mpaMarkersListFile, markersInformation);
        this.readTaxonomyFile(this.mpaTaxonomyListFile, taxonomyInformation, cladeName2HighRank);

        //Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad = ctx.broadcast(markersInformation);
        //Broadcast<ArrayList<Tuple2<ArrayList<String>, Integer>>> taxonomyInformationBroad = ctx.broadcast(taxonomyInformation);
        //Broadcast<HashMap<String, String>> cladeName2HighRankBroad = ctx.broadcast(cladeName2HighRank);
        this.markersInformationBroad = ctx.broadcast(markersInformation);
        this.taxonomyInformationBroad = ctx.broadcast(taxonomyInformation);
        this.cladeName2HighRankBroad = ctx.broadcast(cladeName2HighRank);



        return samRecordJavaRDD.mapToPair(samRecord -> countTupleGenerator(samRecord.getStringAttribute("RG"), samRecord))
                .reduceByKey(sampleIDPartitioner, (a, b) -> {
                    HashMap<String, Tuple2<Integer, Double>> c;
                    if (a.size() < b.size()) {
                        c = new HashMap<>(b);
                        a.forEach((k, v) -> c.merge(k, v, (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
                    } else {
                        c = new HashMap<>(a);
                        b.forEach((k, v) -> c.merge(k, v,  (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)));
                    }
                    return c;
                }).mapPartitionsToPair(new MEPHComputeAbundanceFunction(this.markersInformationBroad, this.taxonomyInformationBroad, this.cladeName2HighRankBroad, this.options), true);
    }

    private Tuple2<String, HashMap<String, Tuple2<Integer, Double>>> countTupleGenerator(String rgID, SAMRecord record) {

        String markerName = record.getReferenceName();
        Double recaliReadCount = 1.0;

        //LOG.info("[SOAPMetas::" + COMGProfilingMethod.class.getName() + "] Count Single Record: " + record.toString() + " || Reference Gene name: " + geneName);

        if (false){
            // TODO: SAM filter
            return new Tuple2<>(null, null);
        }

        if (this.doGCRecalibration) {
            //recaliReadCount = this.gcBiasRecaliModel.recalibrateForSingle(
            //        SequenceUtil.calculateGc(record.getReadBases()),
            //        //this.referenceInfoMatrix.value().getSpeciesGenoGC(this.referenceInfoMatrix.value().getGeneSpeciesName(geneName))
            //);
        }

        HashMap<String, Tuple2<Integer, Double>> readCount = new HashMap<>(2);
        readCount.put(markerName, new Tuple2<>(1, recaliReadCount));
        return new Tuple2<>(this.sampleIDbySampleName.get(rgID) + "\t" + rgID + "\t" + this.markersInformationBroad.getValue().get(markerName)._1(), readCount);
    }

    @Override
    public void setSampleIDbySampleName(HashMap<String, Integer> sampleIDbySampleName) {
        this.sampleIDbySampleName = sampleIDbySampleName;
    }

    private void readMarkerFile(String mpaMarkersListFile, HashMap<String, Tuple3<String, Integer, ArrayList<String>>> markersInformation) {
        // gi|483970126|ref|NZ_KB891629.1|:c6456-5752      {'ext': {'GCF_000373585', 'GCF_000355695', 'GCF_000226995'}, 'score': 3.0, 'clade': 's__Streptomyces_sp_KhCrAH_244', 'len': 705, 'taxon': 'k__Bacteria|p__Actinobacteria|c__Actinobacteria|o__Actinomycetales|f__Streptomycetaceae|g__Streptomyces|s__Streptomyces_sp_KhCrAH_244'}
        // target structure: {marker: (cladename, len, [extsList])}
        try (JsonReader jsonReader = new JsonReader(new FileReader(mpaMarkersListFile))) {

            String markerName;
            int markerLen = -1;
            String cladeName = null;
            String markerItem;
            ArrayList<String> extArray;

            jsonReader.beginObject();

            while (jsonReader.hasNext()) {

                markerName = jsonReader.nextName();
                //System.out.println("Gene: " + markerName);
                if (exclude marker) {
                    jsonReader.skipValue();
                    continue;
                }

                extArray = new ArrayList<>(5);

                jsonReader.beginObject();
                while (jsonReader.hasNext()) {
                    markerItem = jsonReader.nextName();
                    switch (markerItem) {
                        case "ext": {
                            //System.out.print(markerItem + ": ");
                            jsonReader.beginArray();
                            while (jsonReader.hasNext()) {
                                extArray.add("t__" + jsonReader.nextString());
                            }
                            jsonReader.endArray();
                            //System.out.println(extArray.toString());
                            break;
                        }
                        case "clade": {
                            //System.out.print(markerItem + ": ");
                            cladeName = jsonReader.nextString();
                            //System.out.println(cladeName);
                            break;
                        }
                        case "len": {
                            //System.out.print(markerItem + ": ");
                            markerLen = jsonReader.nextInt();
                            //System.out.println(Integer.toString(markerLen));
                            break;
                        }
                        case "score": {
                            jsonReader.nextInt();
                            break;
                        }
                        case "taxon": {
                            jsonReader.nextString();
                        }
                    }
                }
                jsonReader.endObject();
                if (markerLen < 0 || cladeName == null) {
                    LOG.error("[SOAPMetas::" + MEPHProfilingMethod.class.getName() + "] Gene " + markerName + "doesn't has information.");
                    continue;
                }
                extArray.trimToSize();
                markersInformation.put(markerName, new Tuple3<>(cladeName, markerLen, extArray));
                //System.out.println("Name: " + geneName + "\next: " + extArray.toString() + "\nClade: " + cladeName + "\nLength: " + Integer.toString(geneLen));
            }

            jsonReader.endObject();
        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + MEPHProfilingMethod.class.getName() + "] mpa markers information file not found. " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + MEPHProfilingMethod.class.getName() + "] mpa markers information reader error. " + e.toString());
        }
    }

    private void readTaxonomyFile(String mpaTaxonomyListFile, ArrayList<Tuple2<ArrayList<String>, Integer>> taxonomyInformation, HashMap<String, String> cladeName2HighRank) {
        // k__Bacteria|p__Proteobacteria|c__Gammaproteobacteria|o__Enterobacteriales|f__Enterobacteriaceae|g__Shigella|s__Shigella_flexneri|t__GCF_000213455       4656186
        // target txonomy info:
        // target high rank:

        try (BufferedReader taxonbr = new BufferedReader(new FileReader(mpaTaxonomyListFile))) {
            String currentLine;
            String[] taxonLev;
            int genoLen;
            String[] lineEle;
            int count;

            while ((currentLine = taxonbr.readLine()) != null) {
                lineEle = currentLine.split("\\s+");
                taxonLev = lineEle[0].split("\\|");
                genoLen = Integer.parseInt(lineEle[1]);
                taxonomyInformation.add(new Tuple2<>(new ArrayList<>(Arrays.asList(taxonLev)), genoLen));
                count = taxonLev.length;
                StringBuilder highRank = new StringBuilder();

                for (int i = 0; i < count; i++) {
                    cladeName2HighRank.put(taxonLev[i], highRank.toString());
                    highRank.append(taxonLev[i]);
                }
            }
        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + MEPHProfilingMethod.class.getName() + "] mpa taxonomy list file not found. " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + MEPHProfilingMethod.class.getName() + "] mpa taxonomy list file reader error. " + e.toString());
        }
    }

}
