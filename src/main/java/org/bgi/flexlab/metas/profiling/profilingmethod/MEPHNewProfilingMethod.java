package org.bgi.flexlab.metas.profiling.profilingmethod;

import com.google.gson.stream.JsonReader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.SequenceUtil;
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
import org.bgi.flexlab.metas.profiling.filter.MetasSAMRecordIdentityFilter;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelBase;
import org.bgi.flexlab.metas.profiling.recalibration.gcbias.GCBiasModelFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: MEPHNewProfilingMethod
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MEPHNewProfilingMethod extends ProfilingMethodBase implements Serializable {

    public static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(MEPHNewProfilingMethod.class);

    private boolean doGCRecalibration;
    private GCBiasModelBase gcBiasRecaliModel;

    //private Broadcast<HashMap<String, ArrayList<String>>> markers2extsBroad = null;
    //private Broadcast<HashMap<String, Integer>> markers2lenBroad = null;
    //private Broadcast<HashMap<String, String>> markers2cladeBroad = null;
    private Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad = null; // marker: cladename, len, extsList
    private Broadcast<ArrayList<Tuple3<ArrayList<String>, Integer, ArrayList<String>>>> taxonomyInformationBroad = null;
    private Broadcast<HashMap<String, Tuple2<String, String>>> cladeName2HighRankBroad = null; // "s__Species_name: k__xxx|p__xxx|c__xxx|o_xxx|f__xxx|g__xxx|"
    private Broadcast<HashMap<String, Integer>> sampleNamesBroadcast = null;

    private HashMap<String, Double> geneGCMap;
    private HashSet<String> excludeMarkers;

    //private String metaphlanMpaDBFile;
    private String mpaMarkersListFile; // MetaPhlAn2 markers list file extracted from mpa_v20_m200.pkl "marker"
    private String mpaTaxonomyListFile; // MetaPhlAn2 taxonomy list file extracted from mpa_v20_m200.pkl "taxonomy"

    private boolean doIdentityFiltering = false;
    private boolean doAlignLenFiltering = false;

    private MetasSAMRecordIdentityFilter identityFilter;
    //private MetasSAMRecordAlignLenFilter alignLenFilter;
    private int minAlignLen = 0;
    private Pattern cigarPattern;

    private int minMapQuality = -100;
    private String outFormat;

    private MetasOptions options;

    public MEPHNewProfilingMethod(MetasOptions options, JavaSparkContext jsc){
        super(options, jsc);

        this.options = options;

        this.mpaMarkersListFile = options.getMpaMarkersListFile();
        this.mpaTaxonomyListFile = options.getMpaTaxonomyListFile();

        this.doIdentityFiltering = options.isDoIdentityFiltering();
        this.doAlignLenFiltering = options.isDoAlignLenFiltering();

        if (doIdentityFiltering) {
            this.identityFilter = new MetasSAMRecordIdentityFilter(options.getMinIdentity());
        }
        if (doAlignLenFiltering) {
            //this.alignLenFilter = new MetasSAMRecordAlignLenFilter(options.getMinAlignLength());
            this.cigarPattern = Pattern.compile("(\\d+)(\\D+)");
            this.minAlignLen = options.getMinAlignLength();
        }

        this.excludeMarkers = new HashSet<>();

        String excludeMarkerFile = options.getMpaExcludeMarkersFile();
        if (excludeMarkerFile != null) {
            try (BufferedReader br = new BufferedReader(new FileReader(excludeMarkerFile))) {
                String currentline;
                while ((currentline = br.readLine()) != null) {
                    excludeMarkers.add(currentline);
                }
            } catch (FileNotFoundException e) {
                LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] Markers-to-exclude list file not found. " + e.toString());
            } catch (IOException e) {
                LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] Markers-to-exclude file IO error. " + e.toString());
            }
        }

        this.geneGCMap = new HashMap<>(1036030);
        try (BufferedReader br = new BufferedReader(new FileReader(options.getReferenceMatrixFilePath()))) {
            String currentLine;
            while((currentLine = br.readLine()) != null) {
                String[] ele = currentLine.split("\t");
                this.geneGCMap.put(ele[1], Double.parseDouble(ele[4]));
            }
        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] marker matrix file for metaphlan mode is not found. " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] metaphlan mode marker matrix file IO error. " + e.toString());
        }

        this.minMapQuality = options.getMinMapQuallity();
        this.outFormat = options.getOutputFormat();

        this.doGCRecalibration = options.isDoGcBiasRecalibration();
        if (this.doGCRecalibration) {
            LOG.debug("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] Do GC recalibration.");
            this.gcBiasRecaliModel = new GCBiasModelFactory(options.getGcBiasRecaliModelType(),
                    options.getGcBiasModelInput()).getGCBiasRecaliModel();
            //this.gcBiasRecaliModel.outputCoefficients(options.getProfilingOutputHdfsDir() + "/builtin_model.json");
        } else {
            LOG.debug("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] Skip GC recalibration.");
        }
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

    @Override
    public JavaPairRDD<String, ProfilingResultRecord> runProfiling(JavaRDD<SAMRecord> samRecordJavaRDD, JavaSparkContext ctx) {

        SampleIDPartitioner sampleIDPartitioner = new SampleIDPartitioner(this.sampleIDbySampleName.size() + 1);

        if (this.sampleNamesBroadcast == null) {
            this.sampleNamesBroadcast = ctx.broadcast(this.sampleIDbySampleName);
        }

        if (this.markersInformationBroad == null) {
            //TODO: 这一部分多 partition 之间的同步判定需要进一步考虑

            // MetaPhlAn2 2018 database:
            // markers2clades/exts/lens: 1035649
            // taxa2clades: 16904
            // allClades: 27085
            HashMap<String, Tuple3<String, Integer, ArrayList<String>>> markersInformation = new HashMap<>(1035700); // marker: cladename, len, extsList
            ArrayList<Tuple3<ArrayList<String>, Integer, ArrayList<String>>> taxonomyInformation = new ArrayList<>(17000); // ([k__xxx, p__xxx, c__xxx, ..., s__xxx, t__xxx], genoLength)
            HashMap<String, Tuple2<String, String>> cladeName2HighRank = new HashMap<>(130000); // "s__Species_name: k__xxx|p__xxx|c__xxx|o_xxx|f__xxx|g__xxx|"

            this.readMarkerFile(this.mpaMarkersListFile, markersInformation);
            this.readTaxonomyFile(this.mpaTaxonomyListFile, taxonomyInformation, cladeName2HighRank);

            //Broadcast<HashMap<String, Tuple3<String, Integer, ArrayList<String>>>> markersInformationBroad = ctx.broadcast(markersInformation);
            //Broadcast<ArrayList<Tuple2<ArrayList<String>, Integer>>> taxonomyInformationBroad = ctx.broadcast(taxonomyInformation);
            //Broadcast<HashMap<String, String>> cladeName2HighRankBroad = ctx.broadcast(cladeName2HighRank);
            this.markersInformationBroad = ctx.broadcast(markersInformation);
            this.taxonomyInformationBroad = ctx.broadcast(taxonomyInformation);
            this.cladeName2HighRankBroad = ctx.broadcast(cladeName2HighRank);
        }


        /*
        Input:
         JavaRDD<SAMRecord>

        After mapToPair: (read2marker)
         key: sampleID"\t"rgID"\t"cladeName"\t"markerName
         value: tuple<1, gc_recali_value>
         Partition: default

        After reduceByKey: (marker2)
         key: sampleID"\t"rgID"\t"cladeName"\t"markerName
         value: tuple<count, gc_recali_count>
         Partition: sampleID


        Output:
         Type: JavaPairRDD
         key: sampleID
         value: ProfilingResultRecord (clusterName includes taxonomy information)
        */
        return samRecordJavaRDD.mapToPair(samRecord -> countTupleGenerator(samRecord.getStringAttribute("RG"), samRecord))
                .filter(tuple -> tuple._1 != null)
                .reduceByKey(sampleIDPartitioner, ((v1, v2) -> new Tuple2<>(v1._1+v2._1, v1._2+v2._2)))
                .mapPartitionsToPair(new MEPHNewAbundanceFunction(this.markersInformationBroad, this.taxonomyInformationBroad, this.cladeName2HighRankBroad, this.options), true);
    }

    private Tuple2<String, Tuple2<Integer, Double>> countTupleGenerator(String rgID, SAMRecord record) {

        String markerName = record.getReferenceName();
        if (this.excludeMarkers.contains(markerName)) {
            LOG.info("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] Exclude special marker " + markerName);
            return new Tuple2<>(null, null);
        }

        // TODO: MetaPhlAn 在对多重比对以及可能的 PE 比对 reads 处理时, 由于使用了 set() 来保存 read name, 因此隐含了去除重复元素的步骤
        if (record.isSecondaryAlignment() || record.getMappingQuality() <= this.minMapQuality ) { //TODO: 添加质量分数的 option
            return new Tuple2<>(null, null);
        }

        if (doIdentityFiltering && this.identityFilter.filter(record)){
            return new Tuple2<>(null, null);
        }

        if (doAlignLenFiltering) { // && this.alignLenFilter.filter(record)) {
            int subAlignLen = 0;
            Matcher cigarMatcher = this.cigarPattern.matcher(record.getCigar().toString());
            while(cigarMatcher.find()){
                if (cigarMatcher.group(2).toUpperCase().equals("M")){
                    subAlignLen = Math.max(subAlignLen, Integer.parseInt(cigarMatcher.group(1)));
                }
            }
            if (subAlignLen < this.minAlignLen) {
                return new Tuple2<>(null, null);
            }
        }

        Double recaliReadCount = 1.0;

        if (this.doGCRecalibration) {
            recaliReadCount = this.gcBiasRecaliModel.recalibrateForSingle(
                    SequenceUtil.calculateGc(record.getReadBases()),
                    this.geneGCMap.get(record.getReferenceName())
            );
        }

        Tuple2<Integer, Double> readCount = new Tuple2<>(1, recaliReadCount);

        return new Tuple2<>(
                this.sampleIDbySampleName.get(rgID) + "\t" + rgID + "\t" + this.markersInformationBroad.value().get(markerName)._1() + "\t" + markerName,
                readCount
        );
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
                if (this.excludeMarkers.contains(markerName)) {
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
                            jsonReader.nextDouble();
                            break;
                        }
                        case "taxon":
                        case "oname": {
                            jsonReader.nextString();
                            break;
                        }
                    }
                }
                jsonReader.endObject();
                if (markerLen < 0 || cladeName == null) {
                    LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] Gene " + markerName + "doesn't has information.");
                    continue;
                }
                extArray.trimToSize();
                markersInformation.put(markerName, new Tuple3<>(cladeName, markerLen, extArray));
                //System.out.println("Name: " + geneName + "\next: " + extArray.toString() + "\nClade: " + cladeName + "\nLength: " + Integer.toString(geneLen));
            }

            jsonReader.endObject();
        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] mpa markers information file not found. " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] mpa markers information reader error. " + e.toString());
        }
    }

    private void readTaxonomyFile(String mpaTaxonomyListFile,
                                  ArrayList<Tuple3<ArrayList<String>, Integer, ArrayList<String>>> taxonomyInformation,
                                  HashMap<String, Tuple2<String, String>> cladeName2HighRank) {
        // k__Bacteria|p__Proteobacteria|c__Gammaproteobacteria|o__Enterobacteriales|f__Enterobacteriaceae|g__Shigella|s__Shigella_flexneri|t__GCF_000213455       ("1|22|333|4444|55555", 4656186)
        // target txonomy info:
        // target high rank:

        try (JsonReader jsonReader = new JsonReader(new FileReader(mpaTaxonomyListFile))) {
            String tempItem;
            int genoLen = 0;
            int count;

            if (this.outFormat.equals("CAMI")) {
                jsonReader.beginObject();
                while (jsonReader.hasNext()) {

                    ArrayList<String> taxonList = new ArrayList<>(9);
                    Collections.addAll(taxonList, jsonReader.nextName().split("\\|"));
                    ArrayList<String> taxIDList = new ArrayList<>(9);

                    jsonReader.beginObject();
                    while (jsonReader.hasNext()) {
                        tempItem = jsonReader.nextName();
                        switch (tempItem) {
                            case "genoLen": {
                                genoLen = jsonReader.nextInt();
                                break;
                            }
                            case "taxid": {
                                Collections.addAll(taxIDList, jsonReader.nextString().split("\\|"));
                                break;
                            }
                        }
                    }
                    int appendC = 8 - taxIDList.size();
                    for (int i = 1; i<=appendC; i++) {
                        taxIDList.add("");
                    }
                    jsonReader.endObject();
                    taxonomyInformation.add(new Tuple3<>(taxonList, genoLen, taxIDList));

                    count = taxonList.size();
                    StringBuilder highNameRank = new StringBuilder();
                    StringBuilder highIDRank = new StringBuilder();
                    cladeName2HighRank.put(taxonList.get(0), new Tuple2<>("", ""));
                    highNameRank.append(taxonList.get(0)).append('|');
                    highIDRank.append(taxIDList.get(0)).append('|');
                    for (int i = 1; i < count; i++) {
                        cladeName2HighRank.put(taxonList.get(i), new Tuple2<>(highNameRank.toString().substring(3), highIDRank.toString()));
                        if (taxonList.get(i).contains("_unclassified")) {
                            highNameRank.append("").append('|');
                        } else {
                            highNameRank.append(taxonList.get(i)).append('|');
                        }
                        highIDRank.append(taxIDList.get(i)).append('|');
                    }
                }
            } else {
                jsonReader.beginObject();
                while (jsonReader.hasNext()) {

                    ArrayList<String> taxonList = new ArrayList<>(9);
                    Collections.addAll(taxonList, jsonReader.nextName().split("\\|"));
                    ArrayList<String> taxIDList = new ArrayList<>(9);

                    jsonReader.beginObject();
                    while (jsonReader.hasNext()) {
                        tempItem = jsonReader.nextName();
                        switch (tempItem) {
                            case "genoLen": {
                                genoLen = jsonReader.nextInt();
                                break;
                            }
                            case "taxid": {
                                Collections.addAll(taxIDList, jsonReader.nextString().split("\\|"));
                                break;
                            }
                        }
                    }
                    int appendC = 8 - taxIDList.size();
                    for (int i = 1; i<=appendC; i++) {
                        taxIDList.add("");
                    }
                    jsonReader.endObject();
                    taxonomyInformation.add(new Tuple3<>(taxonList, genoLen, taxIDList));

                    count = taxonList.size();
                    StringBuilder highNameRank = new StringBuilder();
                    StringBuilder highIDRank = new StringBuilder();
                    cladeName2HighRank.put(taxonList.get(0), new Tuple2<>("", ""));
                    highNameRank.append(taxonList.get(0)).append('|');
                    highIDRank.append(taxIDList.get(0)).append('|');
                    for (int i = 1; i < count; i++) {
                        cladeName2HighRank.put(taxonList.get(i), new Tuple2<>(highNameRank.toString(), highIDRank.toString()));
                        highNameRank.append(taxonList.get(i)).append('|');
                        highIDRank.append(taxIDList.get(i)).append('|');
                    }
                }
            }

            jsonReader.endObject();

            //while ((currentLine = taxonbr.readLine()) != null) {
            //    lineEle = currentLine.split("\\s+");
            //    taxonLev = lineEle[0].split("\\|");
            //    genoLen = Integer.parseInt(lineEle[1]);
            //    taxonomyInformation.add(new Tuple2<>(new ArrayList<>(Arrays.asList(taxonLev)), genoLen));
            //    count = taxonLev.length;
            //    StringBuilder highRank = new StringBuilder();
            //    for (int i = 0; i < count; i++) {
            //        cladeName2HighRank.put(taxonLev[i], highRank.toString());
            //        highRank.append(taxonLev[i]).append('|');
            //    }
            //}
        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] mpa taxonomy list file not found. " + e.toString());
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + MEPHNewProfilingMethod.class.getName() + "] mpa taxonomy list file reader error. " + e.toString());
        }
    }
}
