package org.bgi.flexlab.metas.profiling;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.data.mapreduce.input.sam.MetasSamInputFormat;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDReadNamePartitioner;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDPartitioner;
import org.bgi.flexlab.metas.data.mapreduce.output.profiling.ProfilingResultWriteFunction;
import org.bgi.flexlab.metas.data.structure.profiling.ProfilingResultRecord;
import org.bgi.flexlab.metas.data.structure.sam.MetasSamPairRecord;
import org.bgi.flexlab.metas.data.structure.sam.SAMMultiSampleList;
import org.bgi.flexlab.metas.profiling.filter.MetasSamRecordIdentityFilter;
import org.bgi.flexlab.metas.util.DataUtils;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import scala.Tuple2;


import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * ClassName: ProfilingProcessMS
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class ProfilingProcessMS {

    private static final Log LOG = LogFactory.getLog(ProfilingProcessMS.class); // The LOG

    private MetasOptions metasOpt;
    private JavaSparkContext jscontext;

    private String pipeline;
    private ProfilingAnalysisMode analysisMode;

    private SAMMultiSampleList samMultiSampleList;
    private int numPartitionEachSample;

    private boolean doIdentityFiltering = false;
    private MetasSamRecordIdentityFilter identityFilter;

    private String tmpDir;
    private String outputHdfsDir;

    public ProfilingProcessMS(final MetasOptions options, final JavaSparkContext context){
        this.metasOpt = options;
        this.jscontext = context;
        this.processConstruct();
    }

    /**
     * TODO: Add createOutputDirectory method if needed.
     */
    private void processConstruct(){

        this.numPartitionEachSample = Math.max(this.metasOpt.getNumPartitionEachSample(), 1);

        this.pipeline = this.metasOpt.getProfilingPipeline();
        this.analysisMode = this.metasOpt.getProfilingAnalysisMode();

        this.doIdentityFiltering = this.metasOpt.isDoIdentityFiltering();
        if (this.doIdentityFiltering) {
            this.identityFilter = new MetasSamRecordIdentityFilter(this.metasOpt.getMinIdentity());
        }
        Configuration conf = this.jscontext.hadoopConfiguration();

        this.tmpDir = this.metasOpt.getProfilingTmpDir();
        if (this.tmpDir == null) {
            this.tmpDir = DataUtils.getTmpDir(this.jscontext);
        } else {
            try {
                DataUtils.createFolder(conf, this.tmpDir);
            } catch (IOException e){
                LOG.error("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] Fail to create profiling temp directory.");
                e.printStackTrace();
            }
        }

        this.outputHdfsDir = this.metasOpt.getProfilingOutputHdfsDir();
        try {
            DataUtils.createFolder(conf, this.outputHdfsDir);
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] Fail to create profiling output directory.");
            e.printStackTrace();
        }

        LOG.info("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] Profiling process: Temp directory: " + this.tmpDir +
                " Output Directpry: " + this.outputHdfsDir);

    }

    /**
     *
     * @param samFileList Paths string list. Format: ReadGroupID\tPath
     */
    public void processInitialize(List<String> samFileList){
        String multiSamListFile = this.tmpDir + "/" + "tmp-multiSampleSAMList";

        File samList = new File(multiSamListFile);
        FileOutputStream fos1;
        BufferedWriter bw1;

        try {
            fos1 = new FileOutputStream(samList);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            for (String samInput: samFileList) {
                bw1.write(samInput);
                bw1.newLine();
            }

            bw1.close();
        } catch (FileNotFoundException e){
            e.printStackTrace();
            LOG.error("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] MultiSampleFileList not found. " + e.toString());
        } catch (IOException e){
            e.printStackTrace();
            LOG.error("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] MultiSampleFileList IO error. " + e.toString());
        }

        this.processInitialize(multiSamListFile);
    }

    public void processInitialize(String multiSamListFile){

        try {
            samMultiSampleList = new SAMMultiSampleList(multiSamListFile, true, false, true);
            this.jscontext.hadoopConfiguration().set("metas.data.mapreduce.input.samsamplelist", multiSamListFile);

        } catch (IOException e){
            LOG.error("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] Portable error: Multi-sample SAM file list can't be loaded.");
            e.printStackTrace();
        }
    }

    /**
     * Runs profiling. All options should have been set.
     *
     * @return The RDD of ProfilingResultRecord, which stores the data of all the profiling result and
     *     and other necessary information.
     */
    public List<String> runProfilingProcess(){

        int sampleCount = samMultiSampleList.getSampleCount();
        String filePath = this.samMultiSampleList.getAllSAMFilePath();

        // Add one more partition for files without sample information.
        int numPartition = this.numPartitionEachSample * sampleCount + 1;
        SampleIDReadNamePartitioner sampleIDClusterNamePartitioner = new SampleIDReadNamePartitioner(numPartition,
                this.numPartitionEachSample);
        SampleIDPartitioner sampleIDPartitioner = new SampleIDPartitioner(sampleCount + 1);
        LOG.trace("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] SampleCount: " + sampleCount +
                " Partition each sample: " + this.numPartitionEachSample + " Total Partition Number: " + numPartition);

        LOG.trace("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] All input sam file paths: " + filePath);

        /*
        Note: filter() operation of rdd will return a new RDD containing only the elements that makes the filter return true.

        SamRecord reading.

        After newAPIHadoopFile && filter
        key: sampleID + "\t" + readName.replaceFirst("/[12]$", "") <Text>
        value: SAMRecordWritable
        partition: defaultPartitoner

        After mapToPair:
        key: sampleID + "\t" + readName.replaceFirst("/[12]$", "")
        value: SAMRecord
        partition: defaultPartitoner
         */
        JavaPairRDD<String, SAMRecord> cleanMetasSamRecordRDD = this.jscontext
                .newAPIHadoopFile(filePath, MetasSamInputFormat.class, Text.class, SAMRecordWritable.class,
                        this.jscontext.hadoopConfiguration())
                .filter(tup -> tup._2 != null)
                .mapToPair(rec -> {
                    //LOG.trace("[SOAPMetas::" + ProfilingProcessMS.class.getName() + "] Remaining samRecod: " + rec._1.toString());
                    return new Tuple2<>(rec._1.toString(), rec._2.get());
                });
        if (this.doIdentityFiltering){
            cleanMetasSamRecordRDD = cleanMetasSamRecordRDD.filter(this.identityFilter);
        }

        /*
        SamRecord Group and merge.

        InputRDD:
         key: sampleID\treadName
         value: SAMRecord
         partition: defaultPartition

        After groupByKey:
         key: sampleID\treadName
         value: Interable<SAMRecord>
         partition: sampleID + clusterName

        After mapValues && filter:
         key: sampleID\treadName
         value: MetasSamPairRecord
         partition: sampleID + clusterName
        */
        JavaPairRDD<String, MetasSamPairRecord> readMetasSamPairRDD = cleanMetasSamRecordRDD
                .groupByKey(sampleIDClusterNamePartitioner)
                .mapValues(new SamRecordListMergeFunction(this.metasOpt.getSequencingMode()))
                .filter(item -> (item._2 != null));

        readMetasSamPairRDD.persist(StorageLevel.MEMORY_ONLY_SER());


        /*
        Since clusterName functions similarly as readName does, so we use the same partitioner.

        After runProfiling:
         key: sampleID
         value: ProfilingResultRecord (Tuple3<raw read count (uncorrected), corrected read count, merged read>)
         partition: sampleID + clusterName

        Note: Result Record doesn't contains relative abundance. And the RDD has been partitioned
         according to the sampleID and clusterName.hash.
        */
        JavaPairRDD<String, ProfilingResultRecord> profilingResultRecordRDD = getProfilingMethod()
                .runProfiling(readMetasSamPairRDD, sampleIDClusterNamePartitioner);


        profilingResultRecordRDD.persist(StorageLevel.MEMORY_AND_DISK());
        readMetasSamPairRDD.unpersist();

        /*
        Input:
         key: sampleID
         value: ProfilingResultRecord
         partition: sampleID + clusterName

        After mapToPair:
         key: sampleID
         value: (Double) abundance
         partition: sampleID + clusterName

        After reduceByKey:
         key: sampleID
         value: (Double) sampleTotalAbundance
         partition: sampleID
         */
        Map<String, Double> sampleTotalAbundanceMap = profilingResultRecordRDD
                .mapValues(ProfilingResultRecord::getAbundance)
                .reduceByKey(sampleIDPartitioner, Double::sum)
                .collectAsMap();


        /*
        Output Test
         */
        //Iterator<Tuple2<String, String>> resultRecordRDDValues = profilingResultRecordRDD
        //        .mapValues(v -> v.toString()).collect().iterator();
        //try(BufferedWriter bfw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/home/metal/TEST/SOAPMetas_profilingResult"))))) {
        //    Tuple2<String, String> rec;
        //    while (resultRecordRDDValues.hasNext()) {
        //        rec = resultRecordRDDValues.next();
        //        bfw.write(rec._1());
        //        bfw.write(" || value: ");
        //        bfw.write(rec._2());
        //        bfw.newLine();
        //    }
        //} catch (IOException e){
        //    e.printStackTrace();
        //}
        //Iterator<Map.Entry<String, Double>> totalAbunMap = sampleTotalAbundanceMap.entrySet().iterator();
        //try(BufferedWriter bfw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/home/metal/TEST/SOAPMetas_totalAbundance"))))) {
        //    Map.Entry<String, Double> rec;
        //    while (totalAbunMap.hasNext()) {
        //        rec = totalAbunMap.next();
        //        bfw.write(rec.getKey());
        //        bfw.write(" || value: ");
        //        bfw.write(rec.getValue().toString());
        //        bfw.newLine();
        //    }
        //} catch (IOException e){
        //    e.printStackTrace();
        //}
        //resultRecordRDDValues = null;
        //totalAbunMap = null;

        /*
        Input:
         key: sampleID
         value: ProfilingResultRecord (no relative abundance)
         partition: sampleID + clusterName

        After partitionBy:
         key: sampleID
         value: ProfilingResultRecord (no relative abundance)
         partition: sampleID

        After mapPartitions:
         outputHDFSDir/profiling/<appID>-Profiling-<readGroupID>.abundance[.evaluation]
         */
        List<String> outputFilePathsList = profilingResultRecordRDD
                .partitionBy(sampleIDPartitioner)
                .mapPartitionsWithIndex(new ProfilingResultWriteFunction(
                        profilingResultRecordRDD.context(),
                        this.outputHdfsDir, sampleTotalAbundanceMap, this.analysisMode
                        ), true)
                .collect();

        profilingResultRecordRDD.unpersist();
        return outputFilePathsList;
    }

    /**
     * Choosing proper profiling pipeline.
     *
     * @return ProfilingMethodBase New profiling pipeline instance of selected software.
     */
    private ProfilingMethodBase getProfilingMethod(){
        if(this.pipeline.equals("comg")){
            return new COMGProfilingMethod(this.metasOpt);
        } else if (this.pipeline.equals("metaphlan")){
            return new METAPHLANProfilingMethod(this.metasOpt);
        } else {
            return new COMGProfilingMethod(this.metasOpt);
        }
    }
}
