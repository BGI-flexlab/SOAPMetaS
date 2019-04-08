package org.bgi.flexlab.metas.alignment;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.alignment.metasbowtie2.BowtieTabAlignmentMethod;
import org.bgi.flexlab.metas.alignment.metasbowtie2.MetasBowtie;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDReadNamePartitioner;
import org.bgi.flexlab.metas.data.mapreduce.input.fastq.MetasFastqFileInputFormat;
import org.bgi.flexlab.metas.data.structure.fastq.FastqMultiSampleList;
import org.bgi.flexlab.metas.util.DataUtils;
import scala.Tuple2;

import java.io.*;
import java.util.List;

/**
 * ClassName: AlignmentProcessMS
 * Description: Control multi-sample mapping process. A standard sampleList file is needed.
 *
 * Note:
 * 1. Sample List file format:(tab delimited)
 * sampleID(arbitrary unique number) RG_Info(@RG\tID:xxxxx\tPL:xxx) fq_1 fq_2 adapter1 adapter2 [RG_ID]
 * 2. The format is chosen in conjunction with SOAPGaea pre-process pipeline.
 * 3. RG_ID isn't part of original format, so it's optional.
 * 4. The input file path passed to the tool must be identical with that in list file.
 *
 *
 * @author heshixu@genomics.cn
 */

public class AlignmentProcessMS {


    private static final Log LOG = LogFactory.getLog(AlignmentProcess.class); // The LOG
    private JavaSparkContext jscontext;	// The Java Spark Context
    private MetasOptions options;// *Changes: original is options for BWA.

    private FastqMultiSampleList fastqMultiSampleList;
    private int numPartitionEachSample = 1;

    private String samOutputHdfsDir;
    private String tmpDir;


    /**
     * Constructor to build the AlignmentProcess object from Metas Main program.
     * the MetasOptions and the Spark Context objects need to be passed as argument.
     *
     * @param options The MetasOptions object initialized with the user options
     * @param context The Spark Context from the Spark Shell. Usually "sc"
     */
    public AlignmentProcessMS(MetasOptions options, JavaSparkContext context) {
        this.options = options;
        this.jscontext = context;
        this.initProcess();
        LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] AlignmentProcessMS Construction finished.");
    }

    /**
     * Constructor to build the AlignmentProcess from command line arguments.
     *
     * *Changes:
     *  + Change BwaOption to MetasOptions.
     *
     * @param args Arguments got from Linux console when launching MetaS with Spark
     */
    public AlignmentProcessMS(String[] args) {
        this.options = new MetasOptions(args);
        this.jscontext = new JavaSparkContext(new SparkConf().setAppName("MetasBowtie"));
        this.initProcess();
    }

    /**
     * Procedure to initiate the AlignmentProcess configuration parameters
     *
     */
    private void initProcess() {

        this.numPartitionEachSample = Math.max(this.options.getNumPartitionEachSample(), 1);

        this.samOutputHdfsDir = this.options.getSamOutputHdfsDir();

        Configuration conf = this.jscontext.hadoopConfiguration();

        try {
            DataUtils.createFolder(conf, this.samOutputHdfsDir);
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Fail to create SAM output directory.");
            e.printStackTrace();
        }

        /*
        Set temp directory
         */
        this.tmpDir = this.options.getAlignmentTmpDir();
        if (this.tmpDir == null) {
            this.tmpDir = DataUtils.getTmpDir(this.jscontext);
        } else {
            try {
                DataUtils.createFolder(conf, this.tmpDir);
            } catch (IOException e){
                LOG.error("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Fail to create alignment temp directory.");
                e.printStackTrace();
            }
        }

        LOG.info("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Alignment process: Temp directory: " + this.tmpDir +
                " Output Directpry: " + this.samOutputHdfsDir);

        /*
        Read multiple sample list file.
         */
        String multilistFile;
        try {
            if (this.options.isSingleSample()){
                multilistFile = generateMultiSampleList(this.options.getInputFastqPath(), this.options.getInputFastqPath2());
                LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Create multiSampleList file.");
            } else {
                multilistFile = this.options.getMultiSampleList();
            }

            conf.set("metas.data.mapreduce.input.fqsamplelist", multilistFile);
            LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Hadoop context configure " +
                    "\"metas.data.mapreduce.input.fqsamplelist\": " + conf.get("metas.data.mapreduce.input.fqsamplelist"));

            this.fastqMultiSampleList = new FastqMultiSampleList(multilistFile, true, false, true);
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Fail to load multisample list file.");
            e.printStackTrace();
        }
    }

    private String generateMultiSampleList(String fastqPath1, String fastqPath2){
        String[] fq1Array = StringUtils.split(fastqPath1, ',');
        String[] fq2Array = null;
        String rgID = this.options.getReadGroupID();
        if (fastqPath2 != null && !fastqPath2.isEmpty()){
            fq2Array = StringUtils.split(fastqPath2, ',');
            assert fq1Array.length == fq2Array.length: "Numbers of paired fastq files are not equal.";
        }

        String outputMultiSampleList = this.samOutputHdfsDir + "/tmp-multiSampleFastqList";
        File sampleList = new File(outputMultiSampleList);

        try (BufferedWriter bfr = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(sampleList)))){

            if (fq2Array.length > 0) {
                for (int i = 0; i < fq1Array.length; i++) {
                    bfr.write(rgID + "\t" + fq1Array[i] + "\t" + fq2Array[i]);
                    bfr.newLine();
                }
            } else {
                for (int i = 0; i < fq1Array.length; i++) {
                    bfr.write(rgID + "\t" + fq1Array[i]);
                    bfr.newLine();
                }
            }
        } catch (FileNotFoundException e){
            LOG.error("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Can't create multiple sample fastq list file.");
            e.printStackTrace();
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Can't write multiple sample fastq list file.");
            e.printStackTrace();
        }
        return outputMultiSampleList;
    }


    ///**
    // * Form input reads data.
    // *
    // * @return PairRDD. Key: readGroupID; value: bowtie_tab5_format
    // */
    //private JavaPairRDD<String, String> handleMultiSampleReads(){
//
    //    // Add one more partition for files without sample information.
    //    int numPartition = this.numPartitionEachSample * this.fastqMultiSampleList.getSampleCount() + 1;
//
    //    String filePath = this.fastqMultiSampleList.getAllFastqPath();
//
    //    /*
    //    Merge pair read and prepare RDD for repartition.
//
    //    After hadoopfile:
    //     key: sampleID#readName <Text>
    //     value: mateIndex(1 or 2)##sampleID	pos	filelength##readGroupID##sequence	quality
//
    //    After reduceByKey && filter:
    //     key: sampleID#readName <Text>
    //     value: mateIndex(3)##sampleID	pos filelength##readGroupID##seq1    qual1[    seq2    qual2]
//
    //    After mapToPair:
    //     key: sampleID	pos filelength
    //     value: readGroupID##readName   seq1    qual1[    seq2    qual2]
    //     */
    //    JavaPairRDD<String, String> tab5RDD = this.jscontext
    //            .hadoopFile(filePath, MetasFastqFileInputFormat.class,
    //                    Text.class, Text.class)
    //            .reduceByKey(new SampleIDReadNamePartitioner(this.fastqMultiSampleList.getSampleCount()),
    //                    (v1, v2) -> {
    //                String[] values1 = v1.toString().split("##");
    //                String[] values2 = v2.toString().split("##");
    //                int mate1 = Integer.parseInt(values1[0]);
    //                int mate2 = Integer.parseInt(values2[0]);
    //                StringBuilder newsr = new StringBuilder();
    //                if (mate1 == 1 && mate2 == 2){
    //                    newsr.append(3).append("##")
    //                            .append(values1[1]).append("##").append(values1[2]).append("##")
    //                            .append(values1[3]).append("\t").append(values2[3]).trimToSize();
    //                } else if (mate1 == 2 && mate2 == 1){
    //                    newsr.append(3).append("##")
    //                            .append(values2[1]).append("##").append(values2[2]).append("##")
    //                            .append(values2[3]).append("\t").append(values1[3]).trimToSize();
    //                } else {
    //                    LOG.warn("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Ignore read name with three sequences. ReadGroupID: " + values1[2]
    //                    + " Seq and qual: " + values1[3] + "\t" + values2[3]);
    //                    return null;
    //                }
    //                Text newValue = new Text();
    //                newValue.set(newsr.toString());
    //                return newValue;
    //            })
    //            .filter(record -> record._2 != null)
    //            .mapToPair(record -> {
    //                String[] values = record._2.toString().split("##");
    //                Tuple2<String, String> returned = new Tuple2<>(
    //                        values[1],
    //                        values[2] + "##" + record._1.toString().split("#")[1] + "\t" + values[3]
    //                );
    //                return returned;
    //            });
//
    //    /*
    //    Repartition RDD by sample.
//
    //    After partitionBy:
    //     key: sampleID	pos filelength
    //     value: readGroupID##readName   seq1    qual1    seq2    qual2
//
    //    After mapToPair:
    //     key: readGroupID
    //     value: readName   seq1    qual1    seq2    qual2
    //     */
    //    JavaPairRDD<String, String> partitionedTab5RDD = tab5RDD
    //            .partitionBy(new FastqOffsetPartitioner(numPartition, this.options.getNumPartitionEachSample()))
    //            .mapToPair(record -> {
    //                String[] values = record._2.split("##");
    //                return new Tuple2<>(values[0], values[1]);
    //            });
//
    //    return partitionedTab5RDD;
    //}

    /**
     * Form input reads data.
     *
     * @return PairRDD. Key: readGroupID; value: bowtie_tab5_format
     */
    private JavaPairRDD<String, String> handleMultiSampleReads(){

        // Add one more partition for files without sample information.
        int numPartition = this.numPartitionEachSample * this.fastqMultiSampleList.getSampleCount() + 1;
        SampleIDReadNamePartitioner sampleIDPartitioner = new SampleIDReadNamePartitioner(numPartition,
                this.numPartitionEachSample);

        String filePath = this.fastqMultiSampleList.getAllFastqPath();
        LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Start handling multi-sample reads. " +
                "Total partition number: " + numPartition + " Input fastq file: " + filePath);

        //List<Tuple2<String, String>> testHadoopFile = this.jscontext.hadoopFile(filePath, MetasFastqFileInputFormat.class,
        //                Text.class, Text.class)
        //        .mapToPair(rec -> new Tuple2<>(rec._1.toString(), rec._2.toString()))
        //        .collect();
        //try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("/home/metal/TEST/temp.rdd"))))) {
        //    for (Tuple2<String, String> rec : testHadoopFile) {
        //        bw.write(rec._1);
        //        bw.write(" -- ");
        //        bw.write(rec._2);
        //        bw.newLine();
        //    }
        //} catch (IOException e){
        //    e.printStackTrace();
        //}

        /*
        Merge pair read and prepare RDD for repartition.

        After hadoopfile:
		 key: sampleID	readName
		 value: mateIndex(1 or 2 or 0)||readGroupID||sequence	quality||sampleID   readName
		 partition: default

        After reduceByKey && filter:
         key: sampleID  readName
         value: mateIndex(3)||readGroupID##seq1    qual1[    seq2    qual2]||sampleID   readName
         partition:  sampleID + readName

        After mapToPair:
         key: readGroupID
         value: readName   seq1    qual1[    seq2    qual2]
         partition: sampleID + readName
         */

        JavaPairRDD<String, String> partitionedTab5RDD = this.jscontext
                .hadoopFile(filePath, MetasFastqFileInputFormat.class,
                        Text.class, Text.class)
                .mapToPair(rec -> new Tuple2<>(rec._1.toString(), rec._2.toString()))
                .reduceByKey(sampleIDPartitioner, (v1, v2) -> {
                            if (v1 == null) return v2;
                            if (v2 == null) return v1;
                            String[] values1 = StringUtils.split(v1, "||");
                            String[] values2 = StringUtils.split(v2, "||");
                            LOG.trace("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] value1: " +
                                    values1[0] + " " + values1[1] + "\t" + values1[3] +
                                    " |||| value2: " + values2[0] + " " + values2[1] + "\t" + values2[3]);
                            int mate1 = Integer.parseInt(values1[0]);
                            int mate2 = Integer.parseInt(values2[0]);
                            StringBuilder newsr = new StringBuilder();
                            if (mate1 == 1 && mate2 == 2){
                                newsr.append(3).append("||")
                                        .append(values1[1]).append("||").append(values1[2])
                                        .append("\t").append(values2[2])
                                        .append("||").append(values1[3]).trimToSize();
                            } else if (mate1 == 2 && mate2 == 1){
                                newsr.append(3).append("||")
                                        .append(values2[1]).append("||").append(values2[2])
                                        .append("\t").append(values1[2])
                                        .append("||").append(values2[3]).trimToSize();
                            } else {
                                LOG.warn("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] " +
                                        "Wrong mate number, Please check fastq file. ReadGroupID1: " + values1[1] +
                                        "\tmate1: " + mate1 + "\tSampleID_and_ReadName1: " + values1[3] +
                                        "\tReadGroupID2: " + values2[1] + "\tmate2: " + mate2 +
                                        "\tSampleID_and_ReadName2:" + values2[3]);
                                if (mate1 == 3) return v1;
                                if (mate2 == 3) return v2;
                                if (v1.equals(v2)) return v1;
                                return null;
                            }
                            return newsr.toString();
                        })
                .filter(record -> {
                    //LOG.trace("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Check for filtering. key: " + record._1);
                    return record._2 != null;
                })
                .mapToPair(record -> {
                    String[] values = StringUtils.split(record._2, "||");
                    return new Tuple2<>(
                            values[1],
                            StringUtils.split(record._1, '\t')[1] + "\t" + values[2]
                    );
                });

        return partitionedTab5RDD;
    }

    /**
     * Runs BWA with the specified options
     *
     * *Changes:
     *  + Change getOutputHdfsDir to getSamOutputHdfsDir
     *  + Turn off merge procedure. Merge by sample needs more consideration.
     *  + Delete deprecated code block which is commented originally.
     *  + Change Bwa instance to AlignmentToolWrapper instance.
     *  + Log content
     *
     * TODO: Method of "merge by sample". A new class for merging SAM file is needed.
     *
     * Note: This function runs BWA with the input data selected and with the options also selected
     *     by the user.
     */
    public List<String> runAlignment() {

        LOG.info("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Starting Multiple Sample Alignment");

        AlignmentToolWrapper alignmentToolWrapper = getAlignmentTool(this.options.getAlignmentTool());

        JavaPairRDD<String, String> partitionedTab5RDD = handleMultiSampleReads();

        /*
        Returned list of mapping results files' paths.

        Input:
         key: readGroupID
         value: readName   seq1    qual1[    seq2    qual2]
         partition: sampleID + readName

        After mapPartitionsWithIndex:
         readGroupID    outputHDFSDir/<appId>-RDDPart<index>-<readGroupID>.sam
         */
        List<String> returnedValues = partitionedTab5RDD
                .mapPartitionsWithIndex(new BowtieTabAlignmentMethod(partitionedTab5RDD.context(), alignmentToolWrapper), true)
                .collect();

        partitionedTab5RDD.unpersist();


        return returnedValues;
    }

    /**
     * Factory method for AlignmentToolWrapper.
     *
     * *Changes:
     *  + The method is newly created method for generation of wrapper object.
     *
     * @return Wrapped instance of alignment tools
     */
    private AlignmentToolWrapper getAlignmentTool(String toolName){

        if (toolName.equals("bowtie")){
            return new MetasBowtie(this.options, this.jscontext);
        } else if (toolName.equals("bwa")) {
            //return new Bwa(this.options);
            LOG.error("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Bwa not support in this version. Switch to bowtie.");
            return new MetasBowtie(this.options, this.jscontext);
        } else {
            return new MetasBowtie(this.options, this.jscontext);
        }
    }

}
