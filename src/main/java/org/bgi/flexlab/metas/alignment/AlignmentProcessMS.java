package org.bgi.flexlab.metas.alignment;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.alignment.metasbowtie2.BowtieTabAlignmentMethod;
import org.bgi.flexlab.metas.alignment.metasbowtie2.MetasBowtie;
import org.bgi.flexlab.metas.data.mapreduce.input.fastq.MetasFastqInputFormat;
import org.bgi.flexlab.metas.data.mapreduce.input.fastq.MetasSEFastqInputFormat;
import org.bgi.flexlab.metas.data.mapreduce.partitioner.SampleIDReadNamePartitioner;
import org.bgi.flexlab.metas.data.structure.fastq.FastqMultiSampleList;
import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;

import java.io.*;
import java.util.List;

/**
 * ClassName: AlignmentProcessMS
 * Description: Control multi-sample alignment process. A standard sampleList file is needed.
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


    private static final Log LOG = LogFactory.getLog(AlignmentProcessMS.class); // The LOG
    private JavaSparkContext jscontext;	// The Java Spark Context
    private MetasOptions options;// *Changes: original is options for BWA.

    private FastqMultiSampleList fastqMultiSampleList;
    private int numPartition = 32;
    private SequencingMode seqMode;

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
        //LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] AlignmentProcessMS Construction finished.");
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

        this.seqMode = this.options.getSequencingMode();

        this.numPartition = Math.max(this.options.getPartitionNumber(), 1);

        //String samOutputHdfsDir = this.options.getSamOutputHdfsDir();

        Configuration conf = this.jscontext.hadoopConfiguration();

        /*
        Read multiple sample list file.
         */
        try {
            String multilistFile = this.options.getMultiSampleList();

            conf.set("metas.data.mapreduce.input.fqsamplelist", multilistFile);
            LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Hadoop context configure " +
                    "\"metas.data.mapreduce.input.fqsamplelist\": " + conf.get("metas.data.mapreduce.input.fqsamplelist"));

            this.fastqMultiSampleList = new FastqMultiSampleList(multilistFile, this.options.isLocalFS(), false, true);
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Fail to load multisample list file. " + e.toString());
        }
    }

    /**
     * Form input reads data.
     *
     * @return PairRDD. Key: readGroupID; value: bowtie_tab5_format
     */
    private JavaPairRDD<String, String> handleMultiSampleReads(){

        // Add one more partition for files without sample information.
        HashPartitioner samplePartitioner = new HashPartitioner(this.numPartition);
        //SampleIDReadNamePartitioner samplePartitioner = new SampleIDReadNamePartitioner(numPartition, this.numPartitionEachSample);

        String filePath = this.fastqMultiSampleList.getAllFastqPath();
        LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Read multi-sample reads in. " +
                "Total partition number: " + numPartition + ". Input fastq file: " + filePath);

        /*
        Merge pair read and prepare RDD for repartition.

        After hadoopFile:
		 key: sampleID	readName
		 value: mateIndex(1 or 2 or 0)||readGroupID SMTag||sequence	quality||sampleID   readName
		 partition: default

        After reduceByKey && filter:
         key: sampleID  readName
         value: mateIndex(3)||readGroupID   SMTag||seq1    qual1[    seq2    qual2]||sampleID   readName
         partition:  sampleID + readName

        After mapToPair:
         key: readGroupID   SMTag
         value: readName   seq1    qual1[    seq2    qual2]
         partition: sampleID + readName
         */

        JavaPairRDD<String, String> partitionedTab5RDD = this.jscontext
                .hadoopFile(filePath, MetasFastqInputFormat.class,
                        Text.class, Text.class)
                .mapToPair(rec -> new Tuple2<>(rec._1.toString(), rec._2.toString()))
                .reduceByKey(samplePartitioner, (v1, v2) -> {
                            if (v1 == null) return v2;
                            if (v2 == null) return v1;

                            String[] values1 = StringUtils.split(v1, "||");
                            String[] values2 = StringUtils.split(v2, "||");
                            //LOG.trace("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] value1: " +
                            //        values1[0] + " " + values1[1] + "\t" + values1[3] +
                            //        " |||| value2: " + values2[0] + " " + values2[1] + "\t" + values2[3]);
                            int mate1 = Integer.parseInt(values1[0]);
                            int mate2 = Integer.parseInt(values2[0]);
                            StringBuilder newsr = new StringBuilder(256);
                            if (mate1 == 1 && mate2 == 2){
                                newsr.append(3).append("||")
                                        .append(values1[1]).append("||").append(values1[2])
                                        .append('\t').append(values2[2])
                                        .append("||").append(values1[3]);
                            } else if (mate1 == 2 && mate2 == 1){
                                newsr.append(3).append("||")
                                        .append(values2[1]).append("||").append(values2[2])
                                        .append('\t').append(values1[2])
                                        .append("||").append(values2[3]);
                            } else {
                                LOG.warn("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] " +
                                        "Wrong mate number, there are three reads with the same name. Please check fastq file. ReadGroupID1: " + values1[1] +
                                        "\tmate1: " + mate1 + "\tSampleID_and_ReadName1: " + values1[3] +
                                        "\tReadGroupID2: " + values2[1] + "\tmate2: " + mate2 +
                                        "\tSampleID_and_ReadName2:" + values2[3]);
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
                            StringUtils.split(record._1, '\t')[1] + '\t' + values[2]
                    );
                });

        /*
        Merge pair read and prepare RDD for repartition.

        After hadoopfile && mapToPair:
		 key: sampleID	readName
		 value: mateIndex(1 or 2 or 0)||readGroupID||sequence	quality||sampleID   readName
		 partition: default

        After groupByKey && mapToPair:
         key: sampleID  readName
         value: mateIndex(3)||readGroupID##seq1    qual1[    seq2    qual2]||sampleID   readName
         partition:  sampleID + readName

        After filter && mapToPair:
         key: readGroupID
         value: readName   seq1    qual1[    seq2    qual2]
         partition: sampleID + readName
         */
        //JavaPairRDD<String, String> partitionedTab5RDD = this.jscontext
        //        .hadoopFile(filePath, MetasFastqInputFormat.class,
        //                Text.class, Text.class)
        //        .mapToPair(rec -> new Tuple2<>(rec._1.toString(), rec._2.toString()))
        //        .groupByKey(sampleIDPartitioner)
        //        .mapToPair(rec -> {
        //            String key = rec._1;
        //            String value = null;
        //            int count = 0;
        //            String[] reads = new String[2];
        //            for (String read: rec._2){
        //                if (count == 2){
        //                    LOG.warn("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] " +
        //                            "Wrong mate number, Please check fastq file. Read key (sampleID readName): " + key);
        //                    return new Tuple2<>(key, null);
        //                }
        //                reads[count] = read;
        //                count++;
        //            }
        //            String[] values1 = StringUtils.split(reads[0], "||");
        //            String[] values2 = StringUtils.split(reads[1], "||");
        //            LOG.trace("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] value1: " +
        //                    values1[0] + " " + values1[1] + "\t" + values1[3] +
        //                    " |||| value2: " + values2[0] + " " + values2[1] + "\t" + values2[3]);
        //            int mate1 = Integer.parseInt(values1[0]);
        //            int mate2 = Integer.parseInt(values2[0]);
        //            StringBuilder newsr = new StringBuilder();
        //            if (mate1 == 1 && mate2 == 2){
        //                value = newsr.append(3).append("||")
        //                        .append(values1[1]).append("||").append(values1[2])
        //                        .append('\t').append(values2[2])
        //                        .append("||").append(values1[3]).toString();
        //            } else if (mate1 == 2 && mate2 == 1){
        //                value = newsr.append(3).append("||")
        //                        .append(values2[1]).append("||").append(values2[2])
        //                        .append('\t').append(values1[2])
        //                        .append("||").append(values2[3]).toString();
        //            } else {
        //                LOG.warn("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] " +
        //                        "Wrong mate number, Please check fastq file. ReadGroupID1: " + values1[1] +
        //                        "\tmate1: " + mate1 + "\tSampleID_and_ReadName1: " + values1[3] +
        //                        "\tReadGroupID2: " + values2[1] + "\tmate2: " + mate2 +
        //                        "\tSampleID_and_ReadName2:" + values2[3]);
        //                if (reads[0].equals(reads[1])) value = reads[0];
        //            }
        //            return new Tuple2<>(key, value);
        //        })
        //        .filter(record -> {
        //            //LOG.trace("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Check for filtering. key: " + record._1);
        //            return record._2 != null;
        //        })
        //        .mapToPair(record -> {
        //            String[] values = StringUtils.split(record._2, "||");
        //            return new Tuple2<>(
        //                    values[1],
        //                    StringUtils.split(record._1, '\t')[1] + "\t" + values[2]
        //            );
        //        });

        return partitionedTab5RDD;
    }

    private JavaPairRDD<String, String> handleSingleEndReads(){

        // Add one more partition for files without sample information.
        HashPartitioner samplePartitioner = new HashPartitioner(this.numPartition);
        //int numPartition = this.numPartitionEachSample * this.fastqMultiSampleList.getSampleCount() + 1;
        //SampleIDReadNamePartitioner sampleIDPartitioner = new SampleIDReadNamePartitioner(numPartition, this.numPartitionEachSample);

        String filePath = this.fastqMultiSampleList.getAllFastqPath();
        LOG.debug("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Read multi-sample reads in. " +
                "Total partition number: " + numPartition + ". Input fastq file: " + filePath);

        /*
        Merge pair read and prepare RDD for repartition.

        After newAPIHadoopFile && partitionBy
		 key: sampleID	readName
		 value: readGroupID SMTag||sequence	quality
		 partition: sampleID + readName

        After mapToPair
         key: readGroupID   SMTag
         value: readName   seq1    qual1
         partition: sampleID + readName
         */
        JavaPairRDD<String, String> partitionedTab5RDD = this.jscontext
                .newAPIHadoopFile(filePath, MetasSEFastqInputFormat.class,
                        Text.class, Text.class, this.jscontext.hadoopConfiguration())
                .mapToPair(rec -> new Tuple2<>(rec._1.toString(), rec._2.toString()))
                .partitionBy(samplePartitioner)
                .mapToPair(rec -> {
                    String[] keys =  StringUtils.split(rec._1, '\t');
                    String[] values = StringUtils.split(rec._2, "||");
                    return new Tuple2<>(values[0], keys[1] + '\t' + values[1]);
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

        JavaPairRDD<String, String> partitionedTab5RDD;

        if (this.seqMode.equals(SequencingMode.PAIREDEND)) {
            LOG.info("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Handling input reads. Paired-end mode.");
            partitionedTab5RDD = handleMultiSampleReads();
        } else {
            LOG.info("[SOAPMetas::" + AlignmentProcessMS.class.getName() + "] Handling input reads. Single-end mode.");
            partitionedTab5RDD = handleSingleEndReads();
        }

        /*
        Returned list of alignment results files' paths.

        Input:
         key: readGroupID   SMTag
         value: readName   seq1    qual1[    seq2    qual2]
         partition: sampleID + readName

        After mapPartitionsWithIndex:
         readGroupID    samTag    outputHDFSDir/<appId>-RDDPart<index>-<readGroupID>.sam
         */
        List<String> returnedValues = partitionedTab5RDD
                .mapPartitionsWithIndex(new BowtieTabAlignmentMethod(partitionedTab5RDD.context(),
                                alignmentToolWrapper), true)
                .collect();

        //partitionedTab5RDD.unpersist();


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
