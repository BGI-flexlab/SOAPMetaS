package org.bgi.flexlab.metas.alignment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.alignment.metasbowtie2.BowtieTabAlignmentMethod;
import org.bgi.flexlab.metas.alignment.metasbowtie2.MetasBowtie;
import org.bgi.flexlab.metas.data.mapreduce.SampleIDReadNamePartitioner;
import org.bgi.flexlab.metas.data.mapreduce.input.fastq.MetasFastqFileInputFormat;
import org.bgi.flexlab.metas.data.structure.fastq.FastqMultiSampleList;
import scala.Tuple2;

import java.io.IOException;
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
 * @author: heshixu@genomics.cn
 */

public class AlignmentProcessMS {


    private static final Log LOG = LogFactory.getLog(AlignmentProcess.class); // The LOG
    private JavaSparkContext jscontext;	// The Java Spark Context
    private MetasOptions options;// *Changes: original is options for BWA.

    private FastqMultiSampleList fastqMultiSampleList;
    private int numPartitionEachSample = 1;

    private String samOutputHdfsDir;


    /**
     * Constructor to build the AlignmentProcess object from Metas Main program.
     * the MetasOptions and the Spark Context objects need to be passed as argument.
     *
     * @param options The MetasOptions object initialized with the user options
     * @param context The Spark Context from the Spark Shell. Usually "sc"
     * @return The AlignmentProcess object with its options initialized.
     */
    public AlignmentProcessMS(MetasOptions options, JavaSparkContext context) {
        this.options = options;
        this.jscontext = context;
        this.initProcess();
    }

    /**
     * Constructor to build the AlignmentProcess from command line arguments.
     *
     * *Changes:
     *  + Change BwaOption to MetasOptions.
     *
     * @param args Arguments got from Linux console when launching MetaS with Spark
     * @return The AlignmentProcess object with its options initialized.
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
    public void initProcess() {

        this.numPartitionEachSample = Math.max(this.options.getNumPartitionEachSample(), 1);

        this.samOutputHdfsDir = this.options.getSamOutputHdfsDir();

        // Multiple Sample information list
        try {
            this.fastqMultiSampleList = new FastqMultiSampleList(this.options.getMultiSampleList(), true, true);
        } catch (IOException e){
            LOG.error("can't load multisample list file.");
            e.printStackTrace();
        }

        this.jscontext.hadoopConfiguration().set("metas.data.mapreduce.input.multisamplelist", this.options.getMultiSampleList());

        createOutputFolder();
    }

    /**
     * Method to create the output folder in HDFS
     *
     * *Changes:
     *  + Change getOutputHdfsDir to getSamOutputHdfsDir.
     */
    private void createOutputFolder() {
        try {
            FileSystem fs = FileSystem.get(this.jscontext.hadoopConfiguration());

            // Path variable
            Path outputDir = new Path(this.samOutputHdfsDir);

            // Directory creation
            if (!fs.exists(outputDir)) {
                fs.mkdirs(outputDir);
            }
            else {
                fs.delete(outputDir, true);
                fs.mkdirs(outputDir);
            }

            fs.close();
        }
        catch (IOException e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
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
    //                    LOG.warn("Ignore read name with three sequences. ReadGroupID: " + values1[2]
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

        /*
        Merge pair read and prepare RDD for repartition.

        After hadoopfile:
		 key: sampleID	readName
		 value: mateIndex(1 or 2)##readGroupID##sequence	quality

        After reduceByKey && filter:
         key: sampleID  readName <Text>
         value: mateIndex(3)##readGroupID##seq1    qual1[    seq2    qual2]

        After mapToPair:
         key: readGroupID
         value: readName   seq1    qual1[    seq2    qual2]
         */
        JavaPairRDD<String, String> partitionedTab5RDD = this.jscontext
                .hadoopFile(filePath, MetasFastqFileInputFormat.class,
                        Text.class, Text.class)
                .reduceByKey(sampleIDPartitioner, (v1, v2) -> {
                            String[] values1 = v1.toString().split("##");
                            String[] values2 = v2.toString().split("##");
                            int mate1 = Integer.parseInt(values1[0]);
                            int mate2 = Integer.parseInt(values2[0]);
                            StringBuilder newsr = new StringBuilder();
                            if (mate1 == 1 && mate2 == 2){
                                newsr.append(3).append("##")
                                        .append(values1[1]).append("##").append(values1[2])
                                        .append("\t").append(values2[2]).trimToSize();
                            } else if (mate1 == 2 && mate2 == 1){
                                newsr.append(3).append("##")
                                        .append(values2[1]).append("##").append(values2[2])
                                        .append("\t").append(values1[2]).trimToSize();
                            } else {
                                LOG.warn("Ignore read name with three sequences. ReadGroupID: " + values1[2]
                                        + " Seq and qual: " + values1[3] + "\t" + values2[3]);
                                return null;
                            }
                            Text newValue = new Text();
                            newValue.set(newsr.toString());
                            return newValue;
                        })
                .filter(record -> record._2 != null)
                .mapToPair(record -> {
                    String[] values = record._2.toString().split("##");
                    Tuple2<String, String> returned = new Tuple2<>(
                            values[1],
                            record._1.toString().split("\t")[1] + "\t" + values[2]
                    );
                    return returned;
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
     * @brief This function runs BWA with the input data selected and with the options also selected
     *     by the user.
     */
    public List<String> runAlignment() {

        LOG.info("["+this.getClass().getName()+"] :: Starting Multiple Sample Alignment");

        AlignmentToolWrapper alignmentToolWrapper = getAlignmentTool(this.options.getAlignmentTool());

        JavaPairRDD<String, String> partitionedTab5RDD = handleMultiSampleReads();

        // Returned list of mapping results files' paths.
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
     * @return
     */
    private AlignmentToolWrapper getAlignmentTool(String toolName){

        AlignmentToolWrapper toolWrapper;

        switch (toolName){
            case "bowtie": {
                toolWrapper = new MetasBowtie(this.options);
                break;
            }
            /*
            case "bwa": {
                toolWrapper = new Bwa(this.options);
                break;
            }
            */
            default:{
                toolWrapper = new MetasBowtie(this.options);
                break;
            }
        }

        return toolWrapper;
    }

}
