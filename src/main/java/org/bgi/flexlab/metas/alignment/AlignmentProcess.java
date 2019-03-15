package org.bgi.flexlab.metas.alignment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.alignment.metasbowtie2.MetasBowtie;
import org.bgi.flexlab.metas.data.structure.fastq.FASTQRecordCreator;
import org.bgi.flexlab.metas.data.structure.fastq.FASTQRecordGrouper;
import org.bgi.flexlab.metas.util.SequencingMode;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * ClassName: AlignmentProcess
 * Description: Wrapper class for the process of alignment. The script is based on
 * com.github.sparkbwa.AlignmentProcess class. All changes are interpreted in comments with
 * "*Changes" label.
 *
 * GNU License.
 *
 * @author: heshixu@genomics.cn
 */

public class AlignmentProcess {

    private static final Log 				LOG = LogFactory.getLog(AlignmentProcess.class); // The LOG
    private SparkConf 						sparkConf; 								// The Spark Configuration to use
    private JavaSparkContext 				ctx;									// The Java Spark Context
    private Configuration 					conf;									// Global Configuration
    private long 							totalInputLength;
    private long 							blocksize;
    private MetasOptions 					options;								// *Changes: original is options for BWA.
    private String 							inputTmpFileName;


    /**
     * Constructor to build the AlignmentProcess object from the Spark shell When creating a
     * AlignmentProcess object from the Spark shell, the MetasOptions and the Spark Context objects need
     * to be passed as argument.
     *
     * @param optionsFromShell The MetasOptions object initialized with the user options
     * @param context The Spark Context from the Spark Shell. Usually "sc"
     * @return The AlignmentProcess object with its options initialized.
     */
    public AlignmentProcess(MetasOptions optionsFromShell, SparkContext context) {

        this.options = optionsFromShell;
        this.ctx = new JavaSparkContext(context);
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
    public AlignmentProcess(String[] args) {

        this.options = new MetasOptions(args);
        this.initProcess();
    }

    /**
     * Constructor to build AlignmentProcess object from MetasOptions object.
     * @param options
     */
    public AlignmentProcess(MetasOptions options){
        this.options = options;
        this.initProcess();
    }

    /**
     * Method to get the length from the FASTQ input or inputs. It is set in the class variable totalInputLength
     *
     * *Changes:
     *  + getInputPath() method is changed to getInputFastqPath().
     *  + the original cSummaryFile2 is also the ContentSummary of getInputPath()'s return value, here we change this to path2.
     */
    private void setTotalInputLength() {
        try {
            // Get the FileSystem
            FileSystem fs = FileSystem.get(this.conf);

            // To get the input files sizes
            ContentSummary cSummaryFile1 = fs.getContentSummary(new Path(options.getInputFastqPath()));

            long lengthFile1 = cSummaryFile1.getLength();
            long lengthFile2 = 0;

            if (!options.getInputFastqPath2().isEmpty()) {
                ContentSummary cSummaryFile2 = fs.getContentSummary(new Path(options.getInputFastqPath2()));
                lengthFile2 = cSummaryFile2.getLength();
            }

            // Total size. Depends on paired or single reads
            this.totalInputLength = lengthFile1 + lengthFile2;
            fs.close();
        } catch (IOException e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    /**
     * Method to create the output folder in HDFS
     *
     * *Changes:
     *  + Change getOutputHdfsDir to getSamOutputHdfsDir.
     */
    private void createOutputFolder() {
        try {
            FileSystem fs = FileSystem.get(this.conf);

            // Path variable
            Path outputDir = new Path(options.getSamOutputHdfsDir());

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

    /**
     * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
     * @param ctx The JavaSparkContext to use
     * @param pathToFastq The path to the FASTQ file
     * @return A JavaPairRDD containing <Long Read ID, String Read>
     */
    public static JavaPairRDD<Long, String> loadFastq(JavaSparkContext ctx, String pathToFastq) {
        JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);

        // Determine which FASTQ record the line belongs to.
        JavaPairRDD<Long, Tuple2<String, Long>> fastqLinesByRecordNum = fastqLines.zipWithIndex()
                .mapToPair(new FASTQRecordGrouper());

        // Group group the lines which belongs to the same record, and concatinate them into a record.
        return fastqLinesByRecordNum.groupByKey().mapValues(new FASTQRecordCreator());
    }

    /**
     * Method to perform and handle the single reads sorting
     *
     * *Changes:
     *  + getInputPath to getInputFastqPath.
     *
     * @return A RDD containing the strings with the sorted reads from the FASTQ file
     */
    private JavaRDD<String> handleSingleReadsSorting() {
        JavaRDD<String> readsRDD = null;

        long startTime = System.nanoTime();

        LOG.info("["+this.getClass().getName()+"] :: Not sorting in HDFS. Timing: " + startTime);

        // Read the FASTQ file from HDFS using the FastqInputFormat class
        JavaPairRDD<Long, String> singleReadsKeyVal = loadFastq(this.ctx, this.options.getInputFastqPath());

        // Sort in memory with no partitioning
        if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
            // First, the join operation is performed. After that,
            // a sortByKey. The resulting values are obtained
            readsRDD = singleReadsKeyVal.sortByKey().values();
            LOG.info("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
        }

        // Sort in memory with partitioning
        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
            singleReadsKeyVal = singleReadsKeyVal.repartition(options.getPartitionNumber());
            readsRDD = singleReadsKeyVal.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
            LOG.info("["+this.getClass().getName()+"] :: Repartition with sort");
        }

        // No Sort with no partitioning
        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
            LOG.info("["+this.getClass().getName()+"] :: No sort and no partitioning");
            readsRDD = singleReadsKeyVal.values();
        }

        // No Sort with partitioning
        else {
            LOG.info("["+this.getClass().getName()+"] :: No sort with partitioning");
            int numPartitions = singleReadsKeyVal.partitions().size();

            /*
             * As in previous cases, the coalesce operation is not suitable
             * if we want to achieve the maximum speedup, so, repartition
             * is used.
             */
            if ((numPartitions) <= options.getPartitionNumber()) {
                LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
            }
            else {
                LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
            }

            readsRDD = singleReadsKeyVal
                    .repartition(options.getPartitionNumber())
                    .values();
            //.persist(StorageLevel.MEMORY_ONLY());

        }

        long endTime = System.nanoTime();
        LOG.info("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
        LOG.info("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");

        //readsRDD.persist(StorageLevel.MEMORY_ONLY());

        return readsRDD;
    }

    /**
     * Method to perform and handle the paired reads sorting
     *
     * *Changes:
     *  + getInputPath to getInputFastqPath.
     *
     * @return A JavaRDD containing grouped reads from the paired FASTQ files
     */
    private JavaRDD<Tuple2<String, String>> handlePairedReadsSorting() {
        JavaRDD<Tuple2<String, String>> readsRDD = null;

        long startTime = System.nanoTime();

        LOG.info("["+this.getClass().getName()+"] ::Not sorting in HDFS. Timing: " + startTime);

        // Read the two FASTQ files from HDFS using the loadFastq method. After that, a Spark join operation is performed
        JavaPairRDD<Long, String> datasetTmp1 = loadFastq(this.ctx, options.getInputFastqPath());
        JavaPairRDD<Long, String> datasetTmp2 = loadFastq(this.ctx, options.getInputFastqPath2());
        JavaPairRDD<Long, Tuple2<String, String>> pairedReadsRDD = datasetTmp1.join(datasetTmp2);

        datasetTmp1.unpersist();
        datasetTmp2.unpersist();

        // Sort in memory with no partitioning
        if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
            readsRDD = pairedReadsRDD.sortByKey().values();
            LOG.info("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
        }

        // Sort in memory with partitioning
        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
            pairedReadsRDD = pairedReadsRDD.repartition(options.getPartitionNumber());
            readsRDD = pairedReadsRDD.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
            LOG.info("["+this.getClass().getName()+"] :: Repartition with sort");
        }

        // No Sort with no partitioning
        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
            LOG.info("["+this.getClass().getName()+"] :: No sort and no partitioning");
        }

        // No Sort with partitioning
        else {
            LOG.info("["+this.getClass().getName()+"] :: No sort with partitioning");
            int numPartitions = pairedReadsRDD.partitions().size();

            /*
             * As in previous cases, the coalesce operation is not suitable
             * if we want to achieve the maximum speedup, so, repartition
             * is used.
             */
            if ((numPartitions) <= options.getPartitionNumber()) {
                LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
            }
            else {
                LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
            }

            readsRDD = pairedReadsRDD
                    .repartition(options.getPartitionNumber())
                    .values();
            //.persist(StorageLevel.MEMORY_ONLY());
        }

        long endTime = System.nanoTime();

        LOG.info("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
        LOG.info("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");
        //readsRDD.persist(StorageLevel.MEMORY_ONLY());

        return readsRDD;
    }

    /**
     * Procedure to perform the alignment using paired reads
     *
     * *Changes:
     *  + Change method name.
     *  + Change Bwa object to AlignmentToolWrapper object.
     *
     * @param alignmentToolWrapper The alignment tool object to use
     * @param readsRDD The RDD containing the paired reads
     * @return A list of strings containing the resulting sam files where the output alignments are stored
     */
    private List<String> MapPairedMethod(AlignmentToolWrapper alignmentToolWrapper, JavaRDD<Tuple2<String, String>> readsRDD) {
        // The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
        return readsRDD
                .mapPartitionsWithIndex(new PairedAlignmentMethod(readsRDD.context(), alignmentToolWrapper), true)
                .collect();
    }

    /**
     * Procedure to perform the alignment using single reads
     *
     * *Changes:
     *  + Change method name.
     *  + Change Bwa object to AlignmentToolWrapper object.
     *
     * @param alignmentToolWrapper The alignment tool object to use
     * @param readsRDD The RDD containing the paired         reads
     * @return A list of strings containing the resulting sam files where the output alignments are stored
     */
    private List<String> MapSingleMethod(AlignmentToolWrapper alignmentToolWrapper, JavaRDD<String> readsRDD) {
        // The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
        return readsRDD
                .mapPartitionsWithIndex(new SingleAlignmentMethod(readsRDD.context(), alignmentToolWrapper), true)
                .collect();
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
     * TODO: Method of "merge by sample".
     *
     * @brief This function runs BWA with the input data selected and with the options also selected
     *     by the user.
     */
    public void runAlignment() {

        LOG.info("["+this.getClass().getName()+"] :: Starting Alignment");

        AlignmentToolWrapper alignmentToolWrapper = getAlignmentTool(this.options.getAlignmentTool());

        // Returned list of mapping results files' paths.
        List<String> returnedValues;

        if (this.options.getSequencingMode().equals(SequencingMode.PAIREND)) {
            JavaRDD<Tuple2<String, String>> readsRDD = handlePairedReadsSorting();
            returnedValues = MapPairedMethod(alignmentToolWrapper, readsRDD);
        }
        else {
            JavaRDD<String> readsRDD = handleSingleReadsSorting();
            returnedValues = MapSingleMethod(alignmentToolWrapper, readsRDD);
        }

        // In the case of use a reducer the final output has to be stored in just one file
        if(false){
        //if(this.options.isMergeOutputSamBySample()) {
            try {
                FileSystem fs = FileSystem.get(this.conf);

                Path finalHdfsOutputFile = new Path(this.options.getSamOutputHdfsDir() + "/FullOutput.sam");
                FSDataOutputStream outputFinalStream = fs.create(finalHdfsOutputFile, true);

                // We iterate over the resulting files in HDFS and agregate them into only one file.
                for (int i = 0; i < returnedValues.size(); i++) {
                    LOG.info("AlignmentProcess :: Returned file ::" + returnedValues.get(i));
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(returnedValues.get(i)))));

                    String line;
                    line = br.readLine();

                    while (line != null) {
                        if (i == 0 || !line.startsWith("@")) {
                            //outputFinalStream.writeBytes(line+"\n");
                            outputFinalStream.write((line + "\n").getBytes());
                        }

                        line = br.readLine();
                    }
                    br.close();

                    fs.delete(new Path(returnedValues.get(i)), true);
                }

                outputFinalStream.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error(e.toString());
            }
        }
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

    /**
     * Procedure to initiate the AlignmentProcess configuration parameters
     */
    public void initProcess() {
        //If ctx is null, this procedure is being called from the Linux console with Spark
        if (this.ctx == null) {

            String sorting;

            //Check for the options to perform the sort reads
            if (options.isSortFastqReads()) {
                sorting = "SortSpark";
            }
            else if (options.isSortFastqReadsHdfs()) {
                sorting = "SortHDFS";
            }
            else {
                sorting = "NoSort";
            }

            //The application name is set
            this.sparkConf = new SparkConf().setAppName("AlignmentProcess"
                    + options.getInputFastqPath().split("/")[options.getInputFastqPath().split("/").length - 1]
                    + "-"
                    + options.getPartitionNumber()
                    + "-"
                    + sorting);

            //The ctx is created from scratch
            this.ctx = new JavaSparkContext(this.sparkConf);

        }
        //Otherwise, the procedure is being called from the Spark shell
        else {

            this.sparkConf = this.ctx.getConf();
        }

        //The Hadoop configuration is obtained
        this.conf = this.ctx.hadoopConfiguration();

        //The block size
        this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);

        createOutputFolder();
        setTotalInputLength();

        //ContextCleaner cleaner = this.ctx.sc().cleaner().get();
    }
}
