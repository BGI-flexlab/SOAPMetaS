package org.bgi.flexlab.metas;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;

import java.util.ArrayList;

/**
 * ClassName: MetasOptions
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class MetasOptions {

    private static final Log LOG = LogFactory.getLog(MetasOptions.class);

    // Running mode group.
    private ProfilingAnalysisMode  profilingAnalysisMode = ProfilingAnalysisMode.PROFILE;
    private SequencingMode sequencingMode;
    private ProfilingAnalysisLevel profilingAnalysisLevel = ProfilingAnalysisLevel.SPECIES;

    // Alignment process arguments group.
    private String alignmentTool = "bowtie";

    private String alignmentIndexPath = "";
    private boolean alignmentShortIndex = true;

    private String inputFastqPath;
    private String inputFastqPath2 = null;
    private boolean singleSample;

    private String samOutputHdfsDir;
    private String alignmentTmpDir;

    private boolean sortFastqReads = false;
    private boolean sortFastqReadsHdfs 	= false;
    private boolean mergeOutputSamBySample = false;

    private String readGroupID = null;
    private ArrayList<String> readGroup = null;
    private String extraAlignmentArguments = "";

    private String multiSampleList; // Fastq InputFormat arguments and configures group.
    private int numPartitionEachSample = 10;// Partition number of each sample. The final partition number is partNumEachSample*NumberOfSample

    // Recalibration arguments group.
    private String gcBiasCorrectionModelType = "builtin";
    private String gcBiasCoefficientsTrainingOutput; // training coefficient output
    private String gcBiasCoefficientsFilePath; // model coefficient input
    private boolean gcBiasTrainingMode = false;
    private int scanWindowSize = 100;
    private boolean doGCBiasRecalibration = false;

    private String gcBiasModelTrainRefFasta;

    private boolean doInsRecalibration;

    // Profiling process arguments group.
    private String  profilingPipeline = "comg";
    private String referenceMatrixFilePath;
    private String speciesGenomeGCFilePath;
    private int insertSize = 300;
    private int readLength = 100;
    private boolean doIdentityFiltering = false;
    private double minIdentity = 0;
    private String profilingTmpDir;
    private String profilingOutputHdfsDir;


    //private String optionsAbbr = "\t\t[other options] --seqmod <pe|se> [--anamod <name>] [--pipemod <name>] [--analev <name>]\n" +
    //        "\t\t[-e/--extra-arg <\"AlignmentTool arguments\">] [--min-identity <float>] [--insert <int>]\n" +
    //        "\t\t[--stage (not support now)] [--merge-sam-sample (not support now)]" +
    //        "\t\t[--large-index] -x/--index <bowtie index> -i/--mulit-sample-list <FastqMultiSampleList>\n" +
    //        "\t\t-r/--reference <reference matrix> -o/--output <output dir> [--tmp <temp dir>]\n";

    private Options options = null;

    /**
     * Constructor
     */
    public MetasOptions() {
    }

    /**
     * Constructor
     * @param args Arguments from command line.
     */
    public MetasOptions(String[] args){
        this.optionsInitiate();
        this.optionsParse(args);
    }

    /**
     * Initiate all available options.
     */
    private void optionsInitiate(){

        this.options = new Options();
        /*
        Alignment Process Parameters group.
          */
        this.options.addOption("a", "align-tool", true,
                "Alignment tool. Now only support Bowtie2. SparkBWA is not integrated in as it doesn't " +
                        "support multiple-sample mode. We implemented JNI to achieve the integration.");

        Option alignmentIndex = new Option("x", "index", true,
                "Alignment tool index path. Refer to Bowtie2 manual.");
        alignmentIndex.setArgName("PATH-PREFIX");
        alignmentIndex.setRequired(true);
        this.options.addOption(alignmentIndex);

        this.options.addOption(null, "large-index", false,
                "Bowtie2 large index mode.");
        this.options.addOption("e", "extra-arg", true,
                "Other parameters for Bowtie2. Please refer to Bowtie2 manual.");

        Option partitionPerSam = new Option("n", "partition-per-sam", true,
                "Partition number of each sample. Default: 10\n" +
                        "The real partition number for Spark partitioner is (sampleNumber * partition-per-sam)." +
                        "For example, if you have 10 samples and set the para to 5, the RDD will be split to 50 partitions.");
        partitionPerSam.setArgName("INT");
        partitionPerSam.setType(Integer.class);
        this.options.addOption(partitionPerSam);

        this.options.addOption(null, "merge-sam-sample", false,
                "Switch argument. SAM file generated by alignment tool will be merged by sample if set." +
                        "Note that the process may slow down with this arg. Note: Merging will slow down the whole process.");

        OptionGroup inputOpt = new OptionGroup();
        Option multiFqSampleListOpt = new Option("i", "multi-sample-list", true,
                "Input file of multi sample fastq path list. File format:\n" +
                        "\t\tReadGroupID(SAM RG:Z:)\tread1_path\tread2_path (header line not included)\n" +
                        "\t\tERR0000001\t/path/to/read_1.fq\t[/path/to/read_2.fq]\n" +
                        "\t\t...");
        multiFqSampleListOpt.setArgName("FILE");
        Option singleFqSampleOpt = new Option("1", "fastq1", true,
                "Comma-seperated Fastq file. File paseed by this argument is treated as single sample, " +
                        "and this arg is mutually exclusive with --multi-sample-list(support single sample).");
        singleFqSampleOpt.setArgName("FILE");
        inputOpt.addOption(multiFqSampleListOpt).addOption(singleFqSampleOpt).setRequired(true);
        this.options.addOptionGroup(inputOpt);

        Option fastq2 = new Option("2", "fastq2", true,
                "Mate fastq file of pair-end sequence (use with --fastq1).");
        fastq2.setArgName("FILE");
        this.options.addOption(fastq2);


        /*
        Recalibration Process arguments group.
         */
        this.options.addOption(null, "gc-cali", false,
                "Switch for GC bias recalibration in profiling process. Recalibration will " +
                        "be done if set, or the corrected read number in profiling result will be equal " +
                        "to raw read number. Note that \"--spe-gc\" (species genome gc information) must be set.");
        // Species genome GC table
        Option speciesGC = new Option("g", "spe-gc", true,
                "Genome GC rate of each species included in reference matrix file. File format: \n" +
                        "\t\ts__Genusname_speciesname\t<float> (header line not included)\n" +
                        "\t\ts__Escherichia_coli\t0.506\n");
        speciesGC.setArgName("FILE");
        this.options.addOption(speciesGC);

        // Species genome sequence fastq
        Option speciesGenome = new Option(null, "spe-fa", true,
                "Genome sequence of reference species used in training process of GC bias correction model. " +
                        "The file is necessary merely for training process, and is the exact ref used in alignment process.");
        speciesGenome.setArgName("FILE");
        this.options.addOption(speciesGenome);

        this.options.addOption(null, "gc-model-type", true,
                "Statistical model used for GC bias correction. Now merely support builtin model.");
        this.options.addOption(null, "gc-model-train", false,
                "Switch for gc training process. The process will run for training if set," +
                        "and this means no profiling process.");

        Option gcTrainOut = new Option(null, "gc-train-out", true,
                "Output json format file of the training result of GC bias correction model." +
                        "We use com.google.gson.stream.JsonWriter for file writing.");
        gcTrainOut.setArgName("FILE");
        this.options.addOption(gcTrainOut);

        Option gcModelFile = new Option(null, "gc-model-file", true,
                "Input gc model coefficients file, users may train their own model for data " +
                        "originated from the same sequencing platform, once and for all.");
        gcModelFile.setArgName("FILE");
        this.options.addOption(gcModelFile);

        Option scanWindowSize = new Option(null, "gc-window-size", true,
                "The size of scanning windows on sequence used for GC calculation. Default: 100");
        scanWindowSize.setArgName("INT");
        scanWindowSize.setType(Integer.class);
        this.options.addOption(scanWindowSize);

        this.options.addOption(null, "ins-cali", false,
                "Switch for insert-size recalibration in profiling process. If set, a Gaussian curve " +
                        "will be used to fit the insert-size distribution of sample data, and we will use " +
                        "the 2-sigma range for insert-size filter. Note: Insert size is exactly the term in " +
                        "PE genome sequencing. Since the insert size may fluctuate around the standard value, " +
                        "so the filtering is somehow necessary.");

        /*
        Filtering arguments group.
         */
        Option insertSize = new Option(null, "insert-size", true,
                "Standard insert size of paired-end sequencing data. Default: 300");
        insertSize.setArgName("INT");
        insertSize.setType(Integer.class);
        this.options.addOption(insertSize);

        this.options.addOption(null, "iden-filt", false,
                "Switch for identity filtering of SAMRecords in profiling process. The filtering " +
                        "will be implemented if set.");

        Option minIdentity = new Option(null, "min-identity", true,
                "Identity threshold for filtering of SAMRecords in profiling process. Default: 0");
        minIdentity.setArgName("Double");
        minIdentity.setType(Double.class);
        this.options.addOption(minIdentity);

        /*
        Profiling analysis arguments group.
         */
        Option sequenceMode = new Option(null, "seq-mode", true,
                "Sequence data type. \"pe\" for pair-end, \"se\" for single-end. This argument affect merely profiling process.");
        sequenceMode.setArgName("MODE");
        sequenceMode.setRequired(true);
        this.options.addOption(sequenceMode);

        Option analysisMode = new Option(null, "ana-mode", true,
                "Analysis mode for profiling.\n" +
                        "\t\tprofile: basic mode with fragment number(also corrected) and relative abundance.\n" +
                        "\t\tevaluation: name list of reads mapped to each cluster.");
        analysisMode.setArgName("MODE");
        this.options.addOption(analysisMode);

        Option analysisLevel = new Option(null, "ana-lev", true,
                "Output level of profiling. Options: species, markers. \"species\" level means the " +
                        "result is profiling of species. \"markers\" means profiling of marker gene (genes of reference).");
        analysisLevel.setArgName("MODE");
        this.options.addOption(analysisLevel);

        Option profilingPipe = new Option(null, "prof-pipe", true,
                "Pipeline of profiling. Only cOMG is supported currently. Please refer " +
                        "to doi:10.1038/nbt.2942 for more information.");
        profilingPipe.setArgName("MODE");
        this.options.addOption(profilingPipe);


        /*
        IO files/directory arguments.
        TODO: Add options for specific directory of sam/profiling result and temp directory of each process.
        TODO: Reference information include genus, is it possible to add genus level profiling?
         */
        Option referenceMatrix = new Option("r", "ref-matrix", true,
                "Reference information matrix file of marker gene. File format:\n" +
                        "\t\tgeneID\tgeneName\tgeneLength\tgeneGC\tspecies[\tgenus\tphylum] (header line not included)\n" +
                        "\t\t1\tT2D-6A_GL0083352\t88230\ts__unclassed[\tg__unclassed\tp__unclassed]\n" +
                        "..." +
                        "\t\t59\t585054.EFER_0542\t21669\ts__Escherichia__coli[\tg__Escherichia\tp__Proteobacteria]\n" +
                        "...");
        referenceMatrix.setRequired(true);
        referenceMatrix.setArgName("FILE");
        this.options.addOption(referenceMatrix);

        Option outputDir = new Option("o", "output-dir", true,
                "Output directory of SAM result and Profiling result. Note that the \"alignment\" and " +
                        "\"profiling\" subdirectory will be created.");
        outputDir.setArgName("PATH");
        outputDir.setRequired(true);
        this.options.addOption(outputDir);

        Option tmpDir = new Option(null, "tmp-dir", true,
                "Temp directory for intermediate temp files.");
        tmpDir.setArgName("PATH");
        this.options.addOption(tmpDir);
        //this.options.addOption(null, "align-out-dir", true, "Output directory of profiling results. Default is output-dir/alignment.")
        //this.options.addOption(null, "prof-out-dir", true, "Output directory of profiling results. Default is output-dir/profiling.")

        /*
        Supplementary arguments. Not useful in current version.

        Note: Arguments "f/hdfs" and "k/spark" is copied from SparkBWA.

        TODO: verbose/version arguments?
         */
        //this.options.addOption(null, "read-length", true, "Standard read length (theoretical value from sequencing) of the data. Default: 100");
        //this.options.addOption("f", "hdfs", false, "The HDFS is used to perform the input FASTQ reads sort.");
        //this.options.addOption("k", "spark", false, "the Spark engine is used to perform the input FASTQ reads sort.");

        this.options.addOption("h", "help", false, "Show help information.");

    }

    private void optionsParse(String[] args){

        if (args.length < 1){
            usage();
            System.exit(0);
        }
        StringBuilder allArgs = new StringBuilder();
        for (int i=0; i<args.length; i++){
            allArgs.append(args[i]);
        }
        LOG.info("[MetasOptions] :: Received arguments: " + allArgs.toString());

        CommandLineParser parser = new BasicParser();
        CommandLine commandLine;

        try {
            commandLine = parser.parse(this.options, args, true);

            if (commandLine.hasOption('h') || commandLine.hasOption("help")){
                usage();
                System.exit(0);
            }
            /*
            Alignment process args parsing.
             */
            if (commandLine.hasOption('a') || commandLine.hasOption("align-tool")) {
                this.alignmentTool = commandLine.getOptionValue('a');
                assert this.alignmentTool.equals("bowtie"): "Alignment tool not support.";
            }

            this.alignmentIndexPath = commandLine.getOptionValue('x');
            if (commandLine.hasOption("large-index")){
                this.alignmentShortIndex = false;
            }

            this.extraAlignmentArguments = commandLine.getOptionValue('e', "");
            this.numPartitionEachSample = Integer.parseInt(commandLine.getOptionValue('n', "10"));

            if (commandLine.hasOption("merge-sam-sample")){
                this.mergeOutputSamBySample = true;
            }

            if (commandLine.hasOption('i') || commandLine.hasOption("multi-sample-list")){
                this.multiSampleList = commandLine.getOptionValue('i');
            } else if (commandLine.hasOption('1') || commandLine.hasOption("fastq1")){
                LOG.warn("[MetasOptions] :: Single sample mode. multi-sample-list is recommended.");
                this.inputFastqPath = commandLine.getOptionValue('1');
                this.singleSample = true;
                if (commandLine.hasOption('2') || commandLine.hasOption("fastq2")){
                    this.inputFastqPath2 = commandLine.getOptionValue('2');
                }
            }

            /*
            Recalibration process arguments parsing.
             */
            if (commandLine.hasOption("gc-cali")){
                this.doGCBiasRecalibration = true;
                this.speciesGenomeGCFilePath = commandLine.getOptionValue('g', null); //Species genome gc
                this.gcBiasCorrectionModelType = commandLine.getOptionValue("gc-model-type", "builtin");
                this.gcBiasCoefficientsFilePath = commandLine.getOptionValue("gc-model-file", null);
                assert this.gcBiasCorrectionModelType.equals("builtin"): "GC bias correction model not support.";
                assert this.speciesGenomeGCFilePath != null: "Please provide species genome GC file.";
            }

            if (commandLine.hasOption("gc-model-train")){
                this.gcBiasTrainingMode = true;
                this.gcBiasCoefficientsTrainingOutput = commandLine.getOptionValue("gc-train-out", null);
                this.scanWindowSize = Integer.parseInt(commandLine.getOptionValue("gc-window-size", "100"));
                this.gcBiasModelTrainRefFasta = commandLine.getOptionValue("spe-fq", null);
                assert  this.gcBiasModelTrainRefFasta != null: "Please provide species genome sequence fasta file.";
            }

            if (commandLine.hasOption("ins-cali")){
                this.doInsRecalibration = true;
            }

            /*
            Filtering arguments parsing.
             */
            this.insertSize = Integer.parseInt(commandLine.getOptionValue("insert-size", "300"));
            this.minIdentity = Double.parseDouble(commandLine.getOptionValue("min-identity", "0.3"));
            if (commandLine.hasOption("iden-filt")){
                this.doIdentityFiltering = true;
            }

            /*
            Profiling analysis arguments parsing.
             */
            this.sequencingMode = SequencingMode.valueOf(commandLine.getOptionValue("seq-mode"));
            this.profilingAnalysisMode = ProfilingAnalysisMode.valueOf(commandLine.getOptionValue("ana-mode", "profile"));
            this.profilingAnalysisLevel = ProfilingAnalysisLevel.valueOf(commandLine.getOptionValue("ana-lev", "species"));
            this.profilingPipeline = commandLine.getOptionValue("prof-pipe", "comg");
            assert this.profilingPipeline.equals("comg"): "Profiling pipeline mode not supported.";

            /*
            IO arguments parsing.
             */
            this.referenceMatrixFilePath = commandLine.getOptionValue('r');
            assert commandLine.getOptionValue('o', null) != null: "Please set output directory.";
            this.profilingOutputHdfsDir = commandLine.getOptionValue('o') + "/profiling";
            this.samOutputHdfsDir = commandLine.getOptionValue('o') + "/alignment";

            this.alignmentTmpDir = commandLine.getOptionValue("tmp-dir", "/tmp/SOAPMetas") + "/alignment";
            this.profilingTmpDir = commandLine.getOptionValue("tmp-dir", "/tmp/SOAPMetas") + "/profiling";

        } catch (UnrecognizedOptionException e) {
            LOG.error("[MetasOptions] :: Unrecognized option." + e.toString());
            e.printStackTrace();
            this.usage();
            System.exit(1);
        } catch (MissingOptionException e) {
            LOG.error("[MetasOptions] :: Required option missing error." + e.toString());
            this.usage();
            System.exit(1);
        } catch (ParseException e){
            LOG.error("[MetasOptions] :: Arguments parsing error." + e.toString());
            e.printStackTrace();
            this.usage();
            System.exit(1);
        }

    }

    private void usage(){

        String submit = "spark-submit [spark options] --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas-0.0.1.jar";
        String header = "SOAPMetas for metagenomic data analysis, include multi-sample distributed alignment and gene/species profiling.";
        String footer = "Author: heshixu@genomics.cn";

        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(150);
        helpFormatter.printHelp(submit, header, this.options, footer, true);

    }

    public String getReferenceMatrixFilePath(){
        return this.referenceMatrixFilePath;
    }

    public String getSpeciesGenomeGCFilePath() {
        return speciesGenomeGCFilePath;
    }

    /*
    Profiling analysis arguments group.
     */

    public ProfilingAnalysisMode getProfilingAnalysisMode(){
        return this.profilingAnalysisMode;
    }

    public SequencingMode getSequencingMode(){
        return this.sequencingMode;
    }

    public String getProfilingPipeline(){
        return this.profilingPipeline;
    }

    public ProfilingAnalysisLevel getProfilingAnalysisLevel(){
        return this.profilingAnalysisLevel;
    }

    public String getProfilingTmpDir() {
        return profilingTmpDir;
    }

    public String getProfilingOutputHdfsDir() {
        return profilingOutputHdfsDir;
    }

    /*
    Filteration related method group.
     */

    public boolean isDoInsRecalibration(){
        return this.doInsRecalibration;
    }

    /**
     * TODO: 如果有没有设定值就返回默认值，如果有设定值就返回设定值
     * @return
     */
    public int getInsertSize(){
        return this.insertSize;
    }

    public double getMinIdentity(){
        return this.minIdentity;
    }

    public boolean isDoIdentityFiltering(){
        return this.doIdentityFiltering;
    }

    public int getReadLength(){
        return this.readLength;
    }

    /*
    GCBias recalibration related method group
     */

    public int getScanWindowSize() {
        return scanWindowSize;
    }

    public String getGcBiasCoefficientsFilePath(){
        return this.gcBiasCoefficientsFilePath;
    }

    public String getGcBiasTrainingOutputFile(){
        return this.gcBiasCoefficientsTrainingOutput;
    }

    public boolean isGCBiasTrainingMode(){
        return this.gcBiasTrainingMode;
    }

    public String getGcBiasCorrectionModelType(){
        return this.gcBiasCorrectionModelType;
    }

    public boolean isDoGcBiasRecalibration(){
        return this.doGCBiasRecalibration;
    }

    /*
    Alignment related method group.
     */

    public boolean isSingleSample() {
        return this.singleSample;
    }

    public String getInputFastqPath() {
        return this.inputFastqPath;
    }

    public String getInputFastqPath2(){
        if (this.inputFastqPath2 == null || this.inputFastqPath2.isEmpty()){
            return null;
        } else {
            return this.inputFastqPath2;
        }
    }

    public String getAlignmentIndexPath(){
        return this.alignmentIndexPath;
    }

    public String getSamOutputHdfsDir() {
        return this.samOutputHdfsDir;
    }

    public boolean isAlignmentShortIndex() {
        return this.alignmentShortIndex;
    }

    public int getPartitionNumber() {
        return Math.abs(this.numPartitionEachSample);
    }

    public boolean isSortFastqReads() {
        return this.sortFastqReads;
    }

    public boolean isMergeOutputSamBySample() {
        return this.mergeOutputSamBySample;
    }

    public String getAlignmentTool(){
        return this.alignmentTool;
    }

    public boolean isSortFastqReadsHdfs() {
        return this.sortFastqReadsHdfs;
    }

    public String getExtraAlignmentArguments(){
        return this.extraAlignmentArguments;
    }

    public String getAlignmentTmpDir() {
        return alignmentTmpDir;
    }

    /*
    Fastq InputFormat related arguments and configures group.
     */

    public String getMultiSampleList() {
        return multiSampleList;
    }

    public int getNumPartitionEachSample() {
        return Math.abs(numPartitionEachSample);
    }

    public ArrayList<String> getReadGroup() {
        return readGroup;
    }

    public String getReadGroupID() {
        return readGroupID;
    }

}
