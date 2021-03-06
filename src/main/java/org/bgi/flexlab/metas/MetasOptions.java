package org.bgi.flexlab.metas;

import org.apache.commons.cli.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bgi.flexlab.metas.util.ProfilingAnalysisLevel;
import org.bgi.flexlab.metas.util.ProfilingAnalysisMode;
import org.bgi.flexlab.metas.util.SequencingMode;

import java.io.Serializable;

/**
 * ClassName: MetasOptions
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class MetasOptions implements Serializable {

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(MetasOptions.class);

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
    private String alignmentTmpDir = null;

    private boolean sortFastqReads = false;
    private boolean sortFastqReadsHdfs 	= false;

    private String readGroupID;
    private String extraAlignmentArguments;

    private String multiSampleList;
    private int numPartitionEachSample = 10;// Partition number of each sample. The final partition number is partNumEachSample*NumberOfSample
    private int numPartEachSampleAlign = 10; // Partition number in alignment process
    private int numPartEachSampleProf = 10; // Partition number in profiling process

    //private String alignLog;

    // Recalibration arguments group.
    private String gcBiasRecaliModelType = "builtin";
    private String gcBiasModelOutput; // training coefficient output
    private String gcBiasModelInput; // model coefficient input
    private boolean gcBiasTrainingMode = false;
    private int scanWindowSize = 100;
    private boolean doGCBiasRecalibration = false;

    private String gcBiasTrainerRefFasta;
    private boolean doInsRecalibration;

    private String nlsControl;
    private String startValue;
    private boolean outputPoint = false;
    private String pointPath;

    // Profiling process arguments group.
    private String  profilingPipeline = "comg";
    private String SAMSampleList;
    private String referenceMatrixFilePath;
    private String speciesGenomeGCFilePath;
    private int insertSize = 300;
    private int readLength = 100;
    private boolean doIdentityFiltering = false;
    private double minIdentity = 0;
    private boolean doAlignLenFiltering = false;
    private int minAlignLength = 0;
    private int minMapQuallity = 5;

    //private String profilingTmpDir = null;
    private String profilingOutputHdfsDir;

    // MetaPhlAn algorithm arguments group
    private boolean doDisqm = true;
    private int statType = 8;
    private int totalNReads = 0;
    private boolean ignoreUnknown = true;
    private String outputFormat = "DEFAULT";
    private String mpaMarkersListFile;
    private String mpaTaxonomyListFile;
    private String mpaExcludeMarkersFile;

    // Process control arguments
    private boolean mergeSamBySample = false;
    private boolean doAlignment = true;
    private boolean doProfiling = true;

    private String hdfsOutputDir;
    private String driverTmpDir;
    private boolean isLocal = false;
    private boolean retainTemp = false;

    private String alignmentExeMemory = "2g";
    private String alignmentExeNumber = "3";
    private String profilingExeMemory = "512m";
    private String profilingExeNumber = "3";
    private String alignmentExeCores = "1";
    //private String alignmentTaskCpus = "1";
    private String profilingExeCores = "1";
    private String profilingTaskCpus = "1";

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
        Alignment Process Options group.
          */
        this.options.addOption("a", "align-tool", true,
                "Alignment tool. Now only support Bowtie2. SparkBWA is not integrated in as it doesn't " +
                        "support multiple-sample mode. We implemented JNI to achieve the integration.");

        Option alignmentIndex = new Option("x", "index", true,
                "The alignment index file prefix, utilized by bowtie2. Refer to Bowtie2 manual (http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml) for more information.");
        alignmentIndex.setArgName("PATH-PREFIX");
        this.options.addOption(alignmentIndex);

        this.options.addOption(null, "large-index", false,
                "Bowtie2 large index mode.");
        this.options.addOption("e", "extra-arg", true,
                "Other options for Bowtie2. Please refer to Bowtie2 manual (http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml) for more information. " +
                        "All options should be enclosed together with quotation marks \"\". Default: \"--very-sensitive --no-unal\"");

        Option partitionPerSam = new Option("n", "partition-per-sam", true,
                "Partition number of each sample (for alignment process only, thus we recommend using \"--npart-align\"). " +
                        "The real partition number for Spark partitioner is (sampleNumber * partition-per-sam). " +
                        "For example, if you have 10 samples and set the para to 5, the RDD will be split to 50 partitions. " +
                        " Increase the number properly may improve the performance. Default: 10");
        partitionPerSam.setArgName("INT");
        Option nPartAlign = new Option(null, "npart-align", true, "Partition number of each sample in alignment process. Default is the value of \"-n\".");
        //Option nPartProf = new Option(null, "npart-prof", true, "Partition number of each sample in profiling process. Default is the value of \"-n\".");
        nPartAlign.setArgName("INT");
        //nPartProf.setArgName("INT");
        //partitionPerSam.setType(Integer.TYPE);
        this.options.addOption(partitionPerSam).addOption(nPartAlign);//.addOption(nPartProf);

        //this.options.addOption(Option.builder("n").longOpt("partition-per-sam")
        //        .desc("Partition number of each sample. Default: 10\n" +
        //        "The real partition number for Spark partitioner is (sampleNumber * partition-per-sam)." +
        //        "For example, if you have 10 samples and set the para to 5, the RDD will be split to 50 partitions.")
        //        .argName("INT")
        //        .required(false)
        //        .type(Integer.TYPE)
        //        .build());

        OptionGroup inputSampleGroup = new OptionGroup();
        Option multiFqSampleListOpt = new Option("i", "multi-fq-list", true,
                "Multiple-sample clean FASTQ files list, one line per sample. The option is exclusive to \"-s\". Note that ReadGroupID info is not considered in current version. File format (tab delimited):\n" +
                        "\t\tReadGroupID1 Sample(SMTag)1 /path/to/read1_1.fq [/path/to/read1_2.fq]\n" +
                        "\t\tReadGroupID1 Sample(SMTag)1 /path/to/read2_1.fq [/path/to/read2_2.fq]\n" +
                        "\t\t...");
        multiFqSampleListOpt.setArgName("FILE");
        //multiFqSampleListOpt.setRequired(true);
        Option multiSamSampleListOpt = new Option("s", "multi-sam-list", true,
                "Multiple-sample SAM file list, Note that ReadGroupID info is not considered in current version. the option is exclusive to \"-i\". File format (tab delimited):\n" +
                        "\t\tReadGroupID1 sample(SMTag)1 /path/to/sample1_part1.sam\n" +
                        "\t\tReadGroupID1 sample(SMTag)1 /path/to/sample1_part2.sam\n" +
                        "\t\tReadGroupID2 sample(SMTag)2 /path/to/sample2_part1.sam\n" +
                        "\t\t...");
        multiSamSampleListOpt.setArgName("FILE");
        inputSampleGroup.addOption(multiFqSampleListOpt).addOption(multiSamSampleListOpt).setRequired(true);
        this.options.addOptionGroup(inputSampleGroup);

        //Option singleFqSampleOpt = new Option("1", "fastq1", true,
        //        "Comma-seperated Fastq file. File paseed by this argument is treated as single sample, " +
        //                "and this arg is mutually exclusive with --multi-fq-list(support single sample).");
        //singleFqSampleOpt.setArgName("FILE");
        //Option fastq2 = new Option("2", "fastq2", true,
        //        "Mate fastq file of pair-end sequence (use with --fastq1). In single-end sequencing " +
        //                "mode, fastq2 will be treated as a independent file.");
        //fastq2.setArgName("FILE");

        /*
        Recalibration Process arguments group.
         */
        //this.options.addOption(null, "gc-cali", false,
        //        "Switch for GC bias recalibration in profiling process. Recalibration will " +
        //                "be done if set, or the recalibrated read number in profiling result will be equal " +
        //                "to raw read number. Note that \"--spe-gc\" (species genome gc information) must be set.");
        // Species genome GC table
        Option speciesGC = new Option("g", "spe-gc", true,
                "Genome information (genome length, genome GC) of each species in reference data. The file is used with \"--ana-lev species\" and \"--prof-pipe comg\". File format (tab delimited):\n" +
                        "\t\ts__Genusname_speciesname\t<Genome_Length>\t<Genome_GC_rate>\n" +
                        "\t\ts__Escherichia_coli\t4641652\t0.508\n");
        speciesGC.setArgName("FILE");
        this.options.addOption(speciesGC);

        // Species genome sequence fastq
        //Option speciesGenome = new Option(null, "spe-fa", true,
        //        "Genome sequence of reference species used in training process of GC bias recalibration model. " +
        //                "The file is necessary merely for training process, and is the exact ref used in alignment process.");
        //speciesGenome.setArgName("FILE");
        //this.options.addOption(speciesGenome);

        //this.options.addOption(null, "gc-model-type", true,
        //        "Statistical model used for GC bias recalibration. Now merely support builtin model.");
        //this.options.addOption(null, "gc-model-train", false,
        //        "Switch for gc training process. The process will run for training if set," +
        //                "and this means no profiling process.");
        //Option gcTrainOut = new Option(null, "gc-train-out", true,
        //        "Output json format file of the training result of GC bias recalibration model." +
        //                "We use com.google.gson.stream.JsonWriter for file writing.");
        //gcTrainOut.setArgName("FILE");
        //this.options.addOption(gcTrainOut);
        //Option gcModelFile = new Option(null, "gc-model-file", true,
        //        "Input gc model coefficients file, users may train their own model for data " +
        //                "originated from the same sequencing platform, once and for all.");
        //gcModelFile.setArgName("FILE");
        //this.options.addOption(gcModelFile);
        //Option scanWindowSize = new Option(null, "gc-window-size", true,
        //        "The size of scanning windows on sequence used for GC calculation. Default: 100");
        //scanWindowSize.setArgName("INT");
        ////scanWindowSize.setType(Integer.TYPE);
        //this.options.addOption(scanWindowSize);

        this.options.addOption(null, "ins-cali-train", false,
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
        //insertSize.setType(Integer.TYPE);
        this.options.addOption(insertSize);

        this.options.addOption(null, "iden-filt", false,
                "Whether to filter alignment results by identity in profiling process.");

        Option minIdentity = new Option(null, "min-identity", true,
                "The minimal alignment identity of reserved sequence. Default: 0.8");
        minIdentity.setArgName("Double");
        //minIdentity.setType(Double.TYPE);
        this.options.addOption(minIdentity);

        this.options.addOption(null, "len-filt", false,
                "Whether to filter alignment results by alignment length in profiling process.");
        Option minAlignLen = new Option(null, "min-align-len", true,
                "The minimal alignment length of reserved sequence. Default: 30");
        minAlignLen.setArgName("Double");
        this.options.addOption(minAlignLen);

        this.options.addOption(null, "no-disqm", false,
                "Switch for the procedure of disambiguating the quasi-markers. Refer to MetaPhlAn2 --avoid_disqm option for detail description.");
        this.options.addOption(null, "meph-abun-algorithm", false,
                "Algorithm used to compute abundance. Refer to MetaPhlAn2 --stat option for details.");

        Option minMapQ = new Option(null, "min-map-quality", true,
                "Minimal mapping quality threshold of the remained mapped reads. Default: 5");
        minMapQ.setArgName("Integer");
        this.options.addOption(minMapQ);


        /*
        Profiling analysis arguments group.
         */
        Option sequenceMode = new Option(null, "seq-mode", true,
                "Sequence data type. \"pe\" for pair-end, \"se\" for single-end.");
        sequenceMode.setArgName("MODE");
        sequenceMode.setRequired(false);
        this.options.addOption(sequenceMode);

        Option analysisMode = new Option(null, "ana-mode", true,
                "Analysis mode for profiling.\n" +
                        "\t\tprofile: basic mode with fragment number(also recalibrated) and relative abundance.\n" +
                        "\t\tevaluation: name list of reads mapped to each cluster.");
        analysisMode.setArgName("MODE");
        this.options.addOption(analysisMode);

        Option analysisLevel = new Option(null, "ana-lev", true,
                "The cluster level of output profiling result, including \"marker\" level for gene profiling and \"species\" level for species profiling." +
                        "The option work with \"--prof-pipe comg\". Default: species." + " Default: species");
        analysisLevel.setArgName("MODE");
        this.options.addOption(analysisLevel);

        Option profilingPipe = new Option(null, "prof-pipe", true,
                "Pipeline of profiling. Option: comg (doi:10.1038/nbt.2942), meph (MetaPhlAn2). Default: comg");
        profilingPipe.setArgName("MODE");
        this.options.addOption(profilingPipe);

        Option readsCount = new Option(null, "total-nreads", true,
                "The total number of reads. Used for CAMI format generation in MetaPhlAn2 2019 version. "+
                        "Note that for PE input you should add the count of each fastq file, because MetaPhlAn2 will process PE as SE.");
        readsCount.setArgName("INTEGER");
        this.options.addOption(readsCount);

        //this.options.addOption(null, "unknown-estimation", false,
        //        "Switch option. If set, the unmapped reads and reads mapped to unknown clade will be considered.");
        this.options.addOption(null, "output-format", true,
                "Output profiling file format. Options: CAMI, DETAILED, DEFAULT. Default: DEFAULT(legacy MetaPhlAn2 format).\n" +
                        "\t\tDEFAULT: clusterName\trel_abun\n" +
                        "\t\tDETAILED: clusterName\traw_read_count\trecali_read_count\tabundance\trel_abun\n" +
                        "\t\tCAMI: taxID\ttaxRank\ttaxPath\ttaxName\trel_abun\n");

        /*
        IO files/directory arguments.
        TODO: Add options for specific directory of sam/profiling result and temp directory of each process.
        TODO: Reference information include genus, is it possible to add genus level profiling?
         */
        Option referenceMatrix = new Option("r", "ref-matrix", true,
                "Reference information matrix file of marker gene. Including sequence length, species information of marker gene. File format (tab delimited):\n" +
                        "\t\tgeneID geneName geneLength geneGC species [genus phylum] (header line not included)\n" +
                        "\t\t1 T2D-6A_GL0083352 88230 s__unclassed [geneGC [g__unclassed p__unclassed]]\n" +
                        "..." +
                        "\t\t59 585054.EFER_0542 21669 s__Escherichia_coli [geneGC [g__Escherichia p__Proteobacteria]]\n" +
                        "...");
        referenceMatrix.setArgName("FILE");
        this.options.addOption(referenceMatrix);

        Option mpaMarkerList = new Option(null, "mpa-marker-list", true,
                "Marker information list for \"--prof-pipe meph\", extracted from MetaPhlAn2 database mpa_v20_m200.pkl. The file is in json format. User may generate the file using python3 json.dump(mpa_pkl[\"markers\"])");
        mpaMarkerList.setArgName("FILE");
        this.options.addOption(mpaMarkerList);
        Option mpaTaxonomyList = new Option(null, "mpa-taxon-list", true,
                "Taxonomy information list for \"--prof-pipe meph\", extracted from MetaPhlAn2 database mpa_v20_m200.pkl. The file is in json format. User may generate the file using python3 json.dump(mpa_pkl[\"taxonomy\"])");
        mpaTaxonomyList.setArgName("FILE");
        this.options.addOption(mpaTaxonomyList);
        Option excludeMarkers = new Option(null, "mpa-exclude-list", true,
                "Markers to exclude for \"--prof-pipe meph\", one marker gene per line. Please reference to variable markers_to_exclude/ingnore_markers in MetaPhlAn2 source code for more information.");
        excludeMarkers.setArgName("FILE");
        this.options.addOption(excludeMarkers);


        Option outputDir = new Option("o", "output-hdfs-dir", true,
                "Path to store alignment and profiling results, support both local path (file://) and HDFS (Hadoop Distributed File System) path. " +
                        "Note that the \"alignment\" and \"profiling\" subdirectory will be created.");
        outputDir.setArgName("PATH");
        outputDir.setRequired(true);
        this.options.addOption(outputDir);

        //Option tmpDirOpt = new Option(null, "driver-tmp-dir", true, "Local temp directory for intermediate files. Default is spark.local.dir or hadoop.tmp.dir, or /tmp/ if none is set. The temp directory is used to save alignment results, GC-model-related outputs and sample list file.");
        Option tmpDirOpt = new Option(null, "driver-tmp-dir", true,
                "Local temp directory on driver node. The directory is used for storing files generated by non-spark codes. Default is /tmp/SOAPMetaS_Driver_temp ." +
                        "The temp directory is used to save SAM sample list file.");
        tmpDirOpt.setArgName("PATH");
        this.options.addOption(tmpDirOpt);

        Option alnTmpDir = new Option(null, "align-tmp-dir", true,
                "Local temp directory on each executor node. The directory is used for storing intermediate tab5, sam and alignment log files. Default is /tmp/SOAPMetaS_Align_temp . " +
                        "Users should delete the directory manually if SOAPMetaS is finished or interrupted. For example: ssh <nodeIP> \"rm -r /tmp/SOAPMetaS_Align_temp\".");
        this.options.addOption(alnTmpDir);
        //this.options.addOption(null, "align-out-dir", true, "Output directory of profiling results. Default is output-dir/alignment.")
        //this.options.addOption(null, "prof-out-dir", true, "Output directory of profiling results. Default is output-dir/profiling.")

        Option local = new Option(null, "local", false,
                "Input fastq/SAM files and output SAM files are stored in local file system. By default, all paths are treated as HDFS format.");
        this.options.addOption(local);

        /*
        Processing stage control.
         */
        ////this.options.addOption(null, "merge-sam-sample", false,
        ////        "*(Not supported in current version) Switch option. SAM file generated by alignment " +
        ////                "tool will be merged by sample if set. Note that the process may slow down with " +
        ////                "this arg. Note: Merging will slow down the whole process.");

        this.options.addOption(null, "skip-alignment", false,
                "Switch option. If set, the alignment process will be skipped, and users must provide formatted SAM sample list (argument \"-s\").");
        this.options.addOption(null, "skip-profiling", false,
                "Switch option. If set, the profiling process will be skipped, and users must provide formatted FASTQ sample list (argument \"-i\").");
        this.options.addOption(null, "retain-temp", false, "Switch option. If set, interval temp files will be retained.");


        /*
        Resource Management
         */
        Option alnExeMem = new Option(null, "align-executor-memory", true,
                "Executor memory in alignment process. The value is related to the size of bowtie index file. Refer to the README in SOAPOMetas repo for more information. Example: 2g, 1600m, etc. Default: 2g");
        alnExeMem.setArgName("VALUE");
        Option alnExeNum = new Option(null, "align-executor-number", true,
                "Executor number in alignment process. Since alignment step doesn't support multi-tasks on one executor, user should adjust both this option and \"-n\" option for better performance. Refer to the README in SOAPOMetas repo for more information. Default: 3");
        alnExeNum.setArgName("INT");
        Option proExeMem = new Option(null, "prof-executor-memory", true,
                "Executor memory in profiling process. The value should be at least 512m or users may encounter some exception of memory. Example: 512m 1g. Default: 512m");
        proExeMem.setArgName("VALUE");
        Option proExeNum = new Option(null, "prof-executor-number", true,
                "Executor number in profiling process. Users may choose the value according to availible resource. Default: 3");
        proExeNum.setArgName("INT");
        this.options.addOption(alnExeMem).addOption(alnExeNum).addOption(proExeMem).addOption(proExeNum);

        Option alnExeCore = new Option(null, "align-executor-cores", true,
                "Executor core number in alignment process. The config in Spark is spark.executor.cores/--executor-cores , we make it more flexible. Default: 1");
        //Option alnTaskCpu = new Option(null, "align-task-cpus", true,
        //        "Task cpu (core) number in alignment process. The config in Spark is spark.task.cpu , we make it more flexible. Default: 1");
        Option proExeCore = new Option(null, "prof-executor-cores", true,
                "Executor core number in profiling process. The config in Spark is spark.executor.cores/--executor-cores , we make it more flexible. Default: 1");
        Option proTaskCpu = new Option(null, "prof-task-cpus", true,
                "Task cpu (core) number in profiling process. The config in Spark is spark.task.cpu , we make it more flexible.Default: 1");
        this.options.addOption(alnExeCore).addOption(proExeCore).addOption(proTaskCpu);

        /*
        Supplementary arguments. Not useful in current version.

        Note: Arguments "f/hdfs" and "k/spark" is copied from SparkBWA.

        TODO: verbose/version arguments?
         */
        //this.options.addOption(null, "read-length", true, "Standard read length (theoretical value from sequencing) of the data. Default: 100");
        //this.options.addOption("f", "hdfs", false, "The HDFS is used to perform the input FASTQ reads sort.");
        //this.options.addOption("k", "spark", false, "the Spark engine is used to perform the input FASTQ reads sort.");
        //this.options.addOption(null, "zz-control", true, "Options for controling nls " +
        //        "estimates. Refer to manual of nls.control in R for more help. This option might be " +
        //        "deprecated in future version. Users who want to control nls estimation in detail in RStudio " +
        //        "may use \"--zz-opoint\" and \"--zz-point-file\" to output data matrix of Normalized Cov, " +
        //        "Window GC (Read GC) and Genome GC. Note: use with \"--gc-model-train\". Default: " +
        //        "\"maxiter=50,tol=1e-05,minFactor=1/1024\".");
        //this.options.addOption(null, "zz-start-value", true, "Start values " +
        //        "used in NLS estimation. Refer to manual of nls in RStudio for more information. Default: " +
        //        "(\"startvalue\" in SOAPMetas_builtinModel.json). Note: use with \"--gc-model-train\".");
        //this.options.addOption(null, "zz-point", false, "Switch option. " +
        //        "If set, data matrix used in GC Bias model training process will be written into file. And " +
        //        "the process will be skipped. Note: use with \"--gc-model-train\".");
        //Option pointPath = new Option(null, "zz-point-file", true, "Path of file to " +
        //        "save data matrix used in training process. Note: use with \"--gc-model-train\".");
        //pointPath.setArgName("FILE");
        //this.options.addOption(pointPath);

        //Option alnLog = new Option(null, "aln-log-pre", true, "LOG file path prefix for alignment process.");
        //alnLog.setArgName("FILE");
        //this.options.addOption(alnLog);

        this.options.addOption("h", "help", false, "Show help information.");

    }

    private void optionsParse(String[] args){

        if (args.length < 1 || args[0].equals("-h") || args[0].equals("--help")){
            usage("");
            System.exit(0);
        }

        StringBuilder allArgs = new StringBuilder(8*args.length);
        for (int i=0; i<args.length; i++){
            allArgs.append(args[i]).append(' ');
        }
        LOG.info("[SOAPMetas::" + MetasOptions.class.getName() + "] Received arguments: " + allArgs.toString());
        allArgs = null;

        CommandLineParser parser = new BasicParser();
        CommandLine commandLine;

        try {
            commandLine = parser.parse(this.options, args, true);

            if (commandLine.hasOption('h') || commandLine.hasOption("help")){
                usage("");
                System.exit(0);
            }

            if (commandLine.hasOption("local")){
                this.isLocal = true;
            }
            /*
            Alignment process args parsing.
             */
            if (commandLine.hasOption('a') || commandLine.hasOption("align-tool")) {
                this.alignmentTool = commandLine.getOptionValue('a');
                assert this.alignmentTool.equals("bowtie"): "Alignment tool not support.";
            }

            this.alignmentIndexPath = commandLine.getOptionValue('x', null);
            if (commandLine.hasOption("large-index")){
                this.alignmentShortIndex = false;
            }

            this.extraAlignmentArguments = commandLine.getOptionValue('e', "--very-sensitive --no-unal");
            this.numPartitionEachSample = Integer.parseInt(commandLine.getOptionValue('n', "10"));

            if (commandLine.hasOption("npart-align")) {
                this.numPartEachSampleAlign = Integer.parseInt(commandLine.getOptionValue("npart-align"));
            } else {
                this.numPartEachSampleAlign = this.numPartitionEachSample;
            }
            
            //if (commandLine.hasOption("npart-prof")) {
            //    this.numPartEachSampleProf = Integer.parseInt(commandLine.getOptionValue("npart-prof"));
            //} else {
            //    this.numPartEachSampleProf = this.numPartitionEachSample;
            //}
            

            if (commandLine.hasOption("merge-sam-sample")){
                this.mergeSamBySample = true;
            }

            //if (commandLine.hasOption('i') || commandLine.hasOption("multi-fq-list")){
            //    this.multiSampleList = commandLine.getOptionValue('i');
            //} else if (commandLine.hasOption('1') || commandLine.hasOption("fastq1")){
            //    LOG.warn("[SOAPMetas::" + MetasOptions.class.getName() + "] Single sample mode. multi-fq-list is
            //    recommended
            //    .");
            //    this.inputFastqPath = commandLine.getOptionValue('1');
            //    this.singleSample = true;
            //    if (commandLine.hasOption('2') || commandLine.hasOption("fastq2")){
            //        this.inputFastqPath2 = commandLine.getOptionValue('2');
            //    }
            //}

            this.multiSampleList = commandLine.getOptionValue('i', null);
            this.SAMSampleList = commandLine.getOptionValue('s', null);
            this.speciesGenomeGCFilePath = commandLine.getOptionValue('g', null); //Species genome gc

            /*
            IO arguments parsing.
             */
            this.referenceMatrixFilePath = commandLine.getOptionValue('r', null);
            this.mpaMarkersListFile = commandLine.getOptionValue("mpa-marker-list", null);
            this.mpaTaxonomyListFile = commandLine.getOptionValue("mpa-taxon-list", null);
            this.mpaExcludeMarkersFile = commandLine.getOptionValue("mpa-exclude-list", null);

            this.hdfsOutputDir = commandLine.getOptionValue('o', null);
            if (this.hdfsOutputDir == null) {
                throw new MissingOptionException("Missing output directory option.");
            }
            if (this.isLocal && !this.hdfsOutputDir.startsWith("file://")){
                this.hdfsOutputDir = "file://" + this.hdfsOutputDir;
            }

            this.profilingOutputHdfsDir = this.hdfsOutputDir + "/profiling/";
            this.samOutputHdfsDir = this.hdfsOutputDir + "/alignment/";

            this.driverTmpDir = commandLine.getOptionValue("driver-tmp-dir", "/tmp/SOAPMetaS_Driver_temp");
            this.alignmentTmpDir = commandLine.getOptionValue("align-tmp-dir", "/tmp/SOAPMetaS_Align_temp");

            //this.alignLog = commandLine.getOptionValue("aln-log-pre", this.samOutputHdfsDir + "/SOAPMetas_alignmentLOG");

            /*
            Recalibration process arguments parsing.
             */
            if (commandLine.hasOption("gc-cali")){
                this.doGCBiasRecalibration = true;
                this.gcBiasRecaliModelType = commandLine.getOptionValue("gc-model-type", "builtin");
                this.gcBiasModelInput = commandLine.getOptionValue("gc-model-file", null);
                if (!this.gcBiasRecaliModelType.equals("builtin")){
                    throw new UnrecognizedOptionException("GC bias recalibration model not support in current version.");
                }
                if (this.speciesGenomeGCFilePath == null) {
                    throw new MissingArgumentException("GC-recalibration is set, please provide species genome information file.");
                }
            }

            if (commandLine.hasOption("gc-model-train")){
                this.gcBiasTrainingMode = true;
                this.gcBiasModelOutput = commandLine.getOptionValue("gc-train-out", this.driverTmpDir + "./SOAPMetas_modelTrainResult.json");
                this.scanWindowSize = Integer.parseInt(commandLine.getOptionValue("gc-window-size", "100"));
                this.gcBiasTrainerRefFasta = commandLine.getOptionValue("spe-fa", null);
                if (this.gcBiasTrainerRefFasta == null){
                    throw new MissingArgumentException("Please provide species genome sequence fasta file.");
                }
                this.nlsControl = commandLine.getOptionValue("zz-control", "maxiter=50,tol=1e-05,minFactor=1/1024");
                this.startValue = commandLine.getOptionValue("zz-start-value", "p1=0.812093,p2=49.34331,p3=8.886807,p4=6.829778,p5=0.2642576,p6=-0.005291173,p7=3.188492E-5,p8=-2.502158");

                if (commandLine.hasOption("zz-point")){
                    this.outputPoint = true;
                    this.pointPath = commandLine.getOptionValue("zz-point-file", this.driverTmpDir + "./SOAPMetas_nlsPointMatrix");
                }
            }

            if (commandLine.hasOption("ins-cali-train")){
                this.doInsRecalibration = true;
            }

            /*
            Filtering arguments parsing.
             */
            this.insertSize = Integer.parseInt(commandLine.getOptionValue("insert-size", "300"));
            this.minIdentity = Double.parseDouble(commandLine.getOptionValue("min-identity", "0.8"));
            this.minAlignLength = Integer.parseInt(commandLine.getOptionValue("min-align-len", "0"));
            if (commandLine.hasOption("iden-filt")){
                this.doIdentityFiltering = true;
            }
            if (commandLine.hasOption("len-filt")){
                this.doAlignLenFiltering = true;
            }
            if (commandLine.hasOption("no-disqm")) {
                this.doDisqm = false;
            }
            this.minMapQuallity = Integer.parseInt(commandLine.getOptionValue("min-map-quality", "5"));
            switch (commandLine.getOptionValue("meph-abun-algorithm", "tavg_g")) {
                case "avg_g": this.statType = 2; break;
                case "avg_l": this.statType = 4; break;
                //case "tavg_g": this.statType = 8; break;
                case "tavg_l": this.statType = 16; break;
                case "wavg_g": this.statType = 32; break;
                case "wavg_l": this.statType = 64; break;
                case "med": this.statType = 128; break;
                default: this.statType = 8;
            }

            /*
            Profiling analysis arguments parsing.
             */
            this.sequencingMode = SequencingMode.getValue(commandLine.getOptionValue("seq-mode", "se").toUpperCase());
            this.profilingAnalysisMode = ProfilingAnalysisMode.valueOf(commandLine.getOptionValue("ana-mode", "profile").toUpperCase());
            this.profilingAnalysisLevel = ProfilingAnalysisLevel.valueOf(commandLine.getOptionValue("ana-lev", "species").toUpperCase());
            this.profilingPipeline = commandLine.getOptionValue("prof-pipe", "comg").toLowerCase();
            if (this.profilingPipeline.toLowerCase().equals("meph") && this.sequencingMode.equals(SequencingMode.PAIREDEND)){
                this.sequencingMode = SequencingMode.SINGLEEND;
                LOG.warn("[SOAPMetas￿::" + MetasOptions.class.getName() + "] \"meph\" mode only supports Single-end (SE) sequence mode.");
            }
            this.totalNReads = Integer.parseInt(commandLine.getOptionValue("total-nreads", "0"));

            this.outputFormat = commandLine.getOptionValue("output-format", "DEFAULT").toUpperCase();
            if (this.outputFormat.equals("CAMI")) {
                LOG.warn("[SOAPMetas::" + MetasOptions.class.getName() + "] CAMI output format is only supported in \"mephn\" profiling pipeline. Here we reset prof-pipe to \"mephn\"");
                this.profilingPipeline = "mephn";
            }

            if (this.profilingPipeline.toLowerCase().equals("mephn")) {
                if (this.totalNReads == 0) {
                    LOG.warn("[SOAPMetas::" + MetasOptions.class.getName() + "] total-nreads must be provided in \"mephn\" prof-pipe. Here we reset its value to 1");
                    this.totalNReads = 1;
                }
                if (commandLine.hasOption("unknown-estimation")) {
                    this.ignoreUnknown = false;
                }
            }

            /*
            Process controlling arguments parsing/
             */
            if (commandLine.hasOption("skip-alignment")) {
                //LOG.trace("[SOAPMetas::" + MetasOptions.class.getName() + "] Option \"--skip-alignment\" is set. " +
                //        "Skip alignment process. SAM file list: " + this.SAMSampleList);
                this.doAlignment = false;
                if (this.SAMSampleList == null) {
                    throw new MissingOptionException("Missing --multi-sam-list option.");
                }
            } else {
                if (this.alignmentIndexPath == null) {
                    throw new MissingOptionException("Missing -x (--index) option.");
                }
                if (this.multiSampleList == null) {
                    throw new MissingOptionException("Missing -i (--multi-fq-list) option.");
                }
            }
            if (commandLine.hasOption("skip-profiling") || this.gcBiasTrainingMode){
                this.doProfiling = false;
            } else {
                if (this.referenceMatrixFilePath == null) {
                    throw new MissingOptionException("Missing -r (--ref-matrix) option.");
                }
                if (this.profilingPipeline.equals("meph") && (this.mpaTaxonomyListFile == null || this.mpaMarkersListFile == null)) {
                    throw new MissingOptionException("Missing --mpa-marker-list or --mpa-taxon-list option.");
                }
                if (this.profilingPipeline.equals("comg") &&
                        this.profilingAnalysisLevel.equals(ProfilingAnalysisLevel.SPECIES) &&
                        this.speciesGenomeGCFilePath == null){
                    throw new MissingArgumentException("Missing -g (--spe-gc) option. Please provide species genome information file in \"species\" analysis level.");
                }
            }

            if (commandLine.hasOption("retain-temp")){
                this.retainTemp = true;
            }

            this.alignmentExeMemory = commandLine.getOptionValue("align-executor-memory", "2g");
            this.alignmentExeNumber = commandLine.getOptionValue("align-executor-number", "3");
            this.profilingExeMemory = commandLine.getOptionValue("prof-executor-memory", "512m");
            this.profilingExeNumber = commandLine.getOptionValue("prof-executor-number", "3");
            this.alignmentExeCores = commandLine.getOptionValue("align-executor-cores", "1");
            //this.alignmentTaskCpus = commandLine.getOptionValue("align-task-cpus", "1");
            this.profilingExeCores = commandLine.getOptionValue("prof-executor-cores", "1");
            this.profilingTaskCpus = commandLine.getOptionValue("prof-task-cpus", "1");
            //if (!this.alignmentExeCores.equals(this.alignmentTaskCpus)) {
            //    LOG.warn("[SOAPMetas::" + MetasOptions.class.getName() + "] The alignment process only support one-task-one-executor, or the Bowtie2 library ccould cause error. Here we readjust \"align-executor-cores\" and \"align-task-cpus\" to the smaller one.");
            //    String cpuValue = Integer.toString(Math.max(Math.min(Integer.parseInt(this.alignmentExeCores), Integer.parseInt(this.alignmentTaskCpus)), 1));
            //}

        } catch (UnrecognizedOptionException e) {
            LOG.error("[SOAPMetas::" + MetasOptions.class.getName() + "] Unrecognized option." + e.toString());
            this.usage(e.toString());
            System.exit(1);
        } catch (MissingOptionException e) {
            LOG.error("[SOAPMetas::" + MetasOptions.class.getName() + "] Required option missing error." + e.toString());
            this.usage(e.toString());
            System.exit(1);
        } catch (MissingArgumentException e){
            LOG.error("[SOAPMetas::" + MetasOptions.class.getName() + "] Required argument missing error. " + e.toString());
            this.usage(e.toString());
            System.exit(1);
        } catch (ParseException e){
            LOG.error("[SOAPMetas::" + MetasOptions.class.getName() + "] Arguments parsing error. Please check input options." + e.toString());
            this.usage(e.toString());
            System.exit(1);
        }

    }

    private void usage(String errInfo){

        String submit = "spark-submit [spark options] --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas-0.0.1.jar";
        String header = "SOAPMetas for metagenomic data analysis, include multi-sample distributed alignment and gene/species profiling.";
        String footer = "Author: heshixu@genomics.cn\n" + errInfo;

        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(100);
        helpFormatter.printHelp(submit, header, this.options, footer, true);
    }

    public String getReferenceMatrixFilePath(){
        return this.referenceMatrixFilePath;
    }

    public String getSpeciesGenomeGCFilePath() {
        return speciesGenomeGCFilePath;
    }

    public String getMpaMarkersListFile() {
        return this.mpaMarkersListFile;
    }

    public String getMpaTaxonomyListFile() {
        return mpaTaxonomyListFile;
    }

    public String getMpaExcludeMarkersFile() {
        return mpaExcludeMarkersFile;
    }

    public String getDriverTmpDir() {
        return this.driverTmpDir;
    }

    /*
    Profiling analysis arguments group.
     */

    public String getSAMSampleList() {
        return SAMSampleList;
    }

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

    //public String getProfilingTmpDir() {
    //    return profilingTmpDir;
    //}

    public String getProfilingOutputHdfsDir() {
        return profilingOutputHdfsDir;
    }

    public int getTotalNReads() {
        return totalNReads;
    }

    public boolean ignoreUnknown(){
        return this.ignoreUnknown;
    }

    public String getOutputFormat() {
        return outputFormat;
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

    public int getMinAlignLength(){
        return this.minAlignLength;
    }

    public boolean isDoAlignLenFiltering(){
        return this.doAlignLenFiltering;
    }

    public int getMinMapQuallity() {
        return minMapQuallity;
    }

    public boolean isDoDisqm(){
        return this.doDisqm;
    }

    public int getStatType() {
        return this.statType;
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

    public String getGcBiasModelInput(){
        return this.gcBiasModelInput;
    }

    public String getGcBiasModelOutput(){
        return this.gcBiasModelOutput;
    }

    public String getGcBiasTrainerRefFasta() {
        return gcBiasTrainerRefFasta;
    }

    public boolean isGCBiasTrainingMode(){
        return this.gcBiasTrainingMode;
    }

    public String getGcBiasRecaliModelType(){
        return this.gcBiasRecaliModelType;
    }

    public boolean isDoGcBiasRecalibration(){
        return this.doGCBiasRecalibration;
    }

    public String getNlsControl() {
        return nlsControl;
    }

    public String getStartValue() {
        return startValue;
    }

    public boolean isOutputPoint() {
        return outputPoint;
    }

    public String getPointPath() {
        return pointPath;
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

    //public int getPartitionNumber() {
    //    return Math.abs(this.numPartitionEachSample);
    //}

    public int getNumPartAlign() {
        return Math.abs(this.numPartEachSampleAlign);
    }

    public int getNumPartProf() {
        return Math.abs(this.numPartEachSampleProf);
    }

    public boolean isSortFastqReads() {
        return this.sortFastqReads;
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

    public String getReadGroupID() {
        return readGroupID;
    }

    /*
    Process controller arguments group.
     */
    public boolean mergeSamBySample() {
        return this.mergeSamBySample;
    }
    public boolean doAlignment(){
        return this.doAlignment;
    }
    public boolean doProfiling(){
        return this.doProfiling;
    }

    public String getHdfsOutputDir() {
        return hdfsOutputDir;
    }

    public boolean isLocalFS() {
        return this.isLocal;
    }

    public boolean isRetainTemp() {
        return retainTemp;
    }

    /*
    Resource management
     */
    public String getAlignmentExeMemory() {
        return this.alignmentExeMemory;
    }
    public String getAlignmentExeNumber() {
        return this.alignmentExeNumber;
    }
    public String getProfilingExeMemory() {
        return this.profilingExeMemory;
    }
    public String getProfilingExeNumber() {
        return this.profilingExeNumber;
    }

    public String getAlignmentExeCores() {
        return this.alignmentExeCores;
    }
    //public String getAlignmentTaskCpus() {
    //    return this.alignmentTaskCpus;
    //}
    public String getProfilingExeCores() {
        return this.profilingExeCores;
    }
    public String getProfilingTaskCpus() {
        return this.profilingTaskCpus;
    }
}
