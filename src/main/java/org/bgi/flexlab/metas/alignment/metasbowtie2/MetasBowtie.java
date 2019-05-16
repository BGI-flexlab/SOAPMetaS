package org.bgi.flexlab.metas.alignment.metasbowtie2;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.alignment.AlignmentToolWrapper;
import org.bgi.flexlab.metas.util.DataUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * ClassName: MetasBowtie
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class MetasBowtie extends AlignmentToolWrapper implements Serializable {

    private static final Logger LOG = LogManager.getLogger(MetasBowtie.class);

    private static final long serialVersionUID = 1L;

    private boolean isShortIndex;
    private boolean isTab5Mode = false;

    // The --rg argument of bowtie2. In bt2, --rg should be set multi-times, so we use an array here.
    private ArrayList<String> rgArray;

    private String extraArguments;

    public MetasBowtie(MetasOptions options, JavaSparkContext jsc){

        this.setIndexPath(options.getAlignmentIndexPath());

        this.isShortIndex = options.isAlignmentShortIndex();
        this.extraArguments = options.getExtraAlignmentArguments();

        this.setOutputHdfsDir(options.getSamOutputHdfsDir());

        String tmpd = options.getAlignmentTmpDir();
        if (tmpd == null || tmpd.isEmpty()) {
            tmpd = DataUtils.getTmpDir(jsc);
        }
        this.setTmpDir(tmpd);

        this.setSequencingMode(options.getSequencingMode());
    }

    public void setTab5Mode(){
        this.isTab5Mode = true;
    }


    /**
     * Parse all arguments as String array.
     *
     * TODO:index文件的读取是否需要考虑 Spark 的内存共享问题？不过 JNI 的模式可能无法实现。
     *
     * @param alnStep **Omitted in Bowtie.
     * @return Arguments array.
     */
    @Override
    protected String[] parseArguments(int alnStep) {
        ArrayList<String> arguments = new ArrayList<String>();
        HashSet<String> omissionArgs = new HashSet<>(Arrays.asList(
                "--index", "-x",
                "-1", "-2", "-U", "--unpaired",
                "-S", "--output",
                "--un","--un-gz","--un-bz2","--un-lz4",
                "--al","--al-gz","--al-bz2","--al-lz4",
                "--un-conc","--un-conc-gz","--un-conc-bz2","--un-conc-lz4",
                "--al-conc","--al-conc-gz","--al-conc-bz2","--al-conc-lz4",
                "--tab5", "--tab6",
                "--rg", "--rg-id"
        ));

        // The first argument should be the name of executor as the main function (bowtie()) of
        // .cpp script will receive all commandline items.
        if (this.isShortIndex){
            // log.info("[SOAPMetas::" + MetasBowtie.class.getName() + "] Index file has small index suffix \".bt2\",  use bowtie2-align-s")
            arguments.add("bowtie2-align-s");
        } else {
            // log.info("[SOAPMetas::" + MetasBowtie.class.getName() + "] Index file doesn't has small index suffix, use as large idx \".bt2l\", use bowtie2-align-l")
            arguments.add("bowtie2-align-l");
        }

        // Add extra arguments. All bowtie arguments excluding index file path, output file path,
        // special output files and input file paths.
        if (!this.extraArguments.isEmpty()) {
            String[] arrayBwaArgs = StringUtils.split(this.extraArguments, ' ');
            int numBwaArgs = arrayBwaArgs.length;

            for( int i = 0; i < numBwaArgs; i++) {
                if (omissionArgs.contains(arrayBwaArgs[i])){
                    i++; // skip the next arg
                    continue;
                }
                arguments.add(arrayBwaArgs[i]);
            }
        }

        // Add output file.
        arguments.add("-S");

        arguments.add(this.getOutputFile());

        // Add index file.
        arguments.add("-x");

        arguments.add(this.getIndexPath());

        arguments.add("--rg-id");
        arguments.add(this.getReadGroupID());

        arguments.add("--rg");
        arguments.add("SM:"+this.getSMTag());

        //if (this.rgArray != null && this.rgArray.size() > 0){
        //    for(String rg: this.rgArray){
        //        arguments.add("--rg");
        //        arguments.add(rg);
        //    }
        //}

        // Add input file.
        if (this.isTab5Mode){
            arguments.add("--tab5");
            arguments.add(this.getInputFile());
        }else if (this.isPairedReads()){
            arguments.add("-1");
            arguments.add(this.getInputFile());
            arguments.add("-2");
            arguments.add(this.getInputFile2());
        }else if (this.isSingleReads()){
            arguments.add("-U");
            arguments.add(this.getInputFile());
        }else{
            arguments.add("-U");
            arguments.add(this.getInputFile() + "," + this.getInputFile2());
        }

        String[] argumentsArray = new String[arguments.size()];

        return arguments.toArray(argumentsArray);
    }

    @Override
    public int run() {
        String[] arguments = this.parseArguments(0);

        LOG.info("[SOAPMetas::" + MetasBowtie.class.getName() + "] Bowtie2 arguments: " + StringUtils.join(arguments, ' '));

        int returnCode;

        if (this.isShortIndex){
            returnCode = new BowtieSJNI().bowtieJNI(arguments, this.alnLog);
        } else {
            returnCode = new BowtieLJNI().bowtieJNI(arguments, this.alnLog);
        }

        if (returnCode == 0){
            LOG.info("[SOAPMetas::" + MetasBowtie.class.getName() + "] Bowtie2 runs successfully for input: " + arguments[arguments.length-1]);
        } else {
            LOG.error("[SOAPMetas::" + MetasBowtie.class.getName() + "] Bowtie2 failed in running for input: " + arguments[arguments.length-1]);
        }

        // 0 means successful execution.
        return returnCode;
    }
}
