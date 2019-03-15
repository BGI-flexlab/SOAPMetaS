package org.bgi.flexlab.metas.alignment.metasbowtie2;

import org.bgi.flexlab.metas.MetasOptions;
import org.bgi.flexlab.metas.alignment.AlignmentToolWrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * ClassName: MetasBowtie
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public class MetasBowtie extends AlignmentToolWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean isShortIndex;
    private String extraArguments;

    public MetasBowtie(MetasOptions options){

        this.setIndexPath(options.getAlignmentIndexPath());

        this.isShortIndex = options.isAlignmentShortIndex();
        this.extraArguments = options.getExtraAlignmentArguments();

        this.setOutputHdfsDir(options.getSamOutputHdfsDir());
        this.setTmpDir(options.getAlignmentTmpDir());

        this.setSequencingMode(options.getSequencingMode());
    }

    private boolean isShortFileSuffix(String indexFilePath){
        return indexFilePath.endsWith("bt2");
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
                "--al-conc","--al-conc-gz","--al-conc-bz2","--al-conc-lz4"
        ));

        // The first argument should be the name of executor as the main function (bowtie()) of
        // .cpp script will receive all commandline items.
        if (this.isShortIndex && this.isShortFileSuffix(this.getIndexPath())){
            // log.info("index file has small index suffix \".bt2\",  use bowtie2-align-s")
            arguments.add("bowtie2-align-s");
        } else {
            // log.info("index file doesn't has small index suffix, use as large idx \".bt2l\", use bowtie2-align-l")
            arguments.add("bowtie2-align-l");
        }

        // Add extra arguments. All bowtie arguments excluding index file path, output file path,
        // special output files and input file paths.
        if (!this.extraArguments.isEmpty()) {
            String[] arrayBwaArgs = this.extraArguments.split(" ");
            int numBwaArgs = arrayBwaArgs.length;

            for( int i = 0; i < numBwaArgs; i++) {
                if (omissionArgs.contains(arrayBwaArgs[i])){
                    i++;
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

        // Add input file.
        if (this.isPairedReads()){
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

        int returnCode;

        if (this.isShortIndex && this.isShortFileSuffix(this.getIndexPath())){
            returnCode = new BowtieSJNI().bowtieJNI(arguments);
        } else {
            returnCode = new BowtieLJNI().bowtieJNI(arguments);
        }

        if (returnCode != 0){
            //Log.error("["+this.getClass().getName()+"] :: Bowtie2 exited with error code: " + String.valueOf(returnCode));
        }

        return returnCode;
    }
}
