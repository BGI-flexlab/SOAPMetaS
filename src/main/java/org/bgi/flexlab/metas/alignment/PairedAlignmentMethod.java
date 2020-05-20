package org.bgi.flexlab.metas.alignment;

/**
 * ClassName: PairedAlignmentMethod
 * Description: The script is based on com.github.sparkbwa.BwaPairedAlignment.
 *
 * @author heshixu@genomics.cn
 */

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;


@Deprecated
public class PairedAlignmentMethod extends AlignmentMethodBase implements Serializable, Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<String>>{

    private static final long serialVersionUID = 1L;

    /**
     * Constructor
     * @param context The Spark context
     * @param toolWrapper AlignmentToolWrapper object, reference to used tool wrapper.
     */
    public PairedAlignmentMethod(SparkContext context, AlignmentToolWrapper toolWrapper) {
        super(context, toolWrapper);
    }

    /**
     * Code to run in each one of the mappers. This is, the alignment with the corresponding entry
     * data The entry data has to be written into the local filesystem
     * @param arg0 The RDD Id
     * @param arg1 An iterator containing the values in this RDD
     * @return An iterator containing the sam file name generated
     * @throws Exception
     */
    public Iterator<String> call(Integer arg0, Iterator<Tuple2<String, String>> arg1) throws Exception {

        // STEP 1: Input fastq reads tmp file creation
        LOG.trace("[SOAPMetas::" + PairedAlignmentMethod.class.getName() + "] Tmp dir: " + this.tmpDir);

        String fastqFileName1;
        String fastqFileName2;

        fastqFileName1 = this.tmpDir + "/" + this.appId + "-RDD" + arg0 + "_1";
        fastqFileName2 = this.tmpDir + "/" + this.appId + "-RDD" + arg0 + "_2";

        //LOG.trace("[SOAPMetas::" + PairedAlignmentMethod.class.getName() + "] Writing file: " + fastqFileName1);
        //LOG.trace("[SOAPMetas::" + PairedAlignmentMethod.class.getName() + "] Writing file: " + fastqFileName2);

        File FastqFile1 = new File(fastqFileName1);
        File FastqFile2 = new File(fastqFileName2);

        FileOutputStream fos1;
        FileOutputStream fos2;

        BufferedWriter bw1;
        BufferedWriter bw2;

        ArrayList<String> returnedValues = new ArrayList<String>();

        //We write the data contained in this split into the two tmp files
        try {
            fos1 = new FileOutputStream(FastqFile1);
            fos2 = new FileOutputStream(FastqFile2);

            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
            bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

            Tuple2<String, String> newFastqRead;

            while (arg1.hasNext()) {
                newFastqRead = arg1.next();

                bw1.write(newFastqRead._1);
                bw1.newLine();

                bw2.write(newFastqRead._2);
                bw2.newLine();
            }

            bw1.close();
            bw2.close();

            arg1 = null;

            // This is where the actual local alignment takes place
            //returnedValues = this.runAlignmentProcess(arg0, fastqFileName1, fastqFileName2);

            // Delete temporary files, as they have now been copied to the output directory
            //LOG.info("[SOAPMetas::" + PairedAlignmentMethod.class.getName() + "] " + PairedAlignmentMethod.class.getName() + "] Deleting file: " + fastqFileName1);
            FastqFile1.delete();

            //LOG.info("[SOAPMetas::" + PairedAlignmentMethod.class.getName() + "] " + PairedAlignmentMethod.class.getName() + "] Deleting file: " + fastqFileName2);
            FastqFile2.delete();

        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + PairedAlignmentMethod.class.getName() + "] "+e.toString());
        }

        return returnedValues.iterator();
    }
}
