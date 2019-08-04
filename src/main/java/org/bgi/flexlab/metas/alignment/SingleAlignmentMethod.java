package org.bgi.flexlab.metas.alignment;


/**
 * ClassName: SingleAlignmentMethod
 * Description: The script is based on com.github.sparkbwa.BwaSingleAlignment.
 *
 * @author heshixu@genomics.cn
 */

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;


@Deprecated
public class SingleAlignmentMethod extends AlignmentMethodBase implements Serializable, Function2<Integer, Iterator<String>, Iterator<String>> {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor of SingleAlignmentMethod.
     *
     * @param context SparkContext
     * @param toolWrapper AlignmentToolWrapper object, reference to used tool wrapper.
     */
    public SingleAlignmentMethod(SparkContext context, AlignmentToolWrapper toolWrapper){
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
    public Iterator<String> call(Integer arg0, Iterator<String> arg1) throws Exception {

        LOG.info("[SOAPMetas::" + SingleAlignmentMethod.class.getName() + "] Tmp dir: " + this.tmpDir);

        String fastqFileName1;

        if(this.tmpDir.endsWith("/")) {
            fastqFileName1 = this.tmpDir + this.appId + "-RDD" + arg0 + "_1";

        }
        else {
            fastqFileName1 = this.tmpDir + "/" + this.appId + "-RDD" + arg0 + "_1";

        }

        LOG.info("[SOAPMetas::" + SingleAlignmentMethod.class.getName() + "] Writing file: " + fastqFileName1);

        File FastqFile1 = new File(fastqFileName1);
        FileOutputStream fos1;
        BufferedWriter bw1;

        ArrayList<String> returnedValues = new ArrayList<String>();

        try {
            fos1 = new FileOutputStream(FastqFile1);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            String newFastqRead;

            while (arg1.hasNext()) {
                newFastqRead = arg1.next();

                bw1.write(newFastqRead);
                bw1.newLine();
            }

            bw1.close();

            //We do not need the input data anymore, as it is written in a local file
            arg1 = null;

            // This is where the actual local alignment takes place
            //returnedValues = this.runAlignmentProcess(arg0, fastqFileName1, null);

            // Delete the temporary file, as is have now been copied to the output directory
            FastqFile1.delete();

        } catch (FileNotFoundException e) {
            LOG.error("[SOAPMetas::" + SingleAlignmentMethod.class.getName() + "] "+e.toString());
        }

        return returnedValues.iterator();
    }
}
