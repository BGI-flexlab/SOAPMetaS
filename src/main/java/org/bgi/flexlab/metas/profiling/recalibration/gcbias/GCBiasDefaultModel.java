package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;

import java.lang.Math;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * ClassName: GCBiasDefaultModel
 * Description: The default recalibration model for GC bias, the model is used only for BGISEQ-500 sequencing
 * platform. New model coefficients should be trained for data from different platform. The default model is
 * created with reference to <https://doi.org/10.1371/journal.pone.0165015> .
 *
 * @author heshixu@genomics.cn
 */

public class GCBiasDefaultModel extends GCBiasModelBase implements Serializable{

    public static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(GCBiasDefaultModel.class.getName());

    private final int COEFFICIENT_NUM = 8;

    private String nlsControlList = "maxiter=50,tol=1e-05,minFactor=1/1024";
    private String startValue = "p1=0.812093,p2=49.34331,p3=8.886807,p4=6.829778,p5=0.2642576,p6=-0.005291173,p7=3.188492E-5,p8=-2.502158";

    public GCBiasDefaultModel(String inputCoefficientsFilePath){
        this.inputCoefficients(inputCoefficientsFilePath);
        LOG.info("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Input model: Function : " +
                this.modelFunction + " | Coefficients: " + Arrays.toString(this.coefficients));
    }

    public GCBiasDefaultModel(){
        //default function refers to <https://doi.org/10.1371/journal.pone.0165015>
        this.modelFunction = "I(p1*exp(-0.5*((readGC-p2)/p3)^2)+p4)+I(p5*readGC)+I(p6*(readGC^2))+I(p7*(readGC^3))+I(p8*log(genomeGC))";

        this.coefficients = new double[]{0.812093, 49.34331,
                8.886807, 6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158};
    }

    /**
     * Recalibration for pair-end sequencing data. We use the average rate of read1GC and read2GC as the
     * gc content of the fragment. Then the computation is the same as single-end mode.
     *
     * @param read1GCContent GC content (rate) of read 1 (first segment in SAM file).
     * @param read2GCContent GC content of read 2.
     * @param genomeGCContent GC content of reference genome.
     * @return Double type number, recalibrated count of read/fragment.
     */
    public Double recalibrateForPair(Double read1GCContent, Double read2GCContent, Double genomeGCContent){
        return recalibrateForSingle((read1GCContent+read2GCContent)/2, genomeGCContent);
    }

    /**
     * Recalibration for single-end sequencing data. All the coefficients should have been set.
     *
     * @param readGCContent GC content (rate) of read.
     * @param genomeGCContent GC content of reference genome.
     * @return Double type number, recalibrated count of read/fragment.
     */
    public Double recalibrateForSingle(Double readGCContent, Double genomeGCContent){
        if (genomeGCContent == 0){
            return 0.0;
        }

        Double cvalue = coefficients[0] * Math.exp(-0.5 * Math.pow((readGCContent-coefficients[1])/coefficients[2], 2)) +
                coefficients[3] + coefficients[4]*readGCContent + coefficients[5]*Math.pow(readGCContent,2) +
                coefficients[6]*Math.pow(readGCContent,3) + coefficients[7]*Math.log(genomeGCContent);

        //LOG.trace("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Input readGCContent: " +
        //        readGCContent.toString() + " | Input genomeGCContent: " + genomeGCContent.toString() +
        //        " | Recalibrated value: " + cvalue.toString());

        if (cvalue < 0.1) {
            cvalue = 0.1;
        }
        return 1/cvalue;
    }

    /**
     * The method is used to save coefficients to file so that the model could be reused.
     *
     * @param outputFilePath Output stream.
     */
    public void outputCoefficients(String outputFilePath){
        try {
            JsonWriter jsonWriter = new JsonWriter(new FileWriter(outputFilePath));
            jsonWriter.setIndent("\t");
            jsonWriter.beginObject();

            // Write function to json.
            jsonWriter.name("function").value(this.modelFunction);

            // Write coefficients to json.
            jsonWriter.name("coefficients");
            jsonWriter.beginArray();
            for (double value : this.coefficients) {
                jsonWriter.value(value);
            }
            jsonWriter.endArray();

            // Write control arguments to json.
            jsonWriter.name("control").value(this.nlsControlList);
            jsonWriter.name("startvalue").value(this.startValue);


            jsonWriter.endObject();
            jsonWriter.close();

        } catch (IOException e){
            LOG.error("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Fail to write coefficients " +
                    "json file: " + outputFilePath + " . " + e.toString());
        }
    }

    /**
     * The method is used to read the saved model file.
     *
     * @param inputFilePath Input stream.
     */
    public void inputCoefficients(String inputFilePath){

        try {
            JsonReader jsonReader = new JsonReader(new FileReader(inputFilePath));

            jsonReader.beginObject();

            while (jsonReader.hasNext()){
                String itemName = jsonReader.nextName();
                switch (itemName){
                    case "function": {
                        this.modelFunction = jsonReader.nextString();
                        break;
                    }
                    case "coefficients":{
                        if (jsonReader.peek() == JsonToken.NULL){
                            throw new JsonParseException("Input coefficients is omitted.");
                        }
                        this.setCoefficients(this.readDoublesArray(jsonReader));
                        break;
                    }
                    default: {
                        // The next string must be extracted or the nextName() method will cause an exception.
                        String currentStr = jsonReader.nextString();
                        LOG.info("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Json Item Name " + itemName + " value: " + currentStr);
                    }
                }
            }

            jsonReader.endObject();
            jsonReader.close();
        } catch (FileNotFoundException e){
            LOG.error("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Can't find input " +
                    "coefficients file: " + inputFilePath + " . " + e.toString());
        } catch (IOException e){
            LOG.error("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Fail to read input " +
                    "coefficients file: " + inputFilePath + " . " + e.toString());
        } catch (JsonParseException e) {
            LOG.error("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Fail to parse " +
                    "coefficients from file: " + inputFilePath + " . " + e.toString());
        }

    }

    private double[] readDoublesArray(JsonReader reader) throws IOException {
        ArrayList<Double> doubles = new ArrayList<>(9);

        reader.beginArray();
        while (reader.hasNext()) {
            doubles.add(reader.nextDouble());
        }
        reader.endArray();

        double[] coe = new double[doubles.size()];

        int size = coe.length;
        for (int i=0; i < size; i++){
            coe[i] = doubles.get(i);
        }

        return coe;
    }

    /**
     * Setter for coefficients, from saved file or trainer.
     *
     * @param coefficients The concrete value from training result or coefficients file.
     */
    public void setCoefficients(double[] coefficients){

        if (coefficients.length != COEFFICIENT_NUM){
            LOG.error("[SOAPMetas::" + GCBiasDefaultModel.class.getName() + "] Wrong number of input " +
                    "coefficients. Expected: 8. Input: " + coefficients.length + " . Only utilize " +
                    "first eight.");
        }

        this.coefficients = coefficients;
        this.setCoefficientsState(true);
    }

    public void setNlsControlList(String nlsControlList) {
        this.nlsControlList = nlsControlList;
    }

    public void setStartValue(String startValue){
        this.startValue = startValue;
    }
}
