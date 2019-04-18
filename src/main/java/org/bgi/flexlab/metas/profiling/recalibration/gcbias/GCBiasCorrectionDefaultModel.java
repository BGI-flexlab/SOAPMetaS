package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang3.ArrayUtils;

import java.io.*;

import java.lang.Math;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: GCBiasCorrectionDefaultModel
 * Description: The default correction model for GC bias, the model is used only for BGISEQ-500 sequencing
 * platform. New model coefficients should be trained for data from different platform. The default model is
 * created with reference to <https://doi.org/10.1371/journal.pone.0165015> .
 *
 * @author heshixu@genomics.cn
 */

public class GCBiasCorrectionDefaultModel extends GCBiasCorrectionModelBase implements Serializable{

    public static final long serialVersionUID = 1L;

    public GCBiasCorrectionDefaultModel(String inputCoefficientsFilePath){
        this.inputCoefficients(inputCoefficientsFilePath);
    }

    public GCBiasCorrectionDefaultModel(){
        this.coefficients = new double[]{0.812093, 49.34331, 8.886807, 6.829778, 0.2642576, -0.005291173, 0.00003188492, -2.502158};
    }

    /**
     * Correction for pair-end sequencing data. We use the average rate of read1GC and read2GC as the
     * gc content of the fragment. Then the computation is the same as single-end mode.
     *
     * @param read1GCContent GC content (rate) of read 1 (first segment in SAM file).
     * @param read2GCContent GC content of read 2.
     * @param genomeGCContent GC content of reference genome.
     * @return Double type number, corrected count of read/fragment.
     */
    public Double correctedCountForPair(Double read1GCContent, Double read2GCContent, Double genomeGCContent){
        return correctedCountForSingle((read1GCContent+read2GCContent)/2, genomeGCContent);
    }

    /**
     * Correction for single-end sequencing data. All the coefficients should have been set.
     *
     * @param readGCContent GC content (rate) of read.
     * @param genomeGCContent GC content of reference genome.
     * @return Double type number, corrected count of read/fragment.
     */
    public Double correctedCountForSingle(Double readGCContent, Double genomeGCContent){
        if (genomeGCContent == 0){
            return 1.0;
        }
        assert this.isCoefficientsSet();
        return coefficients[0] * Math.exp(-0.5 * Math.pow((readGCContent-coefficients[1])/coefficients[2], 2) ) +
                coefficients[3] + coefficients[4]*readGCContent + coefficients[5]*Math.pow(readGCContent,2) +
                coefficients[6]*Math.pow(readGCContent,3) + coefficients[7]*Math.log(genomeGCContent);
    }

    /**
     * The method is used to save coefficients to file so that the model could be reused.
     *
     * @param outputFilePath Output stream.
     */
    public void outputCoefficients(String outputFilePath){
        try (JsonWriter jsonWriter = new JsonWriter(new FileWriter(outputFilePath))){
            jsonWriter.setIndent("    ");
            jsonWriter.beginObject();

            jsonWriter.name("function").value(this.modelFunction);
            jsonWriter.name("coefficients");
            writeDoublesArray(jsonWriter, this.coefficients);

            jsonWriter.endObject();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * The method is used to read the saved model file.
     *
     * @param inputFilePath Input stream.
     */
    public void inputCoefficients(String inputFilePath){

        try (JsonReader jsonReader = new JsonReader(new FileReader(inputFilePath))){
            jsonReader.beginObject();

            while (jsonReader.hasNext()){
                String itemName = jsonReader.nextName();
                switch (itemName){
                    case "function": {
                        this.modelFunction = jsonReader.nextString();
                        break;
                    }

                    case "coefficients":{
                        assert jsonReader.peek() != JsonToken.NULL;
                        this.setCoefficients(this.readDoublesArray(jsonReader));
                    }
                }
            }

            jsonReader.endObject();
        } catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }

    }

    public double[] readDoublesArray(JsonReader reader) throws IOException {
        ArrayList<Double> doubles = new ArrayList<>(9);

        reader.beginArray();
        while (reader.hasNext()) {
            doubles.add(reader.nextDouble());
        }
        reader.endArray();

        doubles.trimToSize();
        if (doubles.size() != 8){

        }
        double[] coe = new double[8];
    }

    public void writeDoublesArray(JsonWriter writer, double[] doubles) throws IOException {
        writer.beginArray();
        for (double value : doubles) {
            writer.value(value);
        }
        writer.endArray();
    }

    /**
     * Setter for coefficients, from saved file or trainer.
     *
     * @param coefficients The concrete value from training result or coefficients file.
     */
    public void setCoefficients(double[] coefficients){
        assert this.coefficients.length == coefficients.length;
        for (int i=0; i < this.coefficients.length; i++){
            this.coefficients[i] = coefficients[i];
        }
        this.setCoefficientsState(true);
    }

}
