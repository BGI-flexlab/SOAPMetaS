package org.bgi.flexlab.metas.profiling.recalibration.gcbias;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

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
 * @author: heshixu@genomics.cn
 */

public class GCBiasCorrectionDefaultModel extends GCBiasCorrectionModelBase {

    public GCBiasCorrectionDefaultModel(File inputCoefficientsFile){
        this.inputCoefficients(inputCoefficientsFile);
    }

    public GCBiasCorrectionDefaultModel(){
        this.coefficients = new Double[8];
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
        assert this.isCoefficientsSet();
        return coefficients[0] * Math.exp(-0.5 * Math.pow((readGCContent-coefficients[1])/coefficients[2], 2) ) +
                coefficients[3] + coefficients[4]*readGCContent + coefficients[5]*Math.pow(readGCContent,2) +
                coefficients[6]*Math.pow(readGCContent,3) + coefficients[7]*Math.log(genomeGCContent);
    }

    /**
     * The method is used to save coefficients to file so that the model could be reused.
     *
     * @param outputFile Output stream.
     */
    public void outputCoefficients(File outputFile){
        try (JsonWriter jsonWriter = new JsonWriter(new FileWriter(outputFile))){
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
     * @param inputFile Input stream.
     */
    public void inputCoefficients(File inputFile){

        try (JsonReader jsonReader = new JsonReader(new FileReader(inputFile));){
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

    public Double[] readDoublesArray(JsonReader reader) throws IOException {
        List<Double> doubles = new ArrayList<Double>(8);

        reader.beginArray();
        while (reader.hasNext()) {
            doubles.add(reader.nextDouble());
        }
        reader.endArray();

        Double[] coe = new Double[doubles.size()];

        coe = doubles.toArray(coe);

        assert coe.length == 8;

        return coe;
    }

    public void writeDoublesArray(JsonWriter writer, Double[] doubles) throws IOException {
        writer.beginArray();
        for (Double value : doubles) {
            writer.value(value);
        }
        writer.endArray();
    }

    /**
     * Setter for coefficients, from saved file or trainer.
     *
     * @param coefficients The concrete value from training result or coefficients file.
     */
    public void setCoefficients(Double[] coefficients){
        assert this.coefficients.length == coefficients.length;
        for (int i=0; i < this.coefficients.length; i++){
            this.coefficients[i] = coefficients[i];
        }
        this.setCoefficientsState(true);
    }

}
