package dataflowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class FirstPipeline
{
    public static void main(String[] args) {
        // Create a Java Collection, in this case a List of Strings.
        final List<String> countryNameList = Arrays.asList("India", "US", "Denmark", "Russia");



        // Create the pipeline.
        Pipeline pipeline = Pipeline.create();
        //// Apply Create, passing the list and the coder, to create the PCollection.
        PCollection<String> countriesCollection = pipeline.apply("Country Names from List",
                Create.of(countryNameList));
        PCollection<String> countriesWithC =countriesCollection.apply("Filter by A", ParDo.of(new DoFn<String, String>() {

            @ProcessElement
            public void processElement(@Element String element, DoFn.ProcessContext c) {
                System.out.println("element:"+element.toLowerCase());
                //c.o
            }
        }));

        pipeline.run();
    }
}
