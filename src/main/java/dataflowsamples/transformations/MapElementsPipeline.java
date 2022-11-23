package dataflowsamples.transformations;

import dataflowsamples.FirstPipeline;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class MapElementsPipeline
{
    public static void main(String[] args) {
        // Create the pipeline.
        Pipeline pipeline = Pipeline.create();
        final List<String> itemList = Arrays.asList("Item1", "Item2", "Item3", "Item4");
        PCollection<String> items = pipeline.apply(Create.of(itemList));
        /* MapElements -applies a simple 1-to-1 mapping function over each element in the collection.
         */
        PCollection<Integer> lineLengths = items.apply(MapElements.via(
                //providing the mapping function using a SimpleFunction
                new SimpleFunction<String, Integer>() {
                    @Override
                    public Integer apply(String line) {
                        return line.length();
                    }
                }));
        lineLengths.apply("Print all items",ParDo.of(new PrintElementFn()));
        /**
         * providing the mapping function using a SerializableFunction, which allows the use of Java 8 lambdas.
         * Due to type erasure, you need to provide a hint indicating the desired return type.
         */
        PCollection<Integer> stringItemLengths = items.apply(MapElements
                .into(TypeDescriptors.integers())
                .via((String line) -> line.length()));
        stringItemLengths.apply("Print all items",ParDo.of(new MapElementsPipeline.PrintElementFn()));
        pipeline.run();
    }




    private static class PrintElementFn extends DoFn<Integer,Void>{
        @ProcessElement
        public void processElement(@Element Integer input){
            System.out.println(input);
        }
    }
}


