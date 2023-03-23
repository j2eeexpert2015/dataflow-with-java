package dataflowsamples.transformations;

import java.io.File;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class FileReadAndWritePipeline
{
    public static void main(String[] args) {
        // Create the pipeline.
        Pipeline pipeline = Pipeline.create();
        String currentDirectory = System.getProperty("user.dir");
        String sourceFilePath = currentDirectory+ File.separator+"src"+File.separator+"main"
                +File.separator+"resources"+File.separator+"input" +File.separator+ "employee.csv";
        PCollection<String> input = pipeline.apply("Reading Input File", TextIO.read().from(sourceFilePath));
        input.apply("Print all items",ParDo.of(new FileReadAndWritePipeline.PrintElementFn()));
        String targetFilePath = currentDirectory+ File.separator+"src"+File.separator+"main"
                +File.separator+"resources"+File.separator+"output" +File.separator+ "employee.csv";
        input.apply("Writing output File", TextIO.write().to(targetFilePath));
        pipeline.run();
    }




    private static class PrintElementFn extends DoFn<String,Void>{
        @ProcessElement
        public void processElement(@Element String input){
            System.out.println(input);
        }
    }
}


