package dataflowsamples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;

public class FirstPipeline
{
    public static void main(String[] args) {
        //pipelineRunWithLocaleFileData();
        pipelineRunWithSalesTransactionData();
    }

    public static void pipelineRunWithDummyData()
    {
        // Create the pipeline.
        Pipeline pipeline = Pipeline.create();
        // Create a Java Collection, in this case a List of Strings.
        final List<String> lines = Arrays.asList("India is great", "US", "Denmark", "Russia");
        PCollection<String> input = pipeline.apply(Create.of(lines));
        input.apply("Print all items",ParDo.of(new PrintElementFn()));
        pipeline.run();
    }
    public static void pipelineRunWithLocaleFileData()
    {
        // Create the pipeline.
        Pipeline pipeline = Pipeline.create();
        String currentDirectory = System.getProperty("user.dir");
        String sourceFilePath = currentDirectory+File.separator+"src"+File.separator+"main"
                +File.separator+"resources"+File.separator+"input" +File.separator+ "input.txt";
        PCollection<String> input = pipeline.apply("Reading Text", TextIO.read().from(sourceFilePath));
        input.apply("Print all items",ParDo.of(new PrintElementFn()));
        pipeline.run();
    }
    public static void pipelineRunWithSalesTransactionData()
    {
        // Create the pipeline.
        Pipeline pipeline = Pipeline.create();
        String currentDirectory = System.getProperty("user.dir");
        String sourceFilePath = currentDirectory+File.separator+"src"+File.separator+"main"
                +File.separator+"resources"+File.separator+"input" +File.separator+ "sales_transactions.csv";
        PCollection<String> input = pipeline.apply("Reading Text", TextIO.read().from(sourceFilePath));
        input.apply("Print all items",ParDo.of(new PrintElementFn()));
        pipeline.run();
    }



    private static class PrintElementFn extends DoFn<String,Void>{
        @ProcessElement
        public void processElement(@Element String input){
            System.out.println(input);
        }
    }
}


