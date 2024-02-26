package it.unipi.hadoop.bloomfilter;

import it.unipi.hadoop.bloomfilter.bloomfilterGenerator.ArrayBoolean;
import it.unipi.hadoop.bloomfilter.bloomfilterGenerator.BloomfilterGeneratorMapper;
import it.unipi.hadoop.bloomfilter.bloomfilterGenerator.BloomfilterGeneratorReducer;
import it.unipi.hadoop.bloomfilter.parametersGenerator.Parameters;
import it.unipi.hadoop.bloomfilter.parametersGenerator.ParamsGeneratorMapper;
import it.unipi.hadoop.bloomfilter.parametersGenerator.ParamsGeneratorReducer;
import it.unipi.hadoop.bloomfilter.test.TestMapper;
import it.unipi.hadoop.bloomfilter.test.TestReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.BitSet;

public class BloomFilter {

    static int num_reducers = 10;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        if (otherArgs.length != 5) {
            System.err.println("Usage: BloomFilter <input path> <output path> <false positive prob> <nLines> <nMappers>");
            System.exit(-1);
        }

        conf.setDouble("p", Double.parseDouble(otherArgs[2])); //Get the false positive rate (p) from args and set it

        String input_file = otherArgs[0];
        String user_output_path = otherArgs[1];

        String parameters_path = user_output_path + "/Parameters";
        String bf_path = user_output_path + "/BloomFilters";
        String test_path = user_output_path + "/Test";

        double nMappers = Integer.parseInt(otherArgs[4]);
        int nLines = (int) Math.ceil(Double.parseDouble(otherArgs[3]) / nMappers); //Get the right number of mappers for the line's number

        long start = System.currentTimeMillis();

        if (!paramsGenerator(conf, input_file, parameters_path, nLines))
            System.exit(-1);

        if (!bloomfilterGenerator(conf, input_file, bf_path, parameters_path + "/part-r-00000", nLines))
            System.exit(-1);

        if (!testBloomFilters(conf, input_file, test_path, bf_path, nLines))
            System.exit(-1);

        long end = System.currentTimeMillis();

        System.out.println(end-start);
    }

    private static boolean paramsGenerator(Configuration conf, String inPath, String outPath, int nLines) throws Exception {
        Job job = Job.getInstance(conf, "paramsGeneratorJob");
        job.setJarByClass(BloomFilter.class);

        //Class type output mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Class type output reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Parameters.class);

        //Set mapper classes of reducer and mapper
        job.setMapperClass(ParamsGeneratorMapper.class);
        job.setReducerClass(ParamsGeneratorReducer.class);

        //Set input file and output file path and format
        FileInputFormat.addInputPath(job,  new Path(inPath));
        FileOutputFormat.setOutputPath(job,  new Path(outPath));
        NLineInputFormat.setNumLinesPerSplit(job, nLines);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean bloomfilterGenerator(Configuration conf, String inPath, String outPath, String inDataPath, int nLines) throws Exception {
        SetParams(conf, inDataPath, ""); //Set the parameters foreach rating

        Job job = Job.getInstance(conf, "bloomfilterGeneratorJob");
        job.setJarByClass(BloomFilter.class);
        job.setNumReduceTasks(num_reducers); //Set the number of reducers used by the job

        //Class type output mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //Class type output reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        //Set mapper classes of reducer and mapper
        job.setMapperClass(BloomfilterGeneratorMapper.class);
        job.setReducerClass(BloomfilterGeneratorReducer.class);

        //Set input file and output file path and format
        FileInputFormat.addInputPath(job,  new Path(inPath));
        FileOutputFormat.setOutputPath(job,  new Path(outPath));
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, nLines);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean testBloomFilters(Configuration conf, String inPath, String outPath, String inDataPath, int nLines) throws Exception {

        //Set the parameters used by the test (bloomfilters)
        for (int i=0; i<num_reducers;i++){
            setBloomFilters(conf, inDataPath + "/part-r-0000" + i, "");
        }

        Job job = Job.getInstance(conf, "testBloomFiltersJob");
        job.setJarByClass(BloomFilter.class);
        job.setNumReduceTasks(num_reducers);

        //Class type output mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BooleanWritable.class);

        //Class type output reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        //Set mapper classes of reducer and mapper
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);

        //Set input file and output file path and format
        FileInputFormat.addInputPath(job,  new Path(inPath));
        FileOutputFormat.setOutputPath(job,  new Path(outPath));
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, nLines);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static void SetParams(Configuration conf, String pathString, String pattern) throws Exception {
        long rating;
        String[] linesplit;
        FileSystem hdfs = FileSystem.get(conf);

        BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(new Path(pathString))));
        try {
            String line;
            line=br.readLine();
            while (line != null){
                linesplit = line.split("\t"); //Split the line by "tab"
                rating = Long.parseLong(linesplit[0]); //Get the first token of the line that is the rating of each film
                conf.set("par_"+rating, line);
                // be sure to read the next line otherwise we get an infinite loop
                line = br.readLine();
            }
        } finally {
            // close out the BufferedReader
            br.close();
        }
    }

    private static void setBloomFilters(Configuration conf, String pathString, String pattern) throws Exception {
        long rating;
        String[] linesplit;
        FileSystem hdfs = FileSystem.get(conf);

        BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(new Path(pathString))));
        try {
            String line;
            line=br.readLine();
            while (line != null){
                linesplit = line.split("\t"); //Split the line by "tab"
                rating = Long.parseLong(linesplit[0]); //Get the first token of the line that is the rating of each film
                String bf = linesplit[1]; //Get the second token of the line that represent the bloomfilter foreach rating

                conf.set("bf_"+rating, bf);
                // be sure to read the next line otherwise we get an infinite loop
                line = br.readLine();
            }
        } finally {
            // close out the BufferedReader
            br.close();
        }
    }

}








