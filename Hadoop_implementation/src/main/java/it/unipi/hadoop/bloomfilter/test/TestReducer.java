package it.unipi.hadoop.bloomfilter.test;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TestReducer extends Reducer<IntWritable, BooleanWritable, IntWritable, DoubleWritable> {

    private static final DoubleWritable fp_rate = new DoubleWritable();

    protected void reduce(IntWritable key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
        double count_true = 0;
        double count = 0;

        //Count the number of false positive and total instances foreach rating
        for (BooleanWritable value : values){
            if (value.get()){
                count_true++;
            }
            count++;
        }

        //Compute the false positive rate
        double false_positive_rate = count_true/count;

        fp_rate.set(false_positive_rate);
        context.write(key,fp_rate);
    }
}
