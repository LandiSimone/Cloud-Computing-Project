package it.unipi.hadoop.bloomfilter.parametersGenerator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
//
public class ParamsGeneratorReducer extends Reducer<IntWritable, IntWritable, IntWritable, Parameters> {

    private static double p;
    private static Parameters parameters = new Parameters();

    protected void setup(Context context) {
        //Gets the false positive (p) parameter
        Configuration conf = context.getConfiguration();
        p = conf.getDouble("p", 0.1);
    }

    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int counter = 0;

       //Sum of the counters for the rating
       for (IntWritable value : values){
           counter += value.get();
       }

       //Compute the parameters for the rating
       long   m = Math.round(-(counter*Math.log(p)) / Math.pow(Math.log(2), 2));
       long   k = Math.round((m/counter) * Math.log(2));

       parameters.set(m,k);

       //Writes the pair (rating, (m,k))
       context.write(key, parameters);
    }
}
