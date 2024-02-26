package it.unipi.hadoop.bloomfilter.parametersGenerator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//chiave, linea del file,
public class ParamsGeneratorMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private int[] film_counter;

    @Override
    protected void setup(Context context) {
        // Create 10 instances counters, one foreach film rating
        film_counter = new int[10];
        for (int i = 0; i < 10; i++) {
            film_counter[i] = 0;
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); //Gets the line of the file
        String tokens[] = line.split("\t"); //Splits the line with tab as splitter
        int rating = (int) Math.round(Double.parseDouble(tokens[1])); //Gets the rating of the film
        film_counter[rating - 1] += 1; //Add the counter corresponding to the rating
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Writes the pair (rating, instances)
        for (int i = 0; i < 10; i++) {
            context.write(new IntWritable(i + 1), new IntWritable(film_counter[i]));
        }
    }
}

