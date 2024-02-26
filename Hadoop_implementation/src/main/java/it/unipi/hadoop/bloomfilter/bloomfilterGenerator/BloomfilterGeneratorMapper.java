package it.unipi.hadoop.bloomfilter.bloomfilterGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hadoop.util.hash.MurmurHash;

public class BloomfilterGeneratorMapper extends Mapper<Object, Text, IntWritable, BytesWritable> {
    private static final BitSet[] bloomFilters = new BitSet[10];
    private static final Long [] msBloomfilters = new Long[10];
    private static final Long [] ksBloomfilters = new Long[10];
    private static final MurmurHash h = new MurmurHash();

    @Override
    protected void setup(Context context) {
        String line;
        String []  linesplit;

        Configuration conf = context.getConfiguration();

        for(int i=0; i<10; i++) {
            line = conf.get("par_" + (i + 1), "");
            linesplit = line.split("\t");

            msBloomfilters[i] = Long.parseLong(linesplit[1]);
            ksBloomfilters[i] = Long.parseLong(linesplit[2]);
        }

        for(int i=0; i<10; i++){
            bloomFilters[i]  = new BitSet(Math.toIntExact(msBloomfilters[i]));
        }
    }

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); //Gets the line of the file
        String tokens[] = line.split("\t"); //Splits the line with tab as splitter

        int rating = (int) Math.round(Double.parseDouble(tokens[1])) - 1; //Extracts the rating from the line
        String film_id = tokens[0]; //Extracts the film_id from the line

        //Gets the bloomfilter and parameters associated with the rating
        BitSet bfRating = bloomFilters[rating];
        Long m = msBloomfilters[rating];
        Long k = ksBloomfilters[rating];

        //Compute the index given by each hash function and sets the bit associated to the index at 1
        for(int i=0; i<k; i++){
            byte[] b = film_id.getBytes();

            int index = (int) (Math.abs(h.hash(b,b.length, i)) % m);

            bfRating.set(index); //Sets the bit to true
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i=0; i<10; i++){
            context.write(new IntWritable(i+1),new BytesWritable(bloomFilters[i].toByteArray()));
        }
    }

}
