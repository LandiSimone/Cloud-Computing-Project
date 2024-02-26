package it.unipi.hadoop.bloomfilter.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.IOException;
import java.util.BitSet;

public class TestMapper extends Mapper<Object, Text, IntWritable, BooleanWritable> {

    static private final BitSet[] bloomFilters = new BitSet[10];
    static private final Long [] msBloomfilters = new Long[10];
    static private final Long [] ksBloomfilters = new Long[10];
    static private final MurmurHash h = new MurmurHash();
    static private final BooleanWritable result = new BooleanWritable();

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();

        //Get the bloomfilters
        for (int i=0; i<10; i++){
            String bf = conf.get("bf_" + (i + 1), "");
            BitSet b = new BitSet(bf.length());
            for (int j = 0; j<bf.length(); j++){
                if (bf.charAt(j) == '1'){
                    b.set(j);
                }
            }
            bloomFilters[i] = b;
        }

        String line;
        String []  linesplit;
        //Get the parameters associated with each bloomfilter
        for(int i=0; i<10; i++) {
            line = conf.get("par_" + (i + 1), "");
            linesplit = line.split("\t");

            msBloomfilters[i] = Long.parseLong(linesplit[1]);
            ksBloomfilters[i] = Long.parseLong(linesplit[2]);
        }
    }


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); //Gets the line of the file
        String[] tokens = line.split("\t"); //Splits the line with tab as splitter
        int rating = (int) Math.round(Double.parseDouble(tokens[1])); //Gets the rating of the film
        String film_id = tokens[0]; //Extracts the film_id from the line

        for (int i=0; i<10; i++) {
            if (rating != i+1){ //Check the presence in each bloomfilter not associated with the film's rating
                int counter=0;
                BitSet bfRating = bloomFilters[i]; //Get the i-th bloomfilter
                Long m = msBloomfilters[i]; //Get the i-th m parameter
                Long k = ksBloomfilters[i]; //Get the i-th k parameter

                //Compute the index given by each hash function and increase the counter by 1
                for(int j=0; j<k; j++){
                    byte[] b = film_id.getBytes();
                    int index = (int) (Math.abs(h.hash(b,b.length, j)) % m);

                    if (bfRating.get(index) == true ){
                        counter += 1;
                    }
                }

                //If the counter is equal to k, we have a false positive
                if (counter == k) {
                    result.set(true);
                }
                else{
                    result.set(false);
                }

                context.write(new IntWritable(i+1),result);
            }
        }
    }
}
