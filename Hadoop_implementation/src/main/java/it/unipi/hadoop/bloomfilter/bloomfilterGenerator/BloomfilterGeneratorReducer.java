package it.unipi.hadoop.bloomfilter.bloomfilterGenerator;

import it.unipi.hadoop.bloomfilter.parametersGenerator.Parameters;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.BitSet;

public class BloomfilterGeneratorReducer extends Reducer<IntWritable, BytesWritable, IntWritable, ArrayBoolean> {

    private static ArrayBoolean bf_final = new ArrayBoolean();

    protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        BytesWritable BloomFilterWrapper = values.iterator().next(); //Get the first bf as BytesWritable
        BitSet BloomFilter = BitSet.valueOf(BloomFilterWrapper.getBytes()); // Convert the bf from BytesWritable to BitSet
        BitSet toMerge = null;

        //Foreach other bloomfilter in the iterator, compute the or operation with the BloomFilterWrapper
        for (BytesWritable value : values){
            toMerge = BitSet.valueOf(value.getBytes());
            BloomFilter.or(toMerge);
        }

        bf_final.set(BloomFilter.toByteArray(),0, BloomFilter.toByteArray().length);

        context.write(key,bf_final);
    }
}
