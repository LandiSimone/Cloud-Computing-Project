package it.unipi.hadoop.bloomfilter.bloomfilterGenerator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import java.lang.reflect.Array;

public class ArrayBoolean extends BytesWritable {

    public ArrayBoolean(byte[] booleanArray){
        super(booleanArray);
    }

    public ArrayBoolean(){
        super();
    }

    @Override
    public String toString() {
        byte[] values = super.copyBytes();

        StringBuilder sb = new StringBuilder();
        for(int j = 0; j < values.length; ++j){
            for (int i = 0; i < 8; ++i) {
                sb.append(values[j] >>> i & 1);
            }
        }

        return sb.toString();
    }
}
