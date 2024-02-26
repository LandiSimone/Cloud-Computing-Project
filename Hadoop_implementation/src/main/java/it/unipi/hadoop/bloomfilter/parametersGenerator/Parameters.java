package it.unipi.hadoop.bloomfilter.parametersGenerator;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Parameters implements Writable {
    private long m;
    private long k;

    public Parameters(long m, long k) { set(m,k); }

    public Parameters() { super(); }

    public void set(long m, long k) {
        this.m = m;
        this.k = k;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(m);
        out.writeLong(k);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        m = in.readLong();
        k = in.readLong();
    }

    public String toString() {
        return  m + "\t" + k;
    }
}
