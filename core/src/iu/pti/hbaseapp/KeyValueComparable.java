package iu.pti.hbaseapp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.WritableComparable;

/**
 * A KeyValue wrapper class that implements the WritableComparable interface.
 * @author gaoxm
 */
public class KeyValueComparable implements WritableComparable<KeyValueComparable> {
	KeyValue kv;
	
	public KeyValueComparable() {
		kv = null;
	}
	
	public KeyValueComparable(byte [] row, byte [] family, byte [] qualifier, byte [] value) {
		kv = new KeyValue(row, family, qualifier, value);
	}
	
	public int compareTo(KeyValueComparable that) {
		return KeyValue.COMPARATOR.compare(this.kv, that.kv);
	}
	
	public KeyValue getKeyValue() {
		return kv;
	}
	
	@Override
	public void write(DataOutput output) throws IOException {
		this.kv.write(output);
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		int length = input.readInt();
		byte[] buffer = new byte[length];
		input.readFully(buffer);
		kv = new KeyValue(buffer, 0);
	}
}
