package iu.pti.hbaseapp;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This application reads a given number of rows sequentially from a given HBase table, and write the content to 
 * stdout in a human readable format. 4 data types are supported for the row key, column family, qualifier, and 
 * cell value: string, int, double, long.
 * <p/>
 * Usage: java iu.pti.hbaseapp.HBaseTableReader [table name] [column family name] [row type] [cf type] [qualifier
 * type] [value type] [number of rows to read]
 * 
 * @author gaoxm
 */
public class HBaseTableReader {
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.HBaseTableReader <table name> <column family name> <row type> "
				+ "<cf type> <qualifier type> <value type> <number of rows to read>");
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
			usage();
			System.exit(1);
		}
		String tableName = args[0];
		String columnFamily = args[1];
		String rowType = args[2].toLowerCase();
		String cfType = args[3].toLowerCase();
		String qualType = args[4].toLowerCase();
		String valType = args[5].toLowerCase();
		int rowCount = Integer.parseInt(args[6]);
		
		Constants.DataType rowDataType = Constants.DataType.INT;
		Constants.DataType qualDataType = Constants.DataType.INT;
		Constants.DataType cfDataType = Constants.DataType.INT;
		Constants.DataType valDataType = Constants.DataType.INT;
		
		if (rowType.equals("string")) {
			rowDataType = Constants.DataType.STRING;
		} else if (rowType.equals("double")) {
			rowDataType = Constants.DataType.DOUBLE;
		}
		
		if (cfType.equals("string")) {
			cfDataType = Constants.DataType.STRING;
		} else if (cfType.equals("double")) {
			cfDataType = Constants.DataType.DOUBLE;
		}
		
		if (qualType.equals("string")) {
			qualDataType = Constants.DataType.STRING;
		} else if (qualType.equals("double")) {
			qualDataType = Constants.DataType.DOUBLE;
		}
		
		if (valType.equals("string")) {
			valDataType = Constants.DataType.STRING;
		} else if (valType.equals("double")) {
			valDataType = Constants.DataType.DOUBLE;
		} else if (valType.equals("long")) {
			valDataType = Constants.DataType.LONG;
		}
		
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable table = new HTable(hbaseConfig, Bytes.toBytes(tableName));
		byte[] cfBytes = null;
		switch (cfDataType) {
		case INT:
			cfBytes = Bytes.toBytes(Integer.valueOf(columnFamily));
			break;
		case STRING:
			cfBytes = Bytes.toBytes(columnFamily);
			break;
		case DOUBLE:
			cfBytes = Bytes.toBytes(Double.valueOf(columnFamily));
		default:
		}
		
		ResultScanner rs = table.getScanner(cfBytes);
		System.out.println("scanning table " + tableName + " on " + columnFamily + "...");
		Result r = rs.next();
		String row = "";
		String qual = "";
		String val = "";
		while (r != null && rowCount > 0) {
			switch (rowDataType) {
			case INT:
				row = Integer.toString(Bytes.toInt(r.getRow()));
				break;
			case STRING:
				row = Bytes.toString(r.getRow());
				break;
			case DOUBLE:
				row = Double.toString(Bytes.toDouble(r.getRow()));
			default:
			}
			
			System.out.println("------------" + row + "------------");
			List<KeyValue> cells = r.list();
			Iterator<KeyValue> iter = cells.iterator();
			int valCount = 0;
			while (iter.hasNext()) {
				KeyValue cell = iter.next();
				valCount++;
				switch (qualDataType) {
				case INT:
					qual = Integer.toString(Bytes.toInt(cell.getQualifier()));
					break;
				case STRING:
					qual = Bytes.toString(cell.getQualifier());
					break;
				case DOUBLE:
					qual = Double.toString(Bytes.toDouble(cell.getQualifier()));
				default:
				}
				
				switch (valDataType) {
				case INT:
					/*
					if (tableName.equals(Constants.TEXTS_POSVEC_TABLE_NAME)) {
						DataInputStream dis = new DataInputStream(new ByteArrayInputStream(CellUtil.cloneValue(cell)));
						StringBuffer sb = new StringBuffer();
						while (dis.available() > 0) {
							sb.append(dis.readInt()).append(',');
						}
						dis.close();
						val = sb.toString();
					} else {
						val = Integer.toString(Bytes.toInt(CellUtil.cloneValue(cell)));
					}*/
					val = Integer.toString(Bytes.toInt(cell.getValue()));
					break;
				case STRING:					
					val = Bytes.toString(cell.getValue());
					break;
				case DOUBLE:
					/*
					if (tableName.equals(Constants.CLUEWEB09_SYNONYM_TABLE_NAME)) {
						if (qual.equals(Constants.QUALIFIER_PLUS) || qual.equals(Constants.QUALIFIER_MIN)) {
							val = Double.toString(Bytes.toDouble(CellUtil.cloneValue(cell)));
						} else if (qual.equals(Constants.QUALIFIER_FORWARD) || qual.equals(Constants.QUALIFIER_BACKWARD)) {
							val = Integer.toString(Bytes.toInt(CellUtil.cloneValue(cell)));
						} else if (qual.equals(Constants.QUALIFIER_TERM1COUNT) || qual.equals(Constants.QUALIFIER_TERM2COUNT)) {
							val = Long.toString(Bytes.toLong(CellUtil.cloneValue(cell)));
						} else {
							val = Double.toString(Bytes.toDouble(CellUtil.cloneValue(cell)));
						}
					} else {
						val = Double.toString(Bytes.toDouble(CellUtil.cloneValue(cell)));
					}*/
					val = Double.toString(Bytes.toDouble(cell.getValue()));
					break;
				case LONG:
					val = Long.toString(Bytes.toLong(cell.getValue()));
					break;
				default:
				}
				System.out.println(qual + " : " + val); 
			}
			System.out.println("Total number of values scanned: " + valCount);
			r = rs.next();
			rowCount--;
		}
		rs.close();
		table.close();
	}
}
