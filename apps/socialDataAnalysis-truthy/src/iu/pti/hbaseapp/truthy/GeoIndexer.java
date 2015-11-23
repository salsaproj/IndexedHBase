package iu.pti.hbaseapp.truthy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import iu.pti.hbaseapp.CustomIndexer;

public class GeoIndexer implements CustomIndexer {
	
	private static int MULT_FACTOR = 100;
	
	@Override
	public Map<byte[], List<Put>> index(String tableName, Put sourcePut) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.GEO_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		Map<byte[], List<Put>> res = new HashMap<byte[], List<Put>>();
		List<Put> geoPuts = getIndexPuts(sourcePut);
		if (geoPuts.size() > 0) {
			res.put(indexTableBytes, geoPuts);
		}
		
		return res;
	}

	@Override
	public void index(String tableName, Put sourcePut, TaskInputOutputContext<?, ?, ImmutableBytesWritable, Put> context) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.GEO_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		List<Put> geoPuts = getIndexPuts(sourcePut);
		for (Put p : geoPuts) {
			context.write(new ImmutableBytesWritable(indexTableBytes), p);
		}
	}

	/*
	 * Returns a string representation of the longitude and latitude upto 2 decimal places.
	 * e.g. if lon = -97.51087576 and lat = 35.46500176,
	 * then this returns "-9751_3546".
	 */
	protected String getLongLat(double lon, double lat){
		if (lon < 0){
			lon = - Math.floor(-lon * GeoIndexer.MULT_FACTOR);
		} else {
			lon =  Math.floor(lon * GeoIndexer.MULT_FACTOR);
		}
		
		if (lat < 0){
			lat = - Math.floor(-lat * GeoIndexer.MULT_FACTOR);
		} else {
			lat = Math.floor(lat * GeoIndexer.MULT_FACTOR);
		}
		
		return String.valueOf((int)lon) + "_" + String.valueOf((int)lat);
	}
	/**
	 * Parse the 'tweets' column of <b>sourcePut</b>, and generate Put objects for the geo index entries.
	 * @param tweetPut
	 * @return a list of Put objects for the geo index entries.
	 */
	protected List<Put> getIndexPuts(Put tweetPut) {
		List<Put> res = new LinkedList<Put>();
		byte[] tweetIdBytes = tweetPut.getRow();
		byte[] ctBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES).get(0).getValue();
		long createTime = Bytes.toLong(ctBytes);
		byte[] coordinatesBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_COORDINATES_BYTES).get(0).getValue();
		byte[] geoBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_GEO_BYTES).get(0).getValue();
		String lonlat = null;
		
		if (coordinatesBytes != null && coordinatesBytes.length != 0) {
			String coordinates = Bytes.toString(coordinatesBytes);
			JsonObject joCoordinates = ConstantsTruthy.jsonParser.parse(coordinates).getAsJsonObject();
			JsonArray jaCoordinates = joCoordinates.get("coordinates").getAsJsonArray();
			
			if (!jaCoordinates.isJsonNull() && jaCoordinates.size() == 2) {
				double lon = jaCoordinates.get(0).getAsFloat();
				double lat = jaCoordinates.get(1).getAsFloat();
				lonlat = getLongLat(lon, lat);
			}
		} else if (geoBytes != null && geoBytes.length != 0) {
			String geo = Bytes.toString(geoBytes);
			JsonObject joGeo = ConstantsTruthy.jsonParser.parse(geo).getAsJsonObject();
			JsonArray jaGeo = joGeo.get("coordinates").getAsJsonArray();
			
			if (!jaGeo.isJsonNull() && jaGeo.size() == 2) {
				double lat = jaGeo.get(0).getAsFloat(); // Note: (lat, lon) in geo, compared to (lon, lat) in Coordinates.
				double lon = jaGeo.get(1).getAsFloat();
				lonlat = getLongLat(lon, lat);
			}
		} 
		if (lonlat != null && lonlat.length() != 0){
			Put geoPut = new Put(Bytes.toBytes(lonlat)); // rowkey
			geoPut.add(ConstantsTruthy.CF_TWEETS_BYTES, tweetIdBytes, createTime, null); // col value
			res.add(geoPut);
		} 
		return res;
	}
}
