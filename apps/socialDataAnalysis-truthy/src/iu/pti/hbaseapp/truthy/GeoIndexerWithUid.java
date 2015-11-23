package iu.pti.hbaseapp.truthy;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class GeoIndexerWithUid extends GeoIndexer {
    private static final Log LOG = LogFactory.getLog(GeoIndexerWithUid.class);
    private long noCoordinateCount = 0;
    private long haveCoordinateCount = 0;
    private long noLonLat = 0;
    private long lonlatProcessed = 0;
    
    @Override
    protected List<Put> getIndexPuts(Put tweetPut) {
        List<Put> res = new LinkedList<Put>();
        byte[] tweetIdBytes = tweetPut.getRow();
        byte[] ctBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES).get(0).getValue();
        long createTime = Bytes.toLong(ctBytes);
        byte[] uidBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES).get(0).getValue();
        //LOG.info("tweet create time = " + createTime);
        //byte[] coordinatesBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_COORDINATES_BYTES).get(0).getValue();
        byte[] coordinatesBytes = null;
        byte[] geoBytes = null;
        List<KeyValue> coordinatesList = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_COORDINATES_BYTES);
        List<KeyValue> geoList = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_GEO_BYTES);
        
        if (!coordinatesList.isEmpty()) {
          coordinatesBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_COORDINATES_BYTES).get(0).getValue();
          haveCoordinateCount++;
        } else if (!geoList.isEmpty()) {
            geoBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_GEO_BYTES).get(0).getValue();
            haveCoordinateCount++;
        }
        
        if ((geoBytes == null || geoBytes.length == 0) 
                && (coordinatesBytes == null || coordinatesBytes.length == 0)) {
            noCoordinateCount++;
//            if (noCoordinateCount % 1000000 == 0) {
//                LOG.info("Amount of tweets does not have coordinates = " + noCoordinateCount);
//            }
            return res;
        }
        
//        if (haveCoordinateCount % 100000 == 0) {
//            LOG.info("Amount of tweets has coordinates = " + haveCoordinateCount);
//        }
        
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
            lonlatProcessed++;
//            if (lonlatProcessed % 100000 == 0) {
//                LOG.info("No. of tweets processed: " + lonlatProcessed);
//            }
            Put geoPut = new Put(Bytes.toBytes(lonlat)); // rowkey
            geoPut.add(ConstantsTruthy.CF_TWEETS_BYTES, tweetIdBytes, createTime, null); // col value
            res.add(geoPut);
        } else {
            noLonLat++;
//            LOG.info("No. of tweets with coordinates/geo, but no lon-lat: " + noLonLat);
        }
        return res;
    }
}
