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

public class MemeIndexer implements CustomIndexer {
	
	@Override
	public Map<byte[], List<Put>> index(String tableName, Put sourcePut) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		Map<byte[], List<Put>> res = new HashMap<byte[], List<Put>>();
		List<Put> memePuts = getIndexPuts(sourcePut);
		if (memePuts.size() > 0) {
			res.put(indexTableBytes, memePuts);
		}
		
		return res;
	}

	@Override
	public void index(String tableName, Put sourcePut, TaskInputOutputContext<?, ?, ImmutableBytesWritable, Put> context) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		List<Put> memePuts = getIndexPuts(sourcePut);
		for (Put p : memePuts) {
			context.write(new ImmutableBytesWritable(indexTableBytes), p);
		}
	}

	/**
	 * Parse the 'entities' column of <b>sourcePut</b>, and generate Put objects for the meme index entries.
	 * @param tweetPut
	 * @return a list of Put objects for the meme index entries.
	 */
	protected List<Put> getIndexPuts(Put tweetPut) {
		List<Put> res = new LinkedList<Put>();
		byte[] tweetIdBytes = tweetPut.getRow();
		byte[] ctBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES).get(0).getValue();
		long createTime = Bytes.toLong(ctBytes);
		byte[] entitiesBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_ENTITIES_BYTES).get(0).getValue();
		if (entitiesBytes == null || entitiesBytes.length == 0) {
			return res;
		}
		String entities = Bytes.toString(entitiesBytes);
		JsonObject joEntities = ConstantsTruthy.jsonParser.parse(entities).getAsJsonObject();
		JsonArray jaUserMentions = joEntities.get("user_mentions").getAsJsonArray();
		JsonArray jaHashTags = joEntities.get("hashtags").getAsJsonArray();
		JsonArray jaUrls = joEntities.get("urls").getAsJsonArray();
		
		if (!jaUserMentions.isJsonNull() && jaUserMentions.size() > 0) {
			Iterator<JsonElement> ium = jaUserMentions.iterator();
			while (ium.hasNext()) {
				JsonObject jomu = ium.next().getAsJsonObject();
				String mention = "@" + jomu.get("id").getAsString() + "(" + jomu.get("screen_name").getAsString().toLowerCase() + ")";
				Put memePut = new Put(Bytes.toBytes(mention));
				memePut.add(ConstantsTruthy.CF_TWEETS_BYTES, tweetIdBytes, createTime, null);
				res.add(memePut);
			}
		}
		if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
			Iterator<JsonElement> iht = jaHashTags.iterator();
			while (iht.hasNext()) {
				String hashtag = "#" + iht.next().getAsJsonObject().get("text").getAsString().toLowerCase();
				Put memePut = new Put(Bytes.toBytes(hashtag));
				memePut.add(ConstantsTruthy.CF_TWEETS_BYTES, tweetIdBytes, createTime, null);
				res.add(memePut);
			}
		}
		if (!jaUrls.isJsonNull() && jaUrls.size() > 0) {
			Iterator<JsonElement> iurl = jaUrls.iterator();
			while (iurl.hasNext()) {
				JsonObject joUrl = iurl.next().getAsJsonObject();
				JsonElement jeChildUrl = joUrl.getAsJsonObject().get("url");
				JsonElement jeChildEurl = joUrl.getAsJsonObject().get("expanded_url");
				if (jeChildUrl != null && !jeChildUrl.isJsonNull()) {
					Put memePut = new Put(Bytes.toBytes(jeChildUrl.getAsString()));
					memePut.add(ConstantsTruthy.CF_TWEETS_BYTES, tweetIdBytes, createTime, null);
					res.add(memePut);
				}
				if (jeChildEurl != null && !jeChildEurl.isJsonNull()) {
					Put memePut = new Put(Bytes.toBytes(jeChildEurl.getAsString()));
					memePut.add(ConstantsTruthy.CF_TWEETS_BYTES, tweetIdBytes, createTime, null);
					res.add(memePut);
				}
			}
		}
		
		return res;
	}
}
