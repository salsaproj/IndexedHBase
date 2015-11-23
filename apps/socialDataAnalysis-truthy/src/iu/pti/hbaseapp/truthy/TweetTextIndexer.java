package iu.pti.hbaseapp.truthy;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.CustomIndexer;
import iu.pti.hbaseapp.GeneralHelpers;

public class TweetTextIndexer implements CustomIndexer {

	/**
	 * Comparator class for comparing strings according to the reverse comparison of their lengths.
	 * I.e. longer strings are ordered 'before' shorter ones.
	 * @author gaoxm
	 */
	public static class StringComparatorByRevLen implements Comparator<String> {
		@Override
		public int compare(String s1, String s2) {
			int diff = s1.length() - s2.length();
			if (diff != 0) {
				return -1 * diff;
			} else {
				return s1.equals(s2) ? 0 : 1;
			}
		}
	}
	
	protected StringComparatorByRevLen memeRevLenComp = new StringComparatorByRevLen();

	@Override
	public Map<byte[], List<Put>> index(String tableName, Put sourcePut) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		Map<byte[], List<Put>> res = new HashMap<byte[], List<Put>>();
		List<Put> textPuts = getIndexPuts(sourcePut);
		if (textPuts.size() > 0) {
			res.put(indexTableBytes, textPuts);
		}
		
		return res;
	}

	@Override
	public void index(String tableName, Put sourcePut, TaskInputOutputContext<?, ?, ImmutableBytesWritable, Put> context) throws Exception {
		int idx = tableName.indexOf('-');
		String month = tableName.substring(idx + 1);
		String indexTableName = ConstantsTruthy.TEXT_INDEX_TABLE_NAME + "-" + month;
		byte[] indexTableBytes = Bytes.toBytes(indexTableName);
		List<Put> textPuts = getIndexPuts(sourcePut);
		for (Put p : textPuts) {
			context.write(new ImmutableBytesWritable(indexTableBytes), p);
		}
	}

	/**
	 * Parse the 'text' column of <b>sourcePut</b>, and generate Put objects for the text index entries.
	 * @param tweetPut
	 * @return a list of Put objects for the text index entries.
	 */
	protected List<Put> getIndexPuts(Put tweetPut) {
		List<Put> res = new LinkedList<Put>();
		byte[] tweetIdBytes = tweetPut.getRow();
		byte[] textBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_TEXT_BYTES).get(0).getValue();
		if (textBytes == null || textBytes.length == 0) {
			return res;
		}
		String text = Bytes.toString(textBytes);
		byte[] ctBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES).get(0).getValue();
		long createTime = Bytes.toLong(ctBytes);
		
		// remove user mentions, hashtags, and URLs from the text
		byte[] entitiesBytes = tweetPut.get(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_ENTITIES_BYTES).get(0).getValue();
		if (entitiesBytes != null && entitiesBytes.length > 0) {
			TreeSet<String> memes = new TreeSet<String>(memeRevLenComp);
			String entities = Bytes.toString(entitiesBytes);
			JsonObject joEntities = ConstantsTruthy.jsonParser.parse(entities).getAsJsonObject();
			JsonArray jaUserMentions = joEntities.get("user_mentions").getAsJsonArray();
			JsonArray jaHashTags = joEntities.get("hashtags").getAsJsonArray();
			JsonArray jaUrls = joEntities.get("urls").getAsJsonArray();	
			if (!jaUserMentions.isJsonNull() && jaUserMentions.size() > 0) {
				Iterator<JsonElement> ium = jaUserMentions.iterator();
				while (ium.hasNext()) {
					JsonObject jomu = ium.next().getAsJsonObject();
					memes.add("@" + jomu.get("screen_name").getAsString());
				}
			}
			if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
				Iterator<JsonElement> iht = jaHashTags.iterator();
				while (iht.hasNext()) {
					JsonObject joHt = iht.next().getAsJsonObject();
					memes.add("#" + joHt.get("text").getAsString());
				}
			}
			if (!jaUrls.isJsonNull() && jaUrls.size() > 0) {
				Iterator<JsonElement> iurl = jaUrls.iterator();
				while (iurl.hasNext()) {
					JsonObject joUrl = iurl.next().getAsJsonObject();
					memes.add(joUrl.get("url").getAsString());
				}
			}
			
			if (memes.size() > 0) {
				char[] caText = text.toCharArray();
				for (String meme : memes) {
					int idx = text.indexOf(meme);
					while (idx >= 0) {
						for (int i=idx; i<idx + meme.length(); i++) {
							caText[i] = ' ';
						}
						idx = text.indexOf(meme, idx + meme.length());
					}
				}
				text = new String(caText);
			}
		}		
		
		// generate index puts
		HashSet<String> terms = new HashSet<String>();
		GeneralHelpers.getTermsByLuceneAnalyzer(Constants.getLuceneAnalyzer(), text, "dummy", terms);
		for (String term : terms) {
			byte[] termBytes = Bytes.toBytes(term.toLowerCase());
			Put termPut = new Put(termBytes);
			termPut.add(ConstantsTruthy.CF_TWEETS_BYTES, tweetIdBytes, createTime, null);
			res.add(termPut);
		}
		
		return res;
	}
}
