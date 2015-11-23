package iu.pti.hbaseapp.truthy.streaming;

import iu.pti.hbaseapp.Constants;
import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.TweetTextIndexer;

import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

public class TweetFeatures {
	public String tweetId;
	public long createTimeMilli;
	public String userId;
	public String retweetedTid;
	public String retweetedUid;
	public List<String> words;
	public Set<String> mentionedUids;
	public Set<String> hashtags;
	public Set<String> urls;
	
	public TweetFeatures(String tweetJson) throws IllegalArgumentException {
		JsonObject joTweet = null;
		try {
			joTweet = ConstantsTruthy.jsonParser.parse(tweetJson).getAsJsonObject();
		} catch (JsonSyntaxException e) {
			throw new IllegalArgumentException("Syntax error in JSON string.");
		}
		
		try {
			tweetId = joTweet.get("id").getAsString();
			createTimeMilli = ConstantsTruthy.dateFormat.parse(joTweet.get("created_at").getAsString()).getTime();
			userId = joTweet.get("user").getAsJsonObject().get("id").getAsString();
			JsonElement jeRetweet = joTweet.get("retweeted_status");
			if (jeRetweet != null && !jeRetweet.isJsonNull()) {
				JsonObject joRetweet = jeRetweet.getAsJsonObject();
				retweetedTid = joRetweet.get("id").getAsString();
				retweetedUid = joRetweet.get("user").getAsJsonObject().get("id").getAsString();
			}
			mentionedUids = new HashSet<String>(2);
			hashtags = new HashSet<String>(4);
			urls = new HashSet<String>(2);
			words = new LinkedList<String>();
			String text = processEntities(joTweet);
			processText(text);
		} catch (Exception e) {
			throw new IllegalArgumentException("Exception when processing the tweet JSON string: " + e.getMessage());
		}
	}
	
	/**
	 * Process the 'entities' field in <b>joTweet</b>, and return the tweet text after the URLs, mentions, and
	 * hashtags are removed. 
	 * @param joTweet
	 * @return
	 * @throws Exception
	 */
	protected String processEntities(JsonObject joTweet) throws Exception {
		String text = joTweet.get("text").getAsString();
		JsonElement jeEntities = joTweet.get("entities");
		JsonArray jaUserMentions = jeEntities.getAsJsonObject().get("user_mentions").getAsJsonArray();
		JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
		JsonArray jaUrls = jeEntities.getAsJsonObject().get("urls").getAsJsonArray();
		JsonElement jeMedia = jeEntities.getAsJsonObject().get("media");		
		TreeSet<String> memesToMove = new TreeSet<String>(new TweetTextIndexer.StringComparatorByRevLen());
		
		if (!jaUserMentions.isJsonNull() && jaUserMentions.size() > 0) {
			Iterator<JsonElement> ium = jaUserMentions.iterator();
			while (ium.hasNext()) {
				JsonObject jomu = ium.next().getAsJsonObject();
				memesToMove.add("@" + jomu.get("screen_name").getAsString());
				mentionedUids.add(jomu.get("id").getAsString());
			}
		}
		if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
			Iterator<JsonElement> iht = jaHashTags.iterator();
			while (iht.hasNext()) {
				JsonObject joHt = iht.next().getAsJsonObject();
				String ht = "#" + joHt.get("text").getAsString();
				memesToMove.add(ht);
				hashtags.add(ht.toLowerCase());
			}
		}
		if (!jaUrls.isJsonNull() && jaUrls.size() > 0) {
			Iterator<JsonElement> iurl = jaUrls.iterator();
			while (iurl.hasNext()) {
				JsonObject joUrl = iurl.next().getAsJsonObject();
				JsonElement jeChildUrl = joUrl.get("url");
				JsonElement jeChildEurl = joUrl.getAsJsonObject().get("expanded_url");
				if (jeChildUrl != null && !jeChildUrl.isJsonNull()) {
					memesToMove.add(jeChildUrl.getAsString());
					if (jeChildEurl != null && !jeChildEurl.isJsonNull()) {
						urls.add(jeChildEurl.getAsString());
					} else {
						urls.add(jeChildUrl.getAsString());
					}
				}
				
			}
		}
		if (jeMedia != null && !jeMedia.isJsonNull()){
			JsonArray jaMedia = jeMedia.getAsJsonArray();		
			Iterator<JsonElement> iMedia = jaMedia.iterator();
			while (iMedia.hasNext()) {
				JsonObject joMedia = iMedia.next().getAsJsonObject();
				JsonElement jeChildUrl = joMedia.get("url");				
				JsonElement jeChildEurl = joMedia.getAsJsonObject().get("expanded_url");
				if (jeChildUrl != null && !jeChildUrl.isJsonNull()) {
					memesToMove.add(jeChildUrl.getAsString());
					if (jeChildEurl != null && !jeChildEurl.isJsonNull()) {
						urls.add(jeChildEurl.getAsString());
					} else {
						urls.add(jeChildUrl.getAsString());
					}
				}	
			}
		}
		
		if (memesToMove.size() > 0) {
			char[] caText = text.toCharArray();
			for (String meme : memesToMove) {
				int idx = text.indexOf(meme);
				while (idx >= 0) {
					for (int i=idx; i<idx + meme.length(); i++) {
						caText[i] = ' ';
					}
					idx = text.indexOf(meme, idx + meme.length());
				}
			}
			text = new String(caText).toLowerCase();
		}
		return text;
	}
	
	/**
	 * Tokenize <b>text</b> (with stopping and stemming) and populate the list of words.
	 * @param text
	 * @throws Exception
	 */
	protected void processText(String text) throws Exception {
		TokenStream tokenStream = new StandardTokenizer(Version.LUCENE_36, new StringReader(text.toLowerCase()));
		tokenStream = new StopFilter(Version.LUCENE_36, tokenStream, Constants.getStopWordSet());
		tokenStream = new PorterStemFilter(tokenStream);
		CharTermAttribute charTermAttr = tokenStream.getAttribute(CharTermAttribute.class);
		while (tokenStream.incrementToken()) {
			words.add(charTermAttr.toString());
		}
		tokenStream.close();
	}
}
