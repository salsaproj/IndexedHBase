package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.DataPreserveMultiTableOutputFormat;
import iu.pti.hbaseapp.GeneralCustomIndexer;
import iu.pti.hbaseapp.GeneralHelpers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Truthy Data Loader using GeneralCustomIndexer for on-the-fly indexing.
 * 
 * @author gaoxm
 */
public class TruthyDataLoaderWithGci {
	protected static final Log LOG = LogFactory.getLog(TruthyDataLoaderWithGci.class);
	
	/**
	 * "TgdlMapper" stands for "Truthy General custom indexer Data Loader Mapper".
	 * @author gaoxm
	 */
	static class TgdlMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		enum InputFileType {
			FILE_LIST, JSON_CONTENT
		}
		
		protected long tweetCount = 0;
		protected boolean toIndex = false;
		InputFileType inputType = InputFileType.FILE_LIST;
		String tweetTableName = null;
		String userTableName = null;
		byte[] tweetTableBytes = null;
		byte[] userTableBytes = null;
		GeneralCustomIndexer gci = null;
		Random random = null;
		protected long lastSleepTime;
		protected long lastSleepLenInMin;
		protected static long MINUTE_MILLI = 60000;
		protected Calendar calTmp = null;
		
		/** number of seconds to wait in case the loader is processing too fast */
		protected long secToWait = 0;
		
		/** Start processing time of this loader */
		protected long startLoadingTime;
		private String month;
		private final String b4BigIntMonth = "2015-06";
		// for months after 2015-06, we use Big Integer to store Tweet IDs in TweetTable and IndexTable
		private boolean useBigInt = false; 
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			if (inputType == InputFileType.FILE_LIST) {
				String inputGzPath = value.toString();
				processJsonGzFile(inputGzPath, context);
			} else {
				String jsonStr = value.toString().trim();
				processJsonString(jsonStr, context);
			}
		}
		
		/**
		 * Sleep for some time to avoid flooding region servers with more puts in cases of
		 * RetriesExhaustedException. The sleep time is dynamically adjusted in this way:
		 * if the time of last sleep happened in less than 10 minutes ago, then sleep for 1
		 * more minute than last sleep. Otherwise sleep for 1 minute. 
		 */
		protected void loadAdjustingSleep(Context context) {
			long sleepStartTime = System.currentTimeMillis();
			long sleepLength = (long)(10 * 60 * 1000 + 5000 * random.nextFloat());
			calTmp.setTimeInMillis(sleepStartTime);
			String sleepStartTimeStr = GeneralHelpers.getDateTimeString(calTmp);
			String info = "Starting load adjusting sleep - , this sleep length: " + (sleepLength / 1000.0) + " seconds.";
			LOG.info(info);
			context.setStatus(info);
			long passedTime = System.currentTimeMillis() - sleepStartTime;
			while (passedTime < sleepLength) {
				try {
					context.setStatus((passedTime / 1000.0) + " seconds passed, continue sleeping..."); 
					Thread.sleep(120000);
				} catch (InterruptedException e) {
					String warn = "Load adjusting sleep interrupted. Sleep starting time: " + sleepStartTimeStr + ", planned sleep length: " 
							+ (sleepLength / 1000.0) + " seconds.";
					LOG.warn(warn);
					context.setStatus(warn);
				}
				passedTime = System.currentTimeMillis() - sleepStartTime;
			}		
			info = "Waking up from load adjusting sleep. Sleep starting time: " + sleepStartTimeStr + ", planned sleep length: " 
					+ (sleepLength / 1000.0) + " seconds.";
			LOG.info(info);
			context.setStatus(info);
		}
		
		/**
		 * Process the JSON string for one tweet.
		 * @param jsonStr
		 * @param context
		 */
		protected void processJsonString(String jsonStr, Context context) {
			if (jsonStr.length() <= 0) {
				return;
			}
			if (secToWait > 0) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					LOG.warn("Exception when trying to sleep for speed adjusting: " + e.getMessage());
				}
				secToWait -= 1;
			}
			
			boolean needProcess = true;
			while (needProcess) {
				try {
					JsonObject joTweet = ConstantsTruthy.jsonParser.parse(jsonStr).getAsJsonObject();
					processJsonTweet(joTweet, context);
					needProcess = false;
				} catch (RetriesExhaustedException e) {
					e.printStackTrace();
					LOG.warn("RetriesExhaustedException: " + e.getMessage());
					System.gc();
					loadAdjustingSleep(context);
				} catch (Exception e) {
					LOG.info("Exception when processing tweet: + " + e.getMessage());
					e.printStackTrace();
					context.setStatus("Exception when processing tweet: + " + e.getMessage() + "\n" + e.toString());
					needProcess = false;
				}
			}
			
			tweetCount++;
			if (tweetCount % 100000 == 0) {
				context.setStatus("loaded " + tweetCount + " tweets");
				LOG.info("loaded " + tweetCount + " tweets");
				long expectedSec = tweetCount / ConstantsTruthy.MODERATE_TWEETS_PER_SEC;
				long realSec = (System.currentTimeMillis() - startLoadingTime) / 1000;
				secToWait = expectedSec - realSec;
				if (secToWait > 0) {
					LOG.info("Processing too fast. Slow down to 1tweet/s for " + secToWait + " seconds."); 
				}
			}
		}

		/**
		 * Process all tweets in JSON in the file specified by inputGzPath.
		 * @param inputGzPath
		 * @param context
		 * @throws IOException
		 * @throws FileNotFoundException
		 * @throws UnsupportedEncodingException
		 */
		protected void processJsonGzFile(String inputGzPath, Context context) throws IOException, FileNotFoundException, UnsupportedEncodingException {
			// Each map() is a single line, where the key is the line number
			// Each line is a local file system path to a .json.gz file
			GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(inputGzPath));
			BufferedReader brJson = new BufferedReader(new InputStreamReader(gzInputStream, "UTF-8"));
			String jsonStr = brJson.readLine();
			while (jsonStr != null) {
				jsonStr = jsonStr.trim();
				processJsonString(jsonStr, context);				
				jsonStr = brJson.readLine();
			}
			brJson.close();
		}
		
		/**
		 * Generate HBase puts for tweets in JSON format and output them with mapper's context
		 * @param joTweet
		 * @param context
		 * @throws Exception
		 */
		protected void processJsonTweet(JsonObject joTweet, Context context) throws Exception {
			String tweetIdStr = joTweet.get("id").getAsString();
			byte[] tweetIdBytes = null;
			if (this.useBigInt) {
			    tweetIdBytes = TruthyHelpers.getTweetIdBigIntBytes(tweetIdStr);
			} else {
			    tweetIdBytes = TruthyHelpers.getTweetIdBytes(tweetIdStr);
			}
			 
			Put tweetPut = new Put(tweetIdBytes);
			JsonElement jeRetweet = null;
			JsonElement jeUser = null;
			
			for (Map.Entry<String, JsonElement> e : joTweet.entrySet()) {
				String jeName = e.getKey();
				JsonElement je = e.getValue();
				if (je.isJsonNull()) {
					continue;
				}
				
				ConstantsTruthy.FieldType ft = ConstantsTruthy.fieldTypeMap.get(jeName);
				if (ft != null) {
					switch (ft) {
					case _IGNORE:
						break;
					case FAVORITED:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_FAVORITED_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case TEXT:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_TEXT_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case CREATE_TIME:
						long createTime = ConstantsTruthy.dateFormat.parse(je.getAsString()).getTime();
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES, Bytes.toBytes(createTime));
						break;
					case RETWEET_COUNT:
						char[] rcChars = je.getAsString().toCharArray();
						StringBuilder sb = new StringBuilder();
						for (char c : rcChars) {
							if (Character.isDigit(c)) {
								sb.append(c);
							} else {
								break;
							}
						}
						int retweetCount = Integer.parseInt(sb.toString());
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEET_COUNT_BYTES, Bytes.toBytes(retweetCount));
						break;
					case REPLY_STATUS_ID:
						//byte[] replyStatusIDBytes = TruthyHelpers.getTweetIdBytes(je.getAsString());
					    String replyStatusIDStr = je.getAsString();
                        byte[] replyStatusIDBytes = null;
                        if (this.useBigInt) {
                            replyStatusIDBytes = TruthyHelpers
                                    .getTweetIdBigIntBytes(replyStatusIDStr);
                        } else {
                            replyStatusIDBytes = TruthyHelpers
                                    .getTweetIdBytes(replyStatusIDStr);
                        }						
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_REPLY_STATUS_ID_BYTES, replyStatusIDBytes);
						break;
					case REPLY_SCREEN_NAME:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_REPLY_SNAME_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case CONTRIBUTORS:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CONTRIBUTORS_BYTES, Bytes.toBytes(je.toString()));
						break;
					case RETWEETED:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEETED_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case SOURCE:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_SOURCE_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case REPLY_USER_ID:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_REPLY_USER_ID_BYTES,
								TruthyHelpers.getUserIdBytes(je.getAsString()));
						break;
					case RETWEET_STATUS:
						String retweetIdStr = je.getAsJsonObject().get("id").getAsString();
                        byte[] retweetIdBytes = null;
                        if (this.useBigInt) {
                            retweetIdBytes = TruthyHelpers
                                    .getTweetIdBigIntBytes(retweetIdStr);
                        } else {
                            retweetIdBytes = TruthyHelpers
                                    .getTweetIdBytes(retweetIdStr);
                        }						
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEET_ID_BYTES,
						        retweetIdBytes);
						jeRetweet = je;
						
						String retweetUserIdStr = je.getAsJsonObject().get("user").getAsJsonObject().get("id").getAsString();
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_RETWEET_UID_BYTES, 
								TruthyHelpers.getUserIdBytes(retweetUserIdStr));
						break;
					case ENTITIES:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_ENTITIES_BYTES, Bytes.toBytes(je.toString()));
						break;
					case COORDINATES:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_COORDINATES_BYTES, Bytes.toBytes(je.toString()));
						break;
					case GEO:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_GEO_BYTES, Bytes.toBytes(je.toString()));
						break;
					case USER:
						String userIdStr = je.getAsJsonObject().get("id").getAsString();
						String userSnameStr = je.getAsJsonObject().get("screen_name").getAsString();
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_ID_BYTES,
								TruthyHelpers.getUserIdBytes(userIdStr));
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_USER_SNAME_BYTES, Bytes.toBytes(userSnameStr));
						jeUser = je;
						break;
					case TRUNCATED:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_TRUNCATED_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case PLACE:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PLACE_BYTES, Bytes.toBytes(je.toString()));
						break;
					case LANGUAGE:
					    tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_LANGUAGE_BYTES, Bytes.toBytes(je.toString()));
					    break;
					default:
						tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, Bytes.toBytes(jeName), Bytes.toBytes(je.toString()));
						break;
					}
				} else {
					tweetPut.add(ConstantsTruthy.CF_DETAIL_BYTES, Bytes.toBytes(jeName), Bytes.toBytes(je.toString()));
				}
			}
			
			context.write(new ImmutableBytesWritable(tweetTableBytes), tweetPut);
			
			if (toIndex) {
				gci.index(tweetTableName, tweetPut, context);
			}
			if (jeUser != null) {
				processJsonUser(jeUser.getAsJsonObject(), tweetIdBytes, context);
			}
			if (jeRetweet != null) {
				processJsonTweet(jeRetweet.getAsJsonObject(), context);
			}
		}

		/**
		 * Generate HBase puts for users in JSON format and output them with mapper's context
		 * @param joUser
		 * @param tweetIdBytes
		 * @param context
		 */
		protected void processJsonUser(JsonObject joUser, byte[] tweetIdBytes, Context context) throws Exception {
			String userIdStr = joUser.get("id").getAsString();
			String userSnameStr = null;
			byte[] userIdBytes = TruthyHelpers.getUserIdBytes(userIdStr);
			byte[] userSnameBytes = null;
			long createTime = 0;
			     
			byte[] rowKey = TruthyHelpers.combineBytes(userIdBytes, tweetIdBytes);
			Put userPut = new Put(rowKey);
			for (Map.Entry<String, JsonElement> e : joUser.entrySet()) {
				String jeName = e.getKey();
				JsonElement je = e.getValue();
				if (je.isJsonNull()) {
					continue;
				}
				
				ConstantsTruthy.FieldType ft = ConstantsTruthy.fieldTypeMap.get(jeName);
				if (ft != null) {
					switch (ft) {
					case _IGNORE:
						break;
					case CREATE_TIME:
						createTime = ConstantsTruthy.dateFormat.parse(je.getAsString()).getTime();
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CREATE_TIME_BYTES, Bytes.toBytes(createTime));
						break;
					case FOLLOW_REQ_SENT:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_FOLLOW_REQ_SENT_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case FOLLOWING:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_FOLLOWING_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case FAVORITES_COUNT:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_FAVORITES_COUNT_BYTES, Bytes.toBytes(je.getAsLong()));
						break;
						// TODO we shouldn't include FRIENDS_COUNT in this loading progress, it will case very heavy loading to reload 
						// all the user information from 2010 to 2015
						// reason: FRIENDS_COUNT wasn't exist in ConstantsTruthy.fieldTypeMap before 09/21/2015
						// so, it's stored general untargeted fields with type of String in the later default section in this switch-case 
//					case FRIENDS_COUNT:
//						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_FRIENDS_COUNT_BYTES, Bytes.toBytes(je.getAsLong()));
//						break;
					case URL:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_URL_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case DESCRIPTION:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_DESCRIPTION_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case FOLLOWERS_COUNT:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_FOLLOWERS_COUNT_BYTES, Bytes.toBytes(je.getAsLong()));
						break;
					case INLINE_MEDIA:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_INLINE_MEDIA_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case LISTED_COUNT:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_LISTED_COUNT_BYTES, Bytes.toBytes(je.getAsLong()));
						break;
					case GEO_ENABLED:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_GEO_ENABLED_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case CONTRIBUTORS_ENABLED:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_CONTRIBUTORS_ENABLED_BYTES, 
								Bytes.toBytes(je.getAsBoolean()));
						break;
					case VERIFIED:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_VERIFIED_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case SCREEN_NAME:
						userSnameStr = je.getAsString();
						userSnameBytes = Bytes.toBytes(userSnameStr);
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_SCREEN_NAME_BYTES, userSnameBytes);
						break;
					case TIME_ZONE:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_TIME_ZONE_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case PROTECTED:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PROTECTED_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case IS_TRANSLATOR:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_IS_TRANSLATOR_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case STATUS_COUNT:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_STATUS_COUNT_BYTES, Bytes.toBytes(je.getAsLong()));
						break;
					case LOCATION:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_LOCATION_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case NAME:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_NAME_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case LANGUAGE:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_LANGUAGE_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case NOTIFICATIONS:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_NOTIFICAIONS_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case UTC_OFFSET:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_UTC_OFFSET_BYTES, Bytes.toBytes(je.getAsLong()));
						break;
					case PROFILE_LINK_COLOR:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PLINK_COLOR_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case PROFILE_SIDEBORDER_COLOR:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PSDB_COLOR_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case PROFILE_USE_BGIMG:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PUSE_BGIMG_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case DEFAULT_PROFILE:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_DEF_PROF_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case PROFILE_BG_COLOR:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PBG_COLOR_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case PROFILE_IMG_URL:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PIMG_URL_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case PROFILE_BGIMG_URL:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PBGIMG_URL_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case PROFILE_TEXT_COLOR:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PTEXT_COLOR_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case PROFILE_SIDEFILL_COLOR:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PSDF_COLOR_BYTES, Bytes.toBytes(je.getAsString()));
						break;
					case DEFUALT_PROFILE_IMG:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_DEF_PIMG_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					case PROFILE_BG_TILE:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, ConstantsTruthy.QUAL_PBG_TILE_BYTES, Bytes.toBytes(je.getAsBoolean()));
						break;
					default:
						userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, Bytes.toBytes(jeName), Bytes.toBytes(je.toString()));
						break;
					}
				} else {
					userPut.add(ConstantsTruthy.CF_DETAIL_BYTES, Bytes.toBytes(jeName), Bytes.toBytes(je.toString()));
				}
			}
			
			context.write(new ImmutableBytesWritable(userTableBytes), userPut);
			
			if (toIndex) {
				gci.index(userTableName, userPut, context);
			}
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			tweetCount = 0;
			toIndex = conf.get("index.or.not").equalsIgnoreCase("index");
			inputType = conf.get("input.file.type").equalsIgnoreCase("filelist") ? InputFileType.FILE_LIST : InputFileType.JSON_CONTENT;
			month = conf.get("month");
			// check if this month is after 2015-06
			this.useBigInt = TruthyHelpers.checkIfb4June2015(this.month);
			String indexConfigPath = conf.get("index.config.path");
			if (toIndex) {
				try {
					gci = new GeneralCustomIndexer(indexConfigPath, null);
				} catch (Exception e) {
					e.printStackTrace();
					throw new IOException("Failed to initialize the general custom indexer: " + e.getMessage());
				}
			}
			tweetTableName = ConstantsTruthy.TWEET_TABLE_NAME + "-" + month;
			userTableName = ConstantsTruthy.USER_TABLE_NAME + "-" + month;			
			tweetTableBytes = Bytes.toBytes(tweetTableName);
			userTableBytes = Bytes.toBytes(userTableName);
			random = new Random();
			lastSleepTime = 0;
			lastSleepLenInMin = 5;
			calTmp = Calendar.getInstance();
			startLoadingTime = System.currentTimeMillis();
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.setStatus("loaded " + tweetCount + " tweets");
		}
	}
	
	/**
	 * Generate memes string from an "entities" JSON element.
	 * @param jeEntities
	 * @return
	 */
	public static String getMemesFromEntities(JsonElement jeEntities) {
		JsonArray jaUserMentions = jeEntities.getAsJsonObject().get("user_mentions").getAsJsonArray();
		JsonArray jaHashTags = jeEntities.getAsJsonObject().get("hashtags").getAsJsonArray();
		JsonArray jaUrls = jeEntities.getAsJsonObject().get("urls").getAsJsonArray();
		
		StringBuilder sb = new StringBuilder();
		if (!jaUserMentions.isJsonNull() && jaUserMentions.size() > 0) {
			Iterator<JsonElement> ium = jaUserMentions.iterator();
			while (ium.hasNext()) {
				JsonObject jomu = ium.next().getAsJsonObject();
				sb = sb.append('@').append(jomu.get("screen_name").getAsString());
				JsonElement jeId = jomu.get("id");
				if (jeId != null && !jeId.isJsonNull()) {
					sb = sb.append('(').append(jeId.getAsString()).append(')');
				}
				sb = sb.append('\t');
			}
		}
		if (!jaHashTags.isJsonNull() && jaHashTags.size() > 0) {
			Iterator<JsonElement> iht = jaHashTags.iterator();
			while (iht.hasNext()) {
				sb = sb.append('#').append(iht.next().getAsJsonObject().get("text").getAsString());
				sb = sb.append('\t');
			}
		}
		if (!jaUrls.isJsonNull() && jaUrls.size() > 0) {
			Iterator<JsonElement> iurl = jaUrls.iterator();
			while (iurl.hasNext()) {
				JsonObject joUrl = iurl.next().getAsJsonObject();
				JsonElement jeChildUrl = joUrl.get("url");
				JsonElement jeChildEurl = joUrl.getAsJsonObject().get("expanded_url");
				if (jeChildUrl != null && !jeChildUrl.isJsonNull()) {
					sb = sb.append(jeChildUrl.getAsString()).append('\t');
				}
				if (jeChildEurl != null && !jeChildEurl.isJsonNull()) {
					sb = sb.append(jeChildEurl.getAsString()).append('\t');
				}
			}
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		String memes = sb.toString();
		return memes;
	}

	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		Path inputPath = new Path(args[0]);
		conf.set("index.or.not", args[1]);
		conf.set("input.file.type", args[2]);
		conf.set("month", args[3]);
		if (args.length > 4) {
			conf.set("index.config.path", args[4]);
		}
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		Job job = Job.getInstance(conf, "Truthy data loader with GeneralCustomIndexer");
		job.setJarByClass(TgdlMapper.class);
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TgdlMapper.class);
		job.setOutputFormatClass(DataPreserveMultiTableOutputFormat.class);
		job.setNumReduceTasks(0);
		TableMapReduceUtil.addDependencyJars(job);
		
		return job;
	}
	
	/**
	 * Check if all HBase tables for the given month have been created.
	 * @param conf
	 * @param month
	 * @return
	 * @throws Exception
	 */
	public static boolean checkTableExist(Configuration conf, String month) throws Exception {
		String[] tableNames = {ConstantsTruthy.TWEET_TABLE_NAME, ConstantsTruthy.USER_TABLE_NAME, ConstantsTruthy.USER_TWEETS_TABLE_NAME,
				ConstantsTruthy.TEXT_INDEX_TABLE_NAME, ConstantsTruthy.MEME_INDEX_TABLE_NAME, ConstantsTruthy.SNAME_INDEX_TABLE_NAME,
				ConstantsTruthy.RETWEET_INDEX_TABLE_NAME};
		HBaseAdmin admin = new HBaseAdmin(conf);		
		for (String tn : tableNames) {
			String tnm = tn + "-" + month;
			if (!admin.tableExists(tnm)) {
				System.err.println("Table " + tnm + " does not exist.");
				admin.close();
				return false;
			}
		}
		admin.close();
		return true;
	}

	/**
	 * Main entry point.
	 * @param args The command line parameters.
	 * @throws Exception When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Wrong number of arguments: " + otherArgs.length);
			System.err.println("Usage: TruthyDataLoaderWithGci <input directory> <'index' or 'noindex' when loading> "
					+ "<'filelist' or 'jsoncontent' for input value type> <month> [<path to index config if 'index' is enabled>]");
			System.exit(-1);
		} else {
			String indexOpt = otherArgs[1];
			if (indexOpt.equalsIgnoreCase("index") && otherArgs.length < 5) {
				System.err.println("Wrong number of arguments: " + otherArgs.length);
				System.err.println("Usage: TruthyDataLoaderWithGci <input directory> <'index' or 'noindex' when loading> "
						+ "<'filelist' or 'jsoncontent' for input value type> <month> [<path to index config if 'index' is enabled>]");
				System.exit(-1);
			}
		}
		
		String month = otherArgs[3];
		if (!checkTableExist(conf, month)) {
			System.err.println();
			System.err.println("Error: Table existence check failed. Please make sure all tables for " + month + " have been created first.");
			System.exit(1);
		}
		
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
