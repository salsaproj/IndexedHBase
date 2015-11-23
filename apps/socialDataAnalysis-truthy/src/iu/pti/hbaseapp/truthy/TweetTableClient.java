package iu.pti.hbaseapp.truthy;

import iu.pti.hbaseapp.GeneralHelpers;
import iu.pti.hbaseapp.truthy.ConstantsTruthy.EXISTING_FIELD_QUALIFIER;
import iu.pti.hbaseapp.truthy.ConstantsTruthy.FieldType;
import iu.pti.hbaseapp.truthy.TweetData.TweetUserData;

import java.io.Closeable;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.Gson;

public class TweetTableClient implements Closeable {
	HTable tweetTable = null;
	HTable userTable = null;
	String month = null;
	Date tmpDate = null;
	boolean useBigInt = false;

    public TweetTableClient(String tableName) throws Exception {
		int idx = tableName.indexOf('-');
		if (idx < 0) {
			throw new Exception("Invalid table name " + tableName);
		}
		month = tableName.substring(idx + 1);
		Configuration conf = HBaseConfiguration.create();
		tweetTable = new HTable(conf, tableName);
		tmpDate = new Date();
		this.useBigInt = TruthyHelpers.checkIfb4June2015(this.month);
	}
	
	public TweetTableClient(MapContext<?, ?, ?, ?> context) throws Exception {
		Configuration conf = context.getConfiguration();
		String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
		int idx = inputFileName.indexOf('_');
		month = inputFileName.substring(0, idx);
		tweetTable = new HTable(conf, ConstantsTruthy.TWEET_TABLE_NAME + "-" + month);
		tmpDate = new Date();
		this.useBigInt = TruthyHelpers.checkIfb4June2015(this.month);
	}
	
	@Override
	public void close() throws IOException {
		if (tweetTable != null) {
			tweetTable.close();
		}
		if (userTable != null) {
			userTable.close();
		}
	}

    public boolean isUseBigInt() {
        return useBigInt;
    }
	
	/**
	 * Make a TweetData object by accessing the tweet table with the given tweetId.
	 * @param tweetId
	 * @param includeUser whether the user information is required in the result.
	 * @param includeRetweeted whether the retweeted status information is required in the result.
	 * @return
	 * @throws Exception
	 */
	public TweetData getTweetData(String tweetId, boolean includeUser, boolean includeRetweeted) throws Exception {
		TweetData result = new TweetData();
		result.id_str = tweetId;
		JSONObject tmpOtherFields = new JSONObject();
		byte[] tweetIdBytes = null;
		if (this.useBigInt) {
		    tweetIdBytes = TruthyHelpers.getTweetIdBigIntBytes(tweetId);
		} else {
		    tweetIdBytes = TruthyHelpers.getTweetIdBytes(tweetId);
		}
		byte[] cfdBytes = ConstantsTruthy.CF_DETAIL_BYTES;
		Get get = new Get(tweetIdBytes);
		Result r = tweetTable.get(get);
		if (r == null) {
			throw new Exception ("Can't find tweet for ID " + tweetId);
		}
		
		byte[] favBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_FAVORITED_BYTES);
		if (favBytes != null) {
			result.favorited = Bytes.toBoolean(favBytes);
		}
		byte[] textBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_TEXT_BYTES);
		if (textBytes != null) {
			result.text = Bytes.toString(textBytes);
		}
		byte[] timeBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_CREATE_TIME_BYTES);
		if (timeBytes != null) {
			tmpDate.setTime(Bytes.toLong(timeBytes));
			result.created_at = ConstantsTruthy.dateFormat.format(tmpDate);
		}		
		byte[] rcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_RETWEET_COUNT_BYTES);
		if (rcBytes != null) {
			result.retweet_count = Bytes.toInt(rcBytes);
		}		
		byte[] rsiBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_REPLY_STATUS_ID_BYTES);
        if (rsiBytes != null) {
            if (this.useBigInt) {
                result.in_reply_to_status_id_str = TruthyHelpers
                        .getTweetIDStrFromBigIntBytes(rsiBytes);
            } else {
                result.in_reply_to_status_id_str = TruthyHelpers
                        .getTweetIDStrFromBytes(rsiBytes);
            }
        }	
		byte[] rsnBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_REPLY_SNAME_BYTES);
		if (rsnBytes != null) {
			result.in_reply_to_screen_name = Bytes.toString(rsnBytes);
		}		
		byte[] ctbBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_CONTRIBUTORS_BYTES);
		if (ctbBytes != null) {
			result.contributors = ConstantsTruthy.jsonParser.parse(Bytes.toString(ctbBytes));
		}		
		byte[] rtdBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_RETWEETED_BYTES);
		if (rtdBytes != null) {
			result.retweeted = Bytes.toBoolean(rtdBytes);
		}
		byte[] srcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_SOURCE_BYTES);
		if (srcBytes != null) {
			result.source = Bytes.toString(srcBytes);
		}		
		byte[] ruiBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_REPLY_USER_ID_BYTES);
		if (ruiBytes != null) {
			result.in_reply_to_user_id_str = TruthyHelpers.getUserIDStrFromBytes(ruiBytes);
		}		
		byte[] rtIdBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_RETWEET_ID_BYTES);
        if (rtIdBytes != null) {
            if (this.useBigInt) {
                result.retweeted_status_id_str = TruthyHelpers
                        .getTweetIDStrFromBigIntBytes(rtIdBytes);
            } else {
                result.retweeted_status_id_str = TruthyHelpers
                        .getTweetIDStrFromBytes(rtIdBytes);
            }
        }	
		byte[] rtUidBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_RETWEET_UID_BYTES);
		if (rtUidBytes != null) {
			result.retweeted_status_user_id_str = TruthyHelpers.getUserIDStrFromBytes(rtUidBytes);
		}
		byte[] entitiesBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_ENTITIES_BYTES);
		if (entitiesBytes != null){
			result.entities = ConstantsTruthy.jsonParser.parse(Bytes.toString(entitiesBytes));
		}				
		byte[] coordBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_COORDINATES_BYTES);
		if (coordBytes != null) {
			result.coordinates = ConstantsTruthy.jsonParser.parse(Bytes.toString(coordBytes));
		}		
		byte[] geoBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_GEO_BYTES);
		if (geoBytes != null) {
			result.geo = ConstantsTruthy.jsonParser.parse(Bytes.toString(geoBytes));
		}		
		byte[] uidBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_USER_ID_BYTES);
		if (uidBytes != null) {
			result.user_id_str = TruthyHelpers.getUserIDStrFromBytes(uidBytes);
		}		
		byte[] usnBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_USER_SNAME_BYTES);
		if (usnBytes != null) {
			result.user_screen_name = Bytes.toString(usnBytes);
		}
		byte[] tcdBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_TRUNCATED_BYTES);
		if (tcdBytes != null) {
			result.truncated = Bytes.toBoolean(tcdBytes);
		}		
		byte[] placeBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PLACE_BYTES);
		if (placeBytes != null) {
			result.place = ConstantsTruthy.jsonParser.parse(Bytes.toString(placeBytes));
		}
		
		// get all other fields without specified in the above section
		// TODO performance may be slower, we may need a flag to turn this one or off
		NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(cfdBytes);
        for (byte[] quantiferInByte : familyMap.keySet()) {
            String quantiferName = Bytes.toString(quantiferInByte);
            if (!this.isExistInConstantsTruthy(quantiferName)) {
                if (tmpOtherFields == null) {
                    tmpOtherFields = new JSONObject();
                }
                tmpOtherFields.put(quantiferName, Bytes.toString(r.getValue(cfdBytes, quantiferInByte)));
            }
        }        
        if (tmpOtherFields != null) {
            result.otherfields = ConstantsTruthy.jsonParser.parse(tmpOtherFields.toString().replace("\\\"", ""));
        }
		
		if (includeUser && uidBytes != null) {
			result.user = getUserData(uidBytes, tweetIdBytes);
		}
		if (includeRetweeted && rtIdBytes != null) {
			result.retweeted_status = getTweetData(result.retweeted_status_id_str, includeUser, false);
		}		
		
		return result;
	}
	
	private boolean isExistInConstantsTruthy(String quantiferName) {
	    EXISTING_FIELD_QUALIFIER efq = ConstantsTruthy.existingFieldMap.get(quantiferName);
	    return efq == null? false: true;
    }

    /**
	 * Get all fields of a TweetUserData object by accessing the a user table, using userIdBytes as the row key. 
	 * 
	 * @param userIdBytes
	 * @param tweetIdBytes
	 * @return
	 * @throws Exception
	 */
	public TweetUserData getUserData(byte[] userIdBytes, byte[] tweetIdBytes) throws Exception {
		if (userTable == null) {
			userTable = new HTable(tweetTable.getConfiguration(), ConstantsTruthy.USER_TABLE_NAME + "-" + month);
		}
		
		String userId = TruthyHelpers.getUserIDStrFromBytes(userIdBytes);
		TweetUserData result = new TweetUserData();
		result.id_str = userId;
		byte[] rowKey = TruthyHelpers.combineBytes(userIdBytes, tweetIdBytes);
		byte[] cfdBytes = ConstantsTruthy.CF_DETAIL_BYTES;
		Get get = new Get(rowKey);
		Result r = userTable.get(get);
		if (r == null) {
			throw new Exception ("Can't find user information for user ID " + userId);
		}
		
		byte[] timeBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_CREATE_TIME_BYTES);
		if (timeBytes != null) {
			tmpDate.setTime(Bytes.toLong(timeBytes));
			result.created_at = ConstantsTruthy.dateFormat.format(tmpDate);
		}
		byte[] frsBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_FOLLOW_REQ_SENT_BYTES);
		if (frsBytes != null) {
			result.follow_request_sent = Bytes.toBoolean(frsBytes);
		}		
		byte[] flBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_FOLLOWING_BYTES);
		if (flBytes != null) {
			result.following = Bytes.toBoolean(flBytes);
		}		
		byte[] fvcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_FAVORITES_COUNT_BYTES);
		if (fvcBytes != null) {
			result.favourites_count = Bytes.toLong(fvcBytes);
		}
		// special handling for friends_count, because we didn't add friends_count as fast processing
		// field in ConstantsTruthy.fieldTypeMap
		byte[] fdcBytes = r.getValue(cfdBytes, Bytes.toBytes("friends_count"));
		if (fdcBytes != null) {
			result.friends_count = Long.valueOf(Bytes.toString(fdcBytes));
		}
		byte[] urlBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_URL_BYTES);
		if (urlBytes != null) {
			result.url = Bytes.toString(urlBytes);
		}
		byte[] desBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_DESCRIPTION_BYTES);
		if (desBytes != null) {
			result.description = Bytes.toString(desBytes);
		}
		byte[] frcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_FOLLOWERS_COUNT_BYTES);
		if (frcBytes != null) {
			result.followers_count = Bytes.toLong(frcBytes);
		}
		byte[] ilmBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_INLINE_MEDIA_BYTES);
		if (ilmBytes != null) {
			result.show_all_inline_media = Bytes.toBoolean(ilmBytes);
		}
		byte[] lscBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_LISTED_COUNT_BYTES);
		if (lscBytes != null) {
			result.listed_count = Bytes.toLong(lscBytes);
		}
		byte[] gebBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_GEO_ENABLED_BYTES);
		if (gebBytes != null) {
			result.geo_enabled = Bytes.toBoolean(gebBytes);
		}
		byte[] ctebBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_CONTRIBUTORS_ENABLED_BYTES);
		if (ctebBytes != null) {
			result.contributors_enabled = Bytes.toBoolean(ctebBytes);
		}
		byte[] vfdBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_VERIFIED_BYTES);
		if (vfdBytes != null) {
			result.verified = Bytes.toBoolean(vfdBytes);
		}
		byte[] snBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_SCREEN_NAME_BYTES);
		if (snBytes != null) {
			result.screen_name = Bytes.toString(snBytes);
		}
		byte[] tzBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_TIME_ZONE_BYTES);
		if (tzBytes != null) {
			result.time_zone = Bytes.toString(tzBytes);
		}
		byte[] ptdBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PROTECTED_BYTES);
		if (ptdBytes != null) {
			result.is_protected = Bytes.toBoolean(ptdBytes);
		}
		byte[] isTrBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_IS_TRANSLATOR_BYTES);
		if (isTrBytes != null) {
			result.is_translator = Bytes.toBoolean(isTrBytes);
		}
		byte[] scBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_STATUS_COUNT_BYTES);
		if (scBytes != null) {
			result.statuses_count = Bytes.toLong(scBytes);
		}
		byte[] locBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_LOCATION_BYTES);
		if (locBytes != null) {
			result.location = Bytes.toString(locBytes);
		}
		byte[] nameBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_NAME_BYTES);
		if (nameBytes != null) {
			result.name = Bytes.toString(nameBytes);
		}
		byte[] langBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_LANGUAGE_BYTES);
		if (langBytes != null) {
			result.lang = Bytes.toString(langBytes);
		}
		byte[] ntfBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_NOTIFICAIONS_BYTES);
		if (ntfBytes != null) {
			result.notifications = Bytes.toBoolean(ntfBytes);
		}
		byte[] uoBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_UTC_OFFSET_BYTES);
		if (uoBytes != null) {
			result.utc_offset = Bytes.toLong(uoBytes);
		}
		byte[] plcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PLINK_COLOR_BYTES);
		if (plcBytes != null) {
			result.profile_link_color = Bytes.toString(plcBytes);
		}
		byte[] psbcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PSDB_COLOR_BYTES);
		if (psbcBytes != null) {
			result.profile_sidebar_border_color = Bytes.toString(psbcBytes);
		}
		byte[] pubiBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PUSE_BGIMG_BYTES);
		if (pubiBytes != null) {
			result.profile_use_background_image = Bytes.toBoolean(pubiBytes);
		}
		byte[] dpBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_DEF_PROF_BYTES);
		if (dpBytes != null) {
			result.default_profile = Bytes.toBoolean(dpBytes);
		}
		byte[] pbgcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PBG_COLOR_BYTES);
		if (pbgcBytes != null) {
			result.profile_background_color = Bytes.toString(pbgcBytes);
		}
		byte[] piuBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PIMG_URL_BYTES);
		if (piuBytes != null) {
			result.profile_image_url = Bytes.toString(piuBytes);
		}
		byte[] pbiuBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PBGIMG_URL_BYTES);
		if (pbiuBytes != null) {
			result.profile_background_image_url = Bytes.toString(pbiuBytes);
		}
		byte[] ptcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PTEXT_COLOR_BYTES);
		if (ptcBytes != null) {
			result.profile_text_color = Bytes.toString(ptcBytes);
		}
		byte[] psfcBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PSDF_COLOR_BYTES);
		if (psfcBytes != null) {
			result.profile_sidebar_fill_color = Bytes.toString(psfcBytes);
		}
		byte[] dpiBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_DEF_PIMG_BYTES);
		if (dpiBytes != null) {
			result.default_profile_image = Bytes.toBoolean(dpiBytes);
		}
		byte[] pbtBytes = r.getValue(cfdBytes, ConstantsTruthy.QUAL_PBG_TILE_BYTES);
		if (pbtBytes != null) {
			result.profile_background_tile = Bytes.toBoolean(pbtBytes);
		}
		
		return result;
	}

}
