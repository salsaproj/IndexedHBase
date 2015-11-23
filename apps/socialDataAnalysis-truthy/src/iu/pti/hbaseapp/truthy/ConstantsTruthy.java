package iu.pti.hbaseapp.truthy;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.JsonParser;

public class ConstantsTruthy {
	// table names
	/** rowkey: tweet id; [cf: details; columns: fields like text, user id, hashtags, etc.] */
	public static final String TWEET_TABLE_NAME = "tweetTable";
	
	/** rowkey: user id followed by tweet id; [cf: details; columns: fields like user name, created time, etc.] */
	public static final String USER_TABLE_NAME = "userTable";
	
	/** rowkey: user id; [cf: tweets; columns: tweet ids.] */
	public static final String USER_TWEETS_TABLE_NAME = "userTweetsIndexTable";
	
	/** rowkey: keyword; [cf: tweets; columns: tweet ids.] */
	public static final String TEXT_INDEX_TABLE_NAME = "textIndexTable";
	
	/** rowkey: meme; [cf: tweets; columns: tweet ids.] */
	public static final String MEME_INDEX_TABLE_NAME = "memeIndexTable";
	
	/** rowkey: tweet id; [cf: tweets; columns: retweet ids.] */
	public static final String RETWEET_INDEX_TABLE_NAME = "retweetIndexTable";
	
	/** rowkey: timestamp; [cf: tweets; columns: tweet ids.] */
	public static final String TIME_INDEX_TABLE_NAME = "timeIndexTable";
	
	/** rowkey: screen name; [cf: users; columns: user ids; cell values: user names] */
	public static final String SNAME_INDEX_TABLE_NAME = "snameIndexTable";
	
	/** rowkey: meme; [cf: tweets; columns: tweet ids; cell values: user ids] */
	public static final String MEME_USER_INDEX_TABLE_NAME = "memeUserIndexTable";
	
	/** rowkey: geo (longitude, latitude); [cf: tweets; columns: tweet ids.] */
	public static final String GEO_INDEX_TABLE_NAME = "geoIndexTable";
	
	/** rowkey: geo; [cf: tweets; columns: tweet ids; cell values: user ids] */
	public static final String GEO_USER_INDEX_TABLE_NAME = "geoUserIndexTable";

	
	public static final byte[] TWEET_TABLE_BYTES = Bytes.toBytes(TWEET_TABLE_NAME);
	public static final byte[] USER_TABLE_BYTES = Bytes.toBytes(USER_TABLE_NAME);
	public static final byte[] USER_TWEETS_TABLE_BYTES = Bytes.toBytes(USER_TWEETS_TABLE_NAME);
	public static final byte[] TEXT_INDEX_TABLE_BYTES = Bytes.toBytes(TEXT_INDEX_TABLE_NAME);
	public static final byte[] MEME_INDEX_TABLE_BYTES = Bytes.toBytes(MEME_INDEX_TABLE_NAME);
	public static final byte[] RETWEET_INDEX_TABLE_BYTES = Bytes.toBytes(RETWEET_INDEX_TABLE_NAME);
	public static final byte[] SNAME_INDEX_TABLE_BYTES = Bytes.toBytes(SNAME_INDEX_TABLE_NAME);
	public static final byte[] MEME_USER_INDEX_TABLE_BYTES = Bytes.toBytes(MEME_USER_INDEX_TABLE_NAME);
	public static final byte[] GEO_INDEX_TABLE_BYTES = Bytes.toBytes(GEO_INDEX_TABLE_NAME);
	public static final byte[] GEO_USER_INDEX_TABLE_BYTES = Bytes.toBytes(GEO_USER_INDEX_TABLE_NAME);
	
	// column family names
	public static final String COLUMN_FAMILY_DETAIL = "d";
	public static final String COLUMN_FAMILY_TWEETS = "t";
	public static final String COLUMN_FAMILY_USERS = "u";
	
	public static final byte[] CF_DETAIL_BYTES = Bytes.toBytes(COLUMN_FAMILY_DETAIL);
	public static final byte[] CF_TWEETS_BYTES = Bytes.toBytes(COLUMN_FAMILY_TWEETS);
	public static final byte[] CF_USERS_BYTES = Bytes.toBytes(COLUMN_FAMILY_USERS);
	
	// column names
	public static final String QUALIFIER_FAVORITED = "fa";
	public static final String QUALIFIER_TEXT = "tx";
	public static final String QUALIFIER_CREATE_TIME = "ct";
	public static final String QUALIFIER_RETWEET_COUNT = "rc";
	public static final String QUALIFIER_REPLY_STATUS_ID = "rs";
	public static final String QUALIFIER_REPLY_SCREEN_NAME = "rn";
	public static final String QUALIFIER_CONTRIBUTORS = "cr";
	public static final String QUALIFIER_RETWEETED = "rd";
	public static final String QUALIFIER_REPLY_USER_ID = "pu";
	public static final String QUALIFIER_SOURCE = "s";
	public static final String QUALIFIER_RETWEET_ID = "rt";
	public static final String QUALIFIER_RETWEET_UID = "ru";
	public static final String QUALIFIER_ENTITIES = "en";
	public static final String QUALIFIER_COORDINATES = "co";
	public static final String QUALIFIER_GEO = "g";
	public static final String QUALIFIER_USER_ID = "ui";
	public static final String QUALIFIER_USER_SCREEN_NAME = "un";
	public static final String QUALIFIER_TRUNCATED = "tn";
	public static final String QUALIFIER_PLACE = "pl";
	public static final String QUALIFIER_FOLLOW_REQ_SENT = "fs";
	public static final String QUALIFIER_FOLLOWING = "fg";
	public static final String QUALIFIER_FAVORITES_COUNT = "vc";
	public static final String QUALIFIER_FRIENDS_COUNT = "ec";
	public static final String QUALIFIER_URL = "r";
	public static final String QUALIFIER_DESCRIPTION = "dp";
	public static final String QUALIFIER_FOLLOWERS_COUNT = "fc";
	public static final String QUALIFIER_INLINE_MEDIA = "im";
	public static final String QUALIFIER_LISTED_COUNT = "lc";
	public static final String QUALIFIER_GEO_ENABLED = "ge";
	public static final String QUALIFIER_CONTRIBUTORS_ENABLED = "ce";
	public static final String QUALIFIER_VERIFIED = "v";
	public static final String QUALIFIER_SCREEN_NAME = "sn";
	public static final String QUALIFIER_TIME_ZONE = "tz";
	public static final String QUALIFIER_PROTECTED = "pt";
	public static final String QUALIFIER_IS_TRANSLATOR = "tl";
	public static final String QUALIFIER_STATUS_COUNT = "sc";
	public static final String QUALIFIER_LOCATION = "lo";
	public static final String QUALIFIER_NAME = "n";
	public static final String QUALIFIER_LANGUAGE = "l";
	public static final String QUALIFIER_NOTIFICAIONS = "nf";
	public static final String QUALIFIER_UTC_OFFSET = "to";
	public static final String QUALIFIER_PROF_LINK_COLOR = "lr";
	public static final String QUALIFIER_PROF_SIDEBORDER_COLOR = "sr";
	public static final String QUALIFIER_PROF_USE_BGIMG = "bm";
	public static final String QUALIFIER_DEFAULT_PROF = "df";
	public static final String QUALIFIER_PROF_BG_COLOR = "br";
	public static final String QUALIFIER_PROF_IMG_URL = "mu";
	public static final String QUALIFIER_PROF_BGIMG_URL = "bu";
	public static final String QUALIFIER_PROF_TEXT_COLOR = "xr";
	public static final String QUALIFIER_PROF_SIDEFILL_COLOR = "fr";
	public static final String QUALIFIER_DEFAULT_PROF_IMG = "dm";
	public static final String QUALIFIER_PROF_BG_TILE = "bt";
	
	public static final String[] EXISTING_QUALIFIER = {"fa", "tx", "ct", "rc", "rs", "rn", "cr",
	    "rd", "pu", "s", "rt", "ru", "en", "co", "g", "ui", "un", "tn", "pl", "fs", "fg", "vc", 
	    "ec", "r", "dp", "fc", "im", "lc", "ge", "ce", "v", "sn", "tz", "pt", "tl", "sc", "lo", 
	    "n", "l", "nf", "to", "lr", "sr", "bm", "df", "br", "mu", "bu", "xr", "fr", "dm", "bt"};
	
	public enum EXISTING_FIELD_QUALIFIER {fa, tx, ct, rc, rs, rn, cr,
	        rd, pu, s, rt, ru, en, co, g, ui, un, tn, pl, fs, fg, vc, 
	        ec, r, dp, fc, im, lc, ge, ce, v, sn, tz, pt, tl, sc, lo, 
	        n, l, nf, to, lr, sr, bm, df, br, mu, bu, xr, fr, dm, bt};

    /**
     * Map field names of tweets in JSON to defined types for fast processing.
     */
    public static final Map<String, EXISTING_FIELD_QUALIFIER> existingFieldMap;
    static {
        existingFieldMap = new HashMap<String, EXISTING_FIELD_QUALIFIER>();
        existingFieldMap.put("fa", EXISTING_FIELD_QUALIFIER.fa);
        existingFieldMap.put("tx", EXISTING_FIELD_QUALIFIER.tx);
        existingFieldMap.put("ct", EXISTING_FIELD_QUALIFIER.ct);
        existingFieldMap.put("rc", EXISTING_FIELD_QUALIFIER.rc);
        existingFieldMap.put("rs", EXISTING_FIELD_QUALIFIER.rs);
        existingFieldMap.put("rn", EXISTING_FIELD_QUALIFIER.rn);
        existingFieldMap.put("cr", EXISTING_FIELD_QUALIFIER.cr);
        existingFieldMap.put("rd", EXISTING_FIELD_QUALIFIER.rd);
        existingFieldMap.put("pu", EXISTING_FIELD_QUALIFIER.pu);
        existingFieldMap.put("s", EXISTING_FIELD_QUALIFIER.s);
        existingFieldMap.put("rt", EXISTING_FIELD_QUALIFIER.rt);
        existingFieldMap.put("ru", EXISTING_FIELD_QUALIFIER.ru);
        existingFieldMap.put("en", EXISTING_FIELD_QUALIFIER.en);
        existingFieldMap.put("co", EXISTING_FIELD_QUALIFIER.co);
        existingFieldMap.put("g", EXISTING_FIELD_QUALIFIER.g);
        existingFieldMap.put("ui", EXISTING_FIELD_QUALIFIER.ui);
        existingFieldMap.put("un", EXISTING_FIELD_QUALIFIER.un);
        existingFieldMap.put("tn", EXISTING_FIELD_QUALIFIER.tn);
        existingFieldMap.put("pl", EXISTING_FIELD_QUALIFIER.pl);
        existingFieldMap.put("fs", EXISTING_FIELD_QUALIFIER.fs);
        existingFieldMap.put("fg", EXISTING_FIELD_QUALIFIER.fg);
        existingFieldMap.put("vc", EXISTING_FIELD_QUALIFIER.vc);
        existingFieldMap.put("ec", EXISTING_FIELD_QUALIFIER.ec);
        existingFieldMap.put("r", EXISTING_FIELD_QUALIFIER.r);
        existingFieldMap.put("dp", EXISTING_FIELD_QUALIFIER.dp);
        existingFieldMap.put("fc", EXISTING_FIELD_QUALIFIER.fc);
        existingFieldMap.put("im", EXISTING_FIELD_QUALIFIER.im);
        existingFieldMap.put("lc", EXISTING_FIELD_QUALIFIER.lc);
        existingFieldMap.put("ge", EXISTING_FIELD_QUALIFIER.ge);
        existingFieldMap.put("ce", EXISTING_FIELD_QUALIFIER.ce);
        existingFieldMap.put("v", EXISTING_FIELD_QUALIFIER.v);
        existingFieldMap.put("sn", EXISTING_FIELD_QUALIFIER.sn);
        existingFieldMap.put("tz", EXISTING_FIELD_QUALIFIER.tz);
        existingFieldMap.put("pt", EXISTING_FIELD_QUALIFIER.pt);
        existingFieldMap.put("tl", EXISTING_FIELD_QUALIFIER.tl);
        existingFieldMap.put("sc", EXISTING_FIELD_QUALIFIER.sc);
        existingFieldMap.put("lo", EXISTING_FIELD_QUALIFIER.lo);
        existingFieldMap.put("n", EXISTING_FIELD_QUALIFIER.n);
        existingFieldMap.put("l", EXISTING_FIELD_QUALIFIER.l);
        existingFieldMap.put("nf", EXISTING_FIELD_QUALIFIER.nf);
        existingFieldMap.put("to", EXISTING_FIELD_QUALIFIER.to);
        existingFieldMap.put("lr", EXISTING_FIELD_QUALIFIER.lr);
        existingFieldMap.put("sr", EXISTING_FIELD_QUALIFIER.sr);
        existingFieldMap.put("bm", EXISTING_FIELD_QUALIFIER.bm);
        existingFieldMap.put("df", EXISTING_FIELD_QUALIFIER.df);
        existingFieldMap.put("br", EXISTING_FIELD_QUALIFIER.br);
        existingFieldMap.put("mu", EXISTING_FIELD_QUALIFIER.mu);
        existingFieldMap.put("bu", EXISTING_FIELD_QUALIFIER.bu);
        existingFieldMap.put("xr", EXISTING_FIELD_QUALIFIER.xr);
        existingFieldMap.put("fr", EXISTING_FIELD_QUALIFIER.fr);
        existingFieldMap.put("dm", EXISTING_FIELD_QUALIFIER.dm);
        existingFieldMap.put("bt", EXISTING_FIELD_QUALIFIER.bt);
    }       
	
	public static final byte[] QUAL_FAVORITED_BYTES = Bytes.toBytes(QUALIFIER_FAVORITED);
	public static final byte[] QUAL_TEXT_BYTES = Bytes.toBytes(QUALIFIER_TEXT);
	public static final byte[] QUAL_CREATE_TIME_BYTES = Bytes.toBytes(QUALIFIER_CREATE_TIME);
	public static final byte[] QUAL_RETWEET_COUNT_BYTES = Bytes.toBytes(QUALIFIER_RETWEET_COUNT);
	public static final byte[] QUAL_REPLY_STATUS_ID_BYTES = Bytes.toBytes(QUALIFIER_REPLY_STATUS_ID);
	public static final byte[] QUAL_REPLY_SNAME_BYTES = Bytes.toBytes(QUALIFIER_REPLY_SCREEN_NAME);
	public static final byte[] QUAL_CONTRIBUTORS_BYTES = Bytes.toBytes(QUALIFIER_CONTRIBUTORS);
	public static final byte[] QUAL_RETWEETED_BYTES = Bytes.toBytes(QUALIFIER_RETWEETED);
	public static final byte[] QUAL_REPLY_USER_ID_BYTES = Bytes.toBytes(QUALIFIER_REPLY_USER_ID);
	public static final byte[] QUAL_SOURCE_BYTES = Bytes.toBytes(QUALIFIER_SOURCE);
	public static final byte[] QUAL_RETWEET_ID_BYTES = Bytes.toBytes(QUALIFIER_RETWEET_ID);
	public static final byte[] QUAL_RETWEET_UID_BYTES = Bytes.toBytes(QUALIFIER_RETWEET_UID);
	public static final byte[] QUAL_ENTITIES_BYTES = Bytes.toBytes(QUALIFIER_ENTITIES);
	public static final byte[] QUAL_COORDINATES_BYTES = Bytes.toBytes(QUALIFIER_COORDINATES);
	public static final byte[] QUAL_GEO_BYTES = Bytes.toBytes(QUALIFIER_GEO);
	public static final byte[] QUAL_USER_ID_BYTES = Bytes.toBytes(QUALIFIER_USER_ID);
	public static final byte[] QUAL_USER_SNAME_BYTES = Bytes.toBytes(QUALIFIER_USER_SCREEN_NAME);
	public static final byte[] QUAL_TRUNCATED_BYTES = Bytes.toBytes(QUALIFIER_TRUNCATED);
	public static final byte[] QUAL_PLACE_BYTES = Bytes.toBytes(QUALIFIER_PLACE);
	public static final byte[] QUAL_FOLLOW_REQ_SENT_BYTES = Bytes.toBytes(QUALIFIER_FOLLOW_REQ_SENT);
	public static final byte[] QUAL_FOLLOWING_BYTES = Bytes.toBytes(QUALIFIER_FOLLOWING);
	public static final byte[] QUAL_FAVORITES_COUNT_BYTES = Bytes.toBytes(QUALIFIER_FAVORITES_COUNT);
	public static final byte[] QUAL_FRIENDS_COUNT_BYTES = Bytes.toBytes(QUALIFIER_FRIENDS_COUNT);
	public static final byte[] QUAL_URL_BYTES = Bytes.toBytes(QUALIFIER_URL);
	public static final byte[] QUAL_DESCRIPTION_BYTES = Bytes.toBytes(QUALIFIER_DESCRIPTION);
	public static final byte[] QUAL_FOLLOWERS_COUNT_BYTES = Bytes.toBytes(QUALIFIER_FOLLOWERS_COUNT);
	public static final byte[] QUAL_INLINE_MEDIA_BYTES = Bytes.toBytes(QUALIFIER_INLINE_MEDIA);
	public static final byte[] QUAL_LISTED_COUNT_BYTES = Bytes.toBytes(QUALIFIER_LISTED_COUNT);
	public static final byte[] QUAL_GEO_ENABLED_BYTES = Bytes.toBytes(QUALIFIER_GEO_ENABLED);
	public static final byte[] QUAL_CONTRIBUTORS_ENABLED_BYTES = Bytes.toBytes(QUALIFIER_CONTRIBUTORS_ENABLED);
	public static final byte[] QUAL_VERIFIED_BYTES = Bytes.toBytes(QUALIFIER_VERIFIED);
	public static final byte[] QUAL_SCREEN_NAME_BYTES = Bytes.toBytes(QUALIFIER_SCREEN_NAME);
	public static final byte[] QUAL_TIME_ZONE_BYTES = Bytes.toBytes(QUALIFIER_TIME_ZONE);
	public static final byte[] QUAL_PROTECTED_BYTES = Bytes.toBytes(QUALIFIER_PROTECTED);
	public static final byte[] QUAL_IS_TRANSLATOR_BYTES = Bytes.toBytes(QUALIFIER_IS_TRANSLATOR);
	public static final byte[] QUAL_STATUS_COUNT_BYTES = Bytes.toBytes(QUALIFIER_STATUS_COUNT);
	public static final byte[] QUAL_LOCATION_BYTES = Bytes.toBytes(QUALIFIER_LOCATION);
	public static final byte[] QUAL_NAME_BYTES = Bytes.toBytes(QUALIFIER_NAME);
	public static final byte[] QUAL_LANGUAGE_BYTES = Bytes.toBytes(QUALIFIER_LANGUAGE);
	public static final byte[] QUAL_NOTIFICAIONS_BYTES = Bytes.toBytes(QUALIFIER_NOTIFICAIONS);
	public static final byte[] QUAL_UTC_OFFSET_BYTES = Bytes.toBytes(QUALIFIER_UTC_OFFSET);
	public static final byte[] QUAL_PLINK_COLOR_BYTES = Bytes.toBytes(QUALIFIER_PROF_LINK_COLOR);
	public static final byte[] QUAL_PSDB_COLOR_BYTES = Bytes.toBytes(QUALIFIER_PROF_SIDEBORDER_COLOR);
	public static final byte[] QUAL_PUSE_BGIMG_BYTES = Bytes.toBytes(QUALIFIER_PROF_USE_BGIMG);
	public static final byte[] QUAL_DEF_PROF_BYTES = Bytes.toBytes(QUALIFIER_DEFAULT_PROF);
	public static final byte[] QUAL_PBG_COLOR_BYTES = Bytes.toBytes(QUALIFIER_PROF_BG_COLOR);
	public static final byte[] QUAL_PIMG_URL_BYTES = Bytes.toBytes(QUALIFIER_PROF_IMG_URL);
	public static final byte[] QUAL_PBGIMG_URL_BYTES = Bytes.toBytes(QUALIFIER_PROF_BGIMG_URL);
	public static final byte[] QUAL_PTEXT_COLOR_BYTES = Bytes.toBytes(QUALIFIER_PROF_TEXT_COLOR);
	public static final byte[] QUAL_PSDF_COLOR_BYTES = Bytes.toBytes(QUALIFIER_PROF_SIDEFILL_COLOR);
	public static final byte[] QUAL_DEF_PIMG_BYTES = Bytes.toBytes(QUALIFIER_DEFAULT_PROF_IMG);
	public static final byte[] QUAL_PBG_TILE_BYTES = Bytes.toBytes(QUALIFIER_PROF_BG_TILE);
	
	/* BigInteger representation of max value of an unsigned 64-bit integer */
	public static final BigInteger max64bBigInt = new BigInteger("18446744073709551615");
	
	public enum FieldType {
		FAVORITED, TEXT, CREATE_TIME, RETWEET_COUNT, REPLY_STATUS_ID, REPLY_SCREEN_NAME,
		CONTRIBUTORS, RETWEETED, REPLY_USER_ID, SOURCE, RETWEET_STATUS, ENTITIES, COORDINATES, 
		GEO, USER, TRUNCATED, PLACE, FOLLOW_REQ_SENT, FOLLOWING, FAVORITES_COUNT, FRIENDS_COUNT, 
		URL, DESCRIPTION, FOLLOWERS_COUNT, INLINE_MEDIA, LISTED_COUNT, GEO_ENABLED, 
		CONTRIBUTORS_ENABLED, VERIFIED, SCREEN_NAME, TIME_ZONE, PROTECTED, IS_TRANSLATOR, 
		STATUS_COUNT, LOCATION, NAME, LANGUAGE, NOTIFICATIONS, UTC_OFFSET, PROFILE_LINK_COLOR,
		PROFILE_SIDEBORDER_COLOR, PROFILE_USE_BGIMG, DEFAULT_PROFILE, PROFILE_BG_COLOR, 
		PROFILE_IMG_URL, PROFILE_BGIMG_URL, PROFILE_TEXT_COLOR, PROFILE_SIDEFILL_COLOR,
		DEFUALT_PROFILE_IMG, PROFILE_BG_TILE, _IGNORE, USER_ID_STR, OTHERFIELDS;
	}

	// we don't use UserFieldType as we include all the user fields inside FieldType
	@Deprecated
    public enum UserFieldType {
        CREATED_AT, PROFILE_LINK_COLOR, DEFAULT_PROFILE, FOLLOW_REQUEST_SENT, FOLLOWING, FAVOURITES_COUNT,
        FRIENDS_COUNT, PROFILE_SIDEBAR_BORDER_COLOR, URL, PROFILE_USE_BACKGROUND_IMAGE, DESCRIPTION, 
        PROFILE_BACKGROUND_COLOR, FOLLOWERS_COUNT, PROFILE_IMAGE_URL, SHOW_ALL_INLINE_MEDIA, LISTED_COUNT,
        GEO_ENABLED, PROFILE_BACKGROUND_IMAGE_URL, CONTRIBUTORS_ENABLED, VERIFIED, SCREEN_NAME, ID_STR,
        TIME_ZONE, PROFILE_TEXT_COLOR, IS_PROTECTED, IS_TRANSLATOR, STATUSES_COUNT, PROFILE_SIDEBAR_FILL_COLOR,
        LOCATION, NAME, DEFAULT_PROFILE_IMAGE, LANG, PROFILE_BACKGROUND_TILE, NOTIFICATIONS, UTC_OFFSET;
    }	
	
	/**
	 * Map field names of tweets in JSON to defined types for fast processing.
	 */
	public static final Map<String, FieldType> fieldTypeMap;
	static {
		fieldTypeMap = new HashMap<String, FieldType>();
		fieldTypeMap.put("favorited", FieldType.FAVORITED);
		fieldTypeMap.put("text", FieldType.TEXT);
		fieldTypeMap.put("created_at", FieldType.CREATE_TIME);
		fieldTypeMap.put("retweet_count", FieldType.RETWEET_COUNT);
		fieldTypeMap.put("in_reply_to_status_id", FieldType.REPLY_STATUS_ID);
		fieldTypeMap.put("in_reply_to_screen_name", FieldType.REPLY_SCREEN_NAME);
		fieldTypeMap.put("in_reply_to_status_id_str", FieldType._IGNORE);
		fieldTypeMap.put("contributors", FieldType.CONTRIBUTORS);
		fieldTypeMap.put("retweeted", FieldType.RETWEETED);
		fieldTypeMap.put("in_reply_to_user_id", FieldType.REPLY_STATUS_ID);
		fieldTypeMap.put("source", FieldType.SOURCE);
		fieldTypeMap.put("in_reply_to_user_id_str", FieldType._IGNORE);
		fieldTypeMap.put("retweeted_status", FieldType.RETWEET_STATUS);
		fieldTypeMap.put("id_str", FieldType._IGNORE);
		fieldTypeMap.put("entities", FieldType.ENTITIES);
		fieldTypeMap.put("coordinates", FieldType.COORDINATES);
		fieldTypeMap.put("geo", FieldType.GEO);
		fieldTypeMap.put("user", FieldType.USER);
		fieldTypeMap.put("truncated", FieldType.TRUNCATED);
		fieldTypeMap.put("id", FieldType._IGNORE);
		fieldTypeMap.put("user_id_str", FieldType.USER_ID_STR);
		fieldTypeMap.put("place", FieldType.PLACE);
		fieldTypeMap.put("profile_link_color", FieldType.PROFILE_LINK_COLOR);
		fieldTypeMap.put("follow_request_sent", FieldType.FOLLOW_REQ_SENT);
		fieldTypeMap.put("following", FieldType.FOLLOWING);
		fieldTypeMap.put("favourites_count", FieldType.FAVORITES_COUNT);
		fieldTypeMap.put("profile_sidebar_border_color", FieldType.PROFILE_SIDEBORDER_COLOR);
		fieldTypeMap.put("url", FieldType.URL);
		fieldTypeMap.put("profile_use_background_image", FieldType.PROFILE_USE_BGIMG);
		fieldTypeMap.put("description", FieldType.DESCRIPTION);
		fieldTypeMap.put("default_profile", FieldType.DEFAULT_PROFILE);
		fieldTypeMap.put("profile_background_color", FieldType.PROFILE_BG_COLOR);
		fieldTypeMap.put("followers_count", FieldType.FOLLOWERS_COUNT);
		fieldTypeMap.put("friends_count", FieldType.FRIENDS_COUNT);
		fieldTypeMap.put("profile_image_url", FieldType.PROFILE_IMG_URL);
		fieldTypeMap.put("show_all_inline_media", FieldType.INLINE_MEDIA);
		fieldTypeMap.put("listed_count", FieldType.LISTED_COUNT);
		fieldTypeMap.put("geo_enabled", FieldType.GEO_ENABLED);
		fieldTypeMap.put("profile_background_image_url", FieldType.PROFILE_BGIMG_URL);
		fieldTypeMap.put("contributors_enabled", FieldType.CONTRIBUTORS_ENABLED);
		fieldTypeMap.put("verified", FieldType.VERIFIED);
		fieldTypeMap.put("screen_name", FieldType.SCREEN_NAME);
		fieldTypeMap.put("time_zone", FieldType.TIME_ZONE);
		fieldTypeMap.put("profile_text_color", FieldType.PROFILE_TEXT_COLOR);
		fieldTypeMap.put("protected", FieldType.PROTECTED);
		fieldTypeMap.put("is_translator", FieldType.IS_TRANSLATOR);
		fieldTypeMap.put("statuses_count", FieldType.STATUS_COUNT);
		fieldTypeMap.put("profile_sidebar_fill_color", FieldType.PROFILE_SIDEFILL_COLOR);
		fieldTypeMap.put("location", FieldType.LOCATION);
		fieldTypeMap.put("name", FieldType.NAME);
		fieldTypeMap.put("default_profile_image", FieldType.DEFUALT_PROFILE_IMG);
		fieldTypeMap.put("lang", FieldType.LANGUAGE);
		fieldTypeMap.put("profile_background_tile", FieldType.PROFILE_BG_TILE);
		fieldTypeMap.put("notifications", FieldType.NOTIFICATIONS);
		fieldTypeMap.put("utc_offset", FieldType.UTC_OFFSET);
		// makeup json field for additional missing fields
		fieldTypeMap.put("otherfields", FieldType.OTHERFIELDS);
	}
	
	/** input path for MapReduce jobs in queries and analysis */
	public static final String MRJOB_INPUT_PATH = "truthy.mrjob.input.path";
	
	/** output path for MapReduce jobs in queries and analysis */
	public static final String MRJOB_OUTPUT_PATH = "truthy.mrjob.output.path";
	
	/** number of reducers used in MapReduce jobs in queries and analysis */
	public static final String MRJOB_NUM_REDUCERS = "truthy.mrjob.reducer.num";
	
	// json parser
	public static final JsonParser jsonParser = new JsonParser();
	
	/** for dealing with time like "Sun Aug 29 15:10:46 +0000 2010"	 */
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
	
	/** upper bound of 64-bit tweet IDs */
	public static final BigInteger maxID64 = new BigInteger("18446744073709551615");
	
	/** 1G */
	public static final long SIZE_1G = 1L << 30;
	
	/** max number of KeyValues to get in a batch when scanning table*/
	public static final int TRUTHY_TABLE_SCAN_BATCH = 10000;
	
	/** number of tweets to process for each mapper in parallel search */
	public static final int TWEETS_PER_MAPPER = 300000;
	
	/** Moderate tweets loading speed: 50 hours for 50,000,000 tweets */
	public static final int MODERATE_TWEETS_PER_SEC = 50000000 / (50 * 3600);
	
	/** Months before 2015-07 are using customized Tweet ID binary stored in HBase */
	public static final String b4BigIntMonth = "2015-06";
}
