package iu.pti.hbaseapp.truthy;

import org.codehaus.jettison.json.JSONObject;

import com.google.gson.JsonElement;

/**
 * This class encapsulates all the fields of a tweet received from the Twitter streaming API. In addition, it exposes 5 extra fields
 * on the 'tweet' object level for ease of processing: user_id_str, user_screen_name, retweeted_status_id_str, retweeted_status_user_id_str
 * memes.
 * 
 * @author gaoxm
 */
public class TweetData {
	public static class TweetUserData {
		public String created_at = null;
		public String profile_link_color = null;
		public Boolean default_profile = null;
		public Boolean follow_request_sent = null;
		public Boolean following = null;
		public Long favourites_count = null;
		public Long friends_count = null;
		public String profile_sidebar_border_color = null;
		public String url = null;
		public Boolean profile_use_background_image = null;
		public String description = null;
		public String profile_background_color = null;
		public Long followers_count = null;
		public String profile_image_url = null;
		public Boolean show_all_inline_media = null;
		public Long listed_count = null;
		public Boolean geo_enabled = null;
		public String profile_background_image_url = null;
		public Boolean contributors_enabled = null;
		public Boolean verified = null;
		public String screen_name = null;
		public String id_str = null;
		public String time_zone = null;
		public String profile_text_color = null;
		public Boolean is_protected = null;
		public Boolean is_translator = null;
		public Long statuses_count = null;
		public String profile_sidebar_fill_color = null;
		public String location = null;
		public String name = null;
		public Boolean default_profile_image = null;
		public String lang = null;
		public Boolean profile_background_tile = null;
		public Boolean notifications = null;
		public Long utc_offset = null;		
	}
	
	public Boolean favorited = null;
	public String text = null;
	public String created_at = null;
	public String user_id_str = null;
	public String user_screen_name = null;
	public Integer retweet_count = null;
	public String in_reply_to_status_id_str = null;
	public String in_reply_to_screen_name = null;
	public JsonElement contributors = null;
	public Boolean retweeted = null;
	public String retweeted_status_id_str = null;
	public String retweeted_status_user_id_str = null;
	public String source = null;
	public String in_reply_to_user_id_str = null;
	public TweetData retweeted_status = null;
	public String id_str = null;
	public JsonElement entities = null;
	public JsonElement coordinates = null;
	public JsonElement geo = null;
	public TweetUserData user = null;
	public Boolean truncated = null;
	public JsonElement place = null;
	public JsonElement otherfields = null;
}
