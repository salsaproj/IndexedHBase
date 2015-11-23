package iu.pti.hbaseapp.truthy.mrqueries;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;

import iu.pti.hbaseapp.truthy.ConstantsTruthy;
import iu.pti.hbaseapp.truthy.ConstantsTruthy.FieldType;
import iu.pti.hbaseapp.truthy.TruthyHelpers;
import iu.pti.hbaseapp.truthy.TweetData;
import iu.pti.hbaseapp.truthy.TweetData.TweetUserData;
import iu.pti.hbaseapp.truthy.TweetTableClient;
import iu.pti.hbaseapp.truthy.TweetSubsetProcessor.TwitterIdProcessMapper;

/**
 * Mapper class for getting the original tweets (in JSON format) corresponding
 * to a subset of tweet IDs.
 * 
 * @author gaoxm
 * @author taklwu
 */
public class GetTweetsMapper extends TwitterIdProcessMapper<Text, NullWritable> {
    private TweetTableClient tweetGetter = null;
    private Gson gson = null;
    private String phraseToMatch = null;
    private boolean needUser = true;
    private boolean needRetweeted = true;
    private boolean idForPhraseSearch = false;
    // added for new filter / strip text
    private boolean includeJsonFields = false;
    private boolean excludeJsonFields = false;
    private String[] targetedJsonFields;
    private static final Log LOG = LogFactory.getLog(GetTweetsMapper.class);
    private long HBaseIOTime = 0L;
    private long UDFStartTime = 0L;

    @Override
    protected void map(LongWritable rowNum, Text txtTweetId, Context context)
            throws IOException, InterruptedException {
        try {
            String tweetId = txtTweetId.toString();
            long startTime = System.currentTimeMillis();
            TweetData td = tweetGetter.getTweetData(tweetId, needUser,
                    needRetweeted);
            this.HBaseIOTime += System.currentTimeMillis() - startTime;
            if (phraseToMatch != null
                    && !td.text.toLowerCase().contains(
                            phraseToMatch.toLowerCase())) {
                return;
            }

            if (phraseToMatch != null && idForPhraseSearch) {
                context.write(txtTweetId, NullWritable.get());
            } else if (this.includeJsonFields || this.excludeJsonFields) {
                TweetData filteredTweet = new TweetData();
                // strip-text handling
                filteredTweet = this.filterTweet(td, targetedJsonFields);
                String tweetJson = gson.toJson(filteredTweet, TweetData.class);
                context.write(new Text(tweetJson), NullWritable.get());
            } else {
                String tweetJson = gson.toJson(td, TweetData.class);
                context.write(new Text(tweetJson), NullWritable.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
            context.setStatus("Exception when analyzing tweet "
                    + txtTweetId.toString());
        }
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        UDFStartTime = System.currentTimeMillis();
        LOG.info("UDF Start Time = " + UDFStartTime);
        LOG.info("Mapper setup start TimeStamp = " + UDFStartTime);
        try {
            tweetGetter = new TweetTableClient(context);
            gson = new Gson();
            Configuration conf = context.getConfiguration();
            if (conf.get("additional.arguments") != null
                    && !conf.get("additional.arguments").isEmpty()) {
                String[] args = conf.get("additional.arguments").split("\\n");
                // check if the first argument is include or exclude
                if (args.length != 0
                        && args[0].equalsIgnoreCase("--json-include")) {
                    this.includeJsonFields = true;
                } else if (args.length != 0
                        && args[0].equalsIgnoreCase("--json-exclude")) {
                    this.excludeJsonFields = true;
                } else { // general cases defined by IndexedHBase first version
                    if (args.length > 1) {
                        needUser = Boolean.valueOf(args[0]);
                        needRetweeted = Boolean.valueOf(args[1]);
                    }
                    if (args.length > 2) {
                        phraseToMatch = args[2];
                        idForPhraseSearch = args[3].equalsIgnoreCase("id");
                    }
                }

                if (this.includeJsonFields || this.excludeJsonFields) {
                    // parse the target json fields in comma
                    targetedJsonFields = args[1].toLowerCase().split(",");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }
        
        long endTime = System.currentTimeMillis();
        LOG.info("Mapper setup endTimeStamp = " + endTime);
        LOG.info("Mapper setup time takes = " + (endTime - UDFStartTime) + " ms");
    }

    @Override
    protected void cleanup(Context context) {
        long startTime = System.currentTimeMillis();
        try {
            if (tweetGetter != null) {
                tweetGetter.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long UDFEndTime = System.currentTimeMillis();
        LOG.info("UDF End Time = " + UDFEndTime);
        LOG.info("HBaseIO Time = " + this.HBaseIOTime + " ms");
        long UDFOverallTime = (UDFEndTime - this.UDFStartTime);
        LOG.info("UDF Computation Time = "
                + (UDFOverallTime - this.HBaseIOTime) + " ms");
        LOG.info("UDF finish Time = " + UDFOverallTime + " ms");
        long endTime = System.currentTimeMillis();
        LOG.info("Mapper cleanup End TimeStamp = " + endTime);
        LOG.info("Mapper cleanup time takes = " + (endTime - startTime) + " ms");
    }

    private TweetData filterTweet(TweetData td, String[] targetedJsonFields) {
        TweetData result = new TweetData();
//        result.retweeted_status = new TweetData();
//        result.user = new TweetUserData();
        if (includeJsonFields) {
            for (String fieldName : targetedJsonFields) {
                this.setValueToTweetRequiredField(result, td, fieldName);
            }

        } else {
            result = td;
            for (String fieldName : targetedJsonFields) {
                this.setNullValueToIgnoreField(result, fieldName);
            }
        }

        return result;
    }

    private void setNullValueToIgnoreField(TweetData output, String fieldName) {
        String[] multilevelFieldName = fieldName.toLowerCase().split("\\.");
        String firstLevelField = multilevelFieldName[0];
        String secondlevelField = null;
        String thirdlevelField = null;
        if (multilevelFieldName.length > 1) {
            secondlevelField = multilevelFieldName[1];
        }
        if (multilevelFieldName.length > 2) {
            thirdlevelField = multilevelFieldName[2];
        }
        // empty original input data
        if (output == null) {
          return;
        }
        FieldType ft = ConstantsTruthy.fieldTypeMap.get(firstLevelField);
        if (ft != null && output != null) {
            switch (ft) {
            case _IGNORE:
                break;
            case FAVORITED:
                output.favorited = null;
                break;
            case TEXT:
                output.text = null;
                break;
            case CREATE_TIME:
                output.created_at = null;
                break;
            case RETWEET_COUNT:
                output.retweet_count = null;
                break;
            case REPLY_STATUS_ID:
                output.in_reply_to_status_id_str = null;
                break;
            case REPLY_SCREEN_NAME:
                output.in_reply_to_screen_name = null;
                break;
            case CONTRIBUTORS:
                output.contributors = null;
                break;
            case RETWEETED:
                output.retweeted = null;
                break;
            case SOURCE:
                output.source = null;
                break;
            case REPLY_USER_ID:
                output.in_reply_to_user_id_str = null;
                break;
            case RETWEET_STATUS:
                if (secondlevelField == null) {
                    output.retweeted_status = null;
                }
                break;
            case ENTITIES:
                output.entities = null;
                break;
            case COORDINATES:
                output.coordinates = null;
                break;
            case GEO:
                output.geo = null;
                break;
            case USER:
                if (secondlevelField == null) {
                    output.user = null;
                }
                break;
            case TRUNCATED:
                output.truncated = null;
                break;
            case PLACE:
                output.place = null;
                break;
            case OTHERFIELDS:
                output.otherfields = null;
            default:
                break;
            }
        }
        // handle secondary fields user.X and retweeted_status.X
        if (secondlevelField != null) {
            if (ft.equals(FieldType.USER) && output.user != null) {
                this.setNullValueToUserObjectField(output.user,
                        secondlevelField);
            }
            if (ft.equals(FieldType.RETWEET_STATUS)
                    && output.retweeted_status != null) {
                this.setNullValueToIgnoreField(output.retweeted_status,
                        secondlevelField);
                // handle third level field only retweeted_status.user.X
                if (thirdlevelField != null
                        && output.retweeted_status.user != null) {
                    this.setNullValueToUserObjectField(
                            output.retweeted_status.user, thirdlevelField);
                }
            }
        }
    }

    private void setNullValueToUserObjectField(TweetUserData user,
            String userField) {
        FieldType ft = ConstantsTruthy.fieldTypeMap.get(userField);
        if (ft != null && user != null) {
            switch (ft) {
            case _IGNORE:
                break;
            case CREATE_TIME:
                user.created_at = null;
                break;
            case FOLLOW_REQ_SENT:
                user.follow_request_sent = null;
                break;
            case FOLLOWING:
                user.following = null;
                break;
            case FAVORITES_COUNT:
                user.favourites_count = null;
                break;
            case FRIENDS_COUNT:
                user.friends_count = null;
                break;
            case URL:
                user.url = null;
                break;
            case DESCRIPTION:
                user.description = null;
                break;
            case FOLLOWERS_COUNT:
                user.favourites_count = null;
                break;
            case INLINE_MEDIA:
                user.show_all_inline_media = null;
                break;
            case LISTED_COUNT:
                user.listed_count = null;
                break;
            case GEO_ENABLED:
                user.geo_enabled = null;
                break;
            case CONTRIBUTORS_ENABLED:
                user.contributors_enabled = null;
                break;
            case VERIFIED:
                user.verified = null;
                break;
            case SCREEN_NAME:
                user.screen_name = null;
                break;
            case TIME_ZONE:
                user.time_zone = null;
                break;
            case PROTECTED:
                user.is_protected = null;
                break;
            case IS_TRANSLATOR:
                user.is_translator = null;
                break;
            case STATUS_COUNT:
                user.statuses_count = null;
                break;
            case LOCATION:
                user.location = null;
                break;
            case NAME:
                user.name = null;
                break;
            case LANGUAGE:
                user.lang = null;
                break;
            case NOTIFICATIONS:
                user.notifications = null;
                break;
            case UTC_OFFSET:
                user.utc_offset = null;
                break;
            case PROFILE_LINK_COLOR:
                user.profile_link_color = null;
                break;
            case PROFILE_SIDEBORDER_COLOR:
                user.profile_sidebar_border_color = null;
                break;
            case PROFILE_USE_BGIMG:
                user.profile_use_background_image = null;
                break;
            case DEFAULT_PROFILE:
                user.default_profile = null;
                break;
            case PROFILE_BG_COLOR:
                user.profile_background_color = null;
                break;
            case PROFILE_IMG_URL:
                user.profile_image_url = null;
                break;
            case PROFILE_BGIMG_URL:
                user.profile_background_image_url = null;
                break;
            case PROFILE_TEXT_COLOR:
                user.profile_text_color = null;
                break;
            case PROFILE_SIDEFILL_COLOR:
                user.profile_sidebar_fill_color = null;
                break;
            case DEFUALT_PROFILE_IMG:
                user.default_profile_image = null;
                break;
            case PROFILE_BG_TILE:
                user.profile_background_tile = null;
                break;
            default:
                break;
            }
        }

    }

    private void setValueToTweetRequiredField(TweetData output,
            TweetData input, String fieldName) {
        String[] multilevelFieldName = fieldName.toLowerCase().split("\\.");
        String firstLevelField = multilevelFieldName[0];
        String secondlevelField = null;
        String thirdlevelField = null;
        if (multilevelFieldName.length > 1) {
            secondlevelField = multilevelFieldName[1];
        }
        if (multilevelFieldName.length > 2) {
            thirdlevelField = multilevelFieldName[2];
        }
        FieldType ft = ConstantsTruthy.fieldTypeMap.get(firstLevelField);
        if (ft != null && input != null) {
            switch (ft) {
            case _IGNORE:
                break;
            case FAVORITED:
                output.favorited = input.favorited != null ? input.favorited
                        : null;
                break;
            case TEXT:
                output.text = input.text != null ? input.text : null;
                break;
            case CREATE_TIME:
                output.created_at = input.created_at != null ? input.created_at
                        : null;
                break;
            case RETWEET_COUNT:
                output.retweet_count = input.retweet_count != null ? input.retweet_count
                        : null;
                break;
            case REPLY_STATUS_ID:
                output.in_reply_to_status_id_str = input.in_reply_to_status_id_str != null ? input.in_reply_to_status_id_str
                        : null;
                break;
            case REPLY_SCREEN_NAME:
                output.in_reply_to_screen_name = input.in_reply_to_screen_name != null ? input.in_reply_to_screen_name
                        : null;
                break;
            case CONTRIBUTORS:
                output.contributors = input.contributors != null ? input.contributors
                        : null;
                break;
            case RETWEETED:
                output.retweeted = input.retweeted != null ? input.retweeted
                        : null;
                break;
            case SOURCE:
                output.source = input.source != null ? input.source : null;
                break;
            case REPLY_USER_ID:
                output.in_reply_to_user_id_str = input.in_reply_to_user_id_str != null ? input.in_reply_to_user_id_str
                        : null;
                break;
            case RETWEET_STATUS:
                if (secondlevelField == null) {
                    output.retweeted_status = input.retweeted_status != null ? input.retweeted_status
                            : null;
                }
                break;
            case ENTITIES:
                output.entities = input.entities != null ? input.entities
                        : null;
                break;
            case COORDINATES:
                output.coordinates = input.coordinates != null ? input.coordinates
                        : null;
                break;
            case GEO:
                output.geo = input.geo != null ? input.geo : null;
                break;
            case USER:
                if (secondlevelField == null) {
                    output.user = input.user != null ? input.user : null;
                }
                break;
            case TRUNCATED:
                output.truncated = input.truncated != null ? input.truncated
                        : null;
                break;
            case PLACE:
                output.place = input.place != null ? input.place : null;
                break;
            case USER_ID_STR:
                output.user_id_str = input.user_id_str != null ? input.user_id_str : null;
                break;
            case OTHERFIELDS:
                output.otherfields = input.otherfields != null ? input.otherfields : null;
            default:
                break;
            }
        }

        // handle secondary fields user.X and retweeted_status.X
        if (secondlevelField != null) {
            if (ft.equals(FieldType.USER)) {
                if (output.user == null) {
                    output.user = new TweetUserData();
                }
                this.setValueToUserObjectRequiredField(output.user, input.user,
                        secondlevelField);
            }
            if (ft.equals(FieldType.RETWEET_STATUS)) {
                if (output.retweeted_status == null) {
                    output.retweeted_status = new TweetData();
                }
                this.setValueToTweetRequiredField(output.retweeted_status,
                        input.retweeted_status, secondlevelField);
                // handle third level field only retweeted_status.user.X
                if (thirdlevelField != null) {
                    if (output.retweeted_status.user == null) {
                        output.retweeted_status.user = new TweetUserData();
                    }
                    this.setValueToUserObjectRequiredField(
                            output.retweeted_status.user,
                            input.retweeted_status.user, thirdlevelField);
                }
            }
        }
    }

    private void setValueToUserObjectRequiredField(TweetUserData output,
            TweetUserData input, String userField) {
        FieldType ft = ConstantsTruthy.fieldTypeMap.get(userField);
        if (ft != null) {
            switch (ft) {
            case _IGNORE:
                break;
            case CREATE_TIME:
                output.created_at = input.created_at;
                break;
            case FOLLOW_REQ_SENT:
                output.follow_request_sent = input.follow_request_sent;
                break;
            case FOLLOWING:
                output.following = input.following;
                break;
            case FAVORITES_COUNT:
                output.favourites_count = input.favourites_count;
                break;
            case FRIENDS_COUNT:
                output.friends_count = input.friends_count;
                break;
            case URL:
                output.url = input.url;
                break;
            case DESCRIPTION:
                output.description = input.description;
                break;
            case FOLLOWERS_COUNT:
                output.favourites_count = input.favourites_count;
                break;
            case INLINE_MEDIA:
                output.show_all_inline_media = input.show_all_inline_media;
                break;
            case LISTED_COUNT:
                output.listed_count = input.listed_count;
                break;
            case GEO_ENABLED:
                output.geo_enabled = input.geo_enabled;
                break;
            case CONTRIBUTORS_ENABLED:
                output.contributors_enabled = input.contributors_enabled;
                break;
            case VERIFIED:
                output.verified = input.verified;
                break;
            case SCREEN_NAME:
                output.screen_name = input.screen_name;
                break;
            case TIME_ZONE:
                output.time_zone = input.time_zone;
                break;
            case PROTECTED:
                output.is_protected = input.is_protected;
                break;
            case IS_TRANSLATOR:
                output.is_translator = input.is_translator;
                break;
            case STATUS_COUNT:
                output.statuses_count = input.statuses_count;
                break;
            case LOCATION:
                output.location = input.location;
                break;
            case NAME:
                output.name = input.name;
                break;
            case LANGUAGE:
                output.lang = input.lang;
                break;
            case NOTIFICATIONS:
                output.notifications = input.notifications;
                break;
            case UTC_OFFSET:
                output.utc_offset = input.utc_offset;
                break;
            case PROFILE_LINK_COLOR:
                output.profile_link_color = input.profile_link_color;
                break;
            case PROFILE_SIDEBORDER_COLOR:
                output.profile_sidebar_border_color = input.profile_sidebar_border_color;
                break;
            case PROFILE_USE_BGIMG:
                output.profile_use_background_image = input.profile_use_background_image;
                break;
            case DEFAULT_PROFILE:
                output.default_profile = input.default_profile;
                break;
            case PROFILE_BG_COLOR:
                output.profile_background_color = input.profile_background_color;
                break;
            case PROFILE_IMG_URL:
                output.profile_image_url = input.profile_image_url;
                break;
            case PROFILE_BGIMG_URL:
                output.profile_background_image_url = input.profile_background_image_url;
                break;
            case PROFILE_TEXT_COLOR:
                output.profile_text_color = null;
                break;
            case PROFILE_SIDEFILL_COLOR:
                output.profile_sidebar_fill_color = input.profile_sidebar_fill_color;
                break;
            case DEFUALT_PROFILE_IMG:
                output.default_profile_image = input.default_profile_image;
                break;
            case PROFILE_BG_TILE:
                output.profile_background_tile = input.profile_background_tile;
                break;
            default:
                break;
            }
        }

    }

}
