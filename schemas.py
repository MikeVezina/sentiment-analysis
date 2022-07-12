from google.cloud import bigquery
import pytz
from google.cloud.bigquery_storage_v1.types import TableFieldSchema


def playlist_item_schema():
    '''
    See: https://developers.google.com/youtube/v3/docs/videos#resource-representation
    '''
    return [
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("kind", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("etag", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_publishedAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_channelId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_title", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_description", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_thumbnails_default_url", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_thumbnails_default_width", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_default_height", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_medium_url", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_thumbnails_medium_width", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_medium_height", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_high_url", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_thumbnails_high_width", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_high_height", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_standard_url", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_thumbnails_standard_width", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_standard_height", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_maxres_url", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_thumbnails_maxres_width", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_thumbnails_maxres_height", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_channelTitle", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_playlistId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_position", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_resourceId_kind", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_resourceId_videoId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("contentDetails_videoId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("contentDetails_note", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("contentDetails_videoPublishedAt", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("status_privacyStatus", bigquery.enums.SqlTypeNames.STRING),

        bigquery.SchemaField("insertion_time", bigquery.enums.SqlTypeNames.DATETIME),
    ]


def comments_schema():
    '''
    See: https://developers.google.com/youtube/v3/docs/comments#resource-representation
    '''
    return [

        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("kind", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("etag", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),

        bigquery.SchemaField("snippet_authorDisplayName", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_authorProfileImageUrl", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_authorChannelUrl", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_authorChannelId_value", bigquery.enums.SqlTypeNames.STRING),

        bigquery.SchemaField("snippet_channelId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_videoId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_textDisplay", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_textOriginal", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_parentId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_canRate", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("snippet_viewerRating", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_likeCount", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_moderationStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_publishedAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_updatedAt", bigquery.enums.SqlTypeNames.STRING),

        bigquery.SchemaField("thread_Id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("insertion_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
    ]


def comment_threads_schema():
    '''
    See: https://developers.google.com/youtube/v3/docs/commentThreads#resource-representation
    '''
    return [
        bigquery.SchemaField("kind", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("etag", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),

        bigquery.SchemaField("snippet_channelId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_videoId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_topLevelComment_Id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("snippet_canReply", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("snippet_totalReplyCount", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("snippet_isPublic", bigquery.enums.SqlTypeNames.BOOLEAN),
        bigquery.SchemaField("insertion_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
    ]


def entities_schema():
    '''
    See: https://cloud.google.com/natural-language/docs/reference/rest/v1/Entity
    '''
    return [
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("salience", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("comment_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("sentiment_magnitude", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("sentiment_score", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("insertion_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
    ]


def entities_mentions_schema():
    '''
    See: https://cloud.google.com/natural-language/docs/reference/rest/v1/Entity
    '''
    return [
        bigquery.SchemaField("type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("comment_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("text_content", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("text_beginOffset", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("sentiment_magnitude", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("sentiment_score", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("insertion_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
    ]


def job_schema():
    return [
        bigquery.SchemaField("channel_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("submitted_at", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("completed_at", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("job_id", bigquery.enums.SqlTypeNames.STRING),
    ]
