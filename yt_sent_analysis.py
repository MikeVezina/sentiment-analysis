import sys
from collections.abc import MutableMapping, Iterable, Sequence
from google.protobuf import json_format

import pandas as pd
from google.cloud import language_v1
from pandas import DataFrame, json_normalize

import schemas
from jobs import Jobs
from schemas import playlist_item_schema, comments_schema
from big_query import BigQueryConnector
from sentiment_analysis import SentimentAnalysisConnector
from youtube import YoutubeConnector
import json
from config import max_replies, max_videos, max_comment_threads

tables = {
    "videos": "videos",
    "commentThreads": "comment_threads",
    "comments": "comments",
    "entities": "entities",
    "entityMentions": "entity_mentions"
}


class YoutubeSentimentAnalysis:

    def __init__(self):
        self.sentiment = SentimentAnalysisConnector()
        self.youtube = YoutubeConnector()
        self.bq = BigQueryConnector()

    def get_comment_ent_sentiment(self, comment_text):
        entities = self.sentiment.sample_analyze_entity_sentiment(comment_text)
        return entities

    def perform_analysis(self, cid):
        channel_resp = self.youtube.get_channel(cid=cid)

        video_count = 0

        if channel_resp is not None:
            uploads_iter = self.youtube.get_channel_uploads(channel_resp, max_results=max_videos - video_count)

            while video_count < max_videos:
                upload_items = next(uploads_iter, None)
                if upload_items is None or upload_items['items'] is None:
                    print("No more uploads..")
                    return

                upload_videos = upload_items['items']
                video_count += len(upload_videos)

                for upload in upload_videos:
                    self.insert_video(upload)
                    self.get_video_comments(upload)

    def get_video_comments(self, upload):
        video_id = upload['contentDetails']['videoId']
        video_title = upload['snippet']['title']

        print("Obtaining comments for:", video_title, '(', video_id, ')')

        comment_count = 0
        comments_iter = self.youtube.get_comment_threads(video_id, max_results=max_comment_threads - comment_count)

        threads_df = DataFrame()
        comments_df = DataFrame()
        entities_df = DataFrame()
        entity_mentions_df = DataFrame()

        while comment_count < max_comment_threads:
            comment_items = next(comments_iter, None)
            if comment_items is None or comment_items['items'] is None:
                print("No more comments..")
                break

            video_comments = comment_items['items']
            comment_count += len(video_comments)

            for comment in video_comments:
                (thr_df, comm_df, ent_df, entity_men_df) = self.upload_comment_thread(comment)
                threads_df = pd.concat([threads_df, thr_df], ignore_index=True)
                comments_df = pd.concat([comments_df, comm_df], ignore_index=True)
                entities_df = pd.concat([entities_df, ent_df], ignore_index=True)
                entity_mentions_df = pd.concat([entity_mentions_df, entity_men_df], ignore_index=True)

        # Write comment thread to DB
        print("Writing threads")
        self.bq.write_df(self.bq.get_table_id(tables['commentThreads']), threads_df,
                         tab_schema=schemas.comment_threads_schema())
        print("Writing thread comments")
        self.bq.write_df(self.bq.get_table_id(tables['comments']), comments_df, tab_schema=comments_schema())
        print("Writing entities")
        self.bq.write_df(self.bq.get_table_id(tables['entities']), entities_df, tab_schema=schemas.entities_schema())
        print("Writing entity mentions")
        self.bq.write_df(self.bq.get_table_id(tables['entityMentions']), entity_mentions_df,
                         tab_schema=schemas.entities_mentions_schema())
        print("Video analysis complete. Successfully wrote analysis to BigQuery")

    def insert_video(self, upload):
        upload_df = json_normalize(upload, sep='_')
        table = self.bq.get_table_id(tables['videos'])
        self.bq.write_df(table, upload_df, tab_schema=playlist_item_schema())

    def filter_dict(self, d, sep='_', filt=None, excl=None):
        '''
        Keeps all top-level key/value pairs, and removes any nested dicts that don't match those in filter.
        '''
        if excl is None:
            excl = []
        if filt is None:
            filt = []

        res = {}
        for k, v in d.items():
            if k in excl:
                continue
            if k in filt and isinstance(v, MutableMapping):
                res[k] = self.filter_dict(v, sep=sep)
            elif isinstance(v, MutableMapping):
                continue
            else:
                res[k] = v
        return res

    def upload_comment_thread(self, thread):
        '''
        See: https://developers.google.com/youtube/v3/docs/commentThreads
        '''

        # Get details
        snippet = thread['snippet']
        top_level_comment = snippet['topLevelComment']

        print("Performing analysis on comment thread:", top_level_comment['id'])

        # TODO: Check if comment thread exists and etag is same
        # TODO: I don't think etag for thread captures comment changes, so no point in checking
        # existing_thread = self.bq.get_by_id(self.bq.get_table_id(tables['commentThreads']), where={
        #     'id': thread['id'],
        #     'etag': thread['etag']
        # })

        # Find ID + etag (limit 1) => existing (no analysis needed)
        # Also, want to ensure there is only 1 unique row for ID + etag
        # if existing_thread:
        #     print("Skipping thread -- existing thread ID and etag. ID:", thread['id'], ", etag:",
        #           thread['etag'])
        #     return thread_df, comments_df, entities_df, entity_mentions_df
        #  TODO: END Snippet

        existing_thread_id = self.bq.get_by_id(self.bq.get_table_id(tables['commentThreads']), where={
            'id': thread['id']
        })

        # Find existing thread ID and remove old info
        if existing_thread_id:
            # Remove old thread meta-data and re-analyze individual comments
            self.bq.remove_where(self.bq.get_table_id(tables['commentThreads']), "id", thread['id'])

        # Separate thread data from comments for table
        thread_data = self.filter_dict(thread, filt=['snippet'])
        thread_data['snippet']['topLevelComment_Id'] = top_level_comment['id']
        thread_df = json_normalize(thread_data, sep='_')

        # Map comment data to entities
        # Contains tuples: (comment_data, parent)
        all_comments = []

        all_comments.append((top_level_comment, None))

        # TODO: Handle replies (requires calls to the comments/list endpoint)
        if snippet['totalReplyCount'] > 0:
            for reply in self.get_replies(top_level_comment['id']):
                all_comments.append((reply, top_level_comment))

        comment_analysis = []

        comments_df = DataFrame()
        entities_df = DataFrame()
        entity_mentions_df = DataFrame()

        for (comment, parent) in all_comments:
            # TODO: Check if id and etag exists for comment
            existing = self.bq.get_by_id(self.bq.get_table_id(tables['comments']), where={
                'id': comment['id'],
                'etag': comment['etag']
            })

            # Find ID + etag (limit 1) => existing (no analysis needed)
            # Also, want to ensure there is only 1 unique row for ID + etag
            if existing and existing.total_rows == 1:
                print("Existing comment and etag. ID:", comment['id'], ", etag:", comment['etag'])
                continue

            # Find ID only => Remove existing analysis + update
            id_existing = self.bq.get_by_id(self.bq.get_table_id(tables['comments']), where={
                'id': comment['id']
            })

            # Remove existing comment and entity analysis, then re-insert
            if id_existing:
                print("Removing existing comment for id:", comment['id'])

                # Remove comment
                self.bq.remove_where(self.bq.get_table_id(tables['comments']), "id", comment['id'])

                # Remove entity mentions
                self.bq.remove_where(self.bq.get_table_id(tables['entityMentions']), "comment_id", comment['id'])

            # Process entity
            entities = self.get_comment_ent_sentiment(comment['snippet']['textOriginal'])
            comment_analysis.append((comment, parent, entities))

            comment_df = pd.json_normalize(comment, sep='_')
            comment_df['thread_Id'] = thread['id']

            comments_df = pd.concat([comments_df, comment_df], ignore_index=True)

            # Replace nan with empty string
            comments_df.fillna('', inplace=True)

            for entity in entities:
                # Convert protobuf to dict
                entity_data = json_format.MessageToDict(entity._pb)

                # Attach comment to entity
                entity_data['comment_id'] = comment['id']

                # Remove nested objects (i.e., mentions)
                mentions = entity_data['mentions']
                del entity_data['mentions']

                entity_df = pd.json_normalize(entity_data, sep='_')

                # Process mentions
                for mention in mentions:
                    # Attach comment to entity
                    mention['comment_id'] = comment['id']
                    mention_df = json_normalize(mention, sep='_')
                    entity_mentions_df = pd.concat([entity_mentions_df, mention_df], ignore_index=True)

                entities_df = pd.concat([entities_df, entity_df], ignore_index=True)

        print("Processed comment thread:", top_level_comment['id'])
        return thread_df, comments_df, entities_df, entity_mentions_df

    def entity_to_dict(self, entity):
        ent_data = {}
        ent_data['name'] = entity.name
        ent_data['type'] = language_v1.Entity.Type(entity.type_).name
        ent_data['salience'] = entity.salience
        ent_data['sentiment'] = {
            'score': entity.sentiment.score,
            'magnitude': entity.sentiment.magnitude,
        }

        meta_map = {}
        for metadata_name, metadata_value in entity.metadata.items():
            meta_map[metadata_name] = metadata_value

        # Put metadata as json in DB
        ent_data['metadata'] = json.dumps(meta_map)

        mentions = []

        for mention in entity.mentions:
            mention_data = {}
            mention_data['text'] = {
                'content': mention.text.content,
                'begin_offset': mention.text.begin_offset
            }

            mention_data['type'] = language_v1.EntityMention.Type(mention.type_).name
            mention_data['sentiment'] = {
                'score': mention.sentiment.score,
                'magnitude': mention.sentiment.magnitude
            }

            mentions.append(mention_data)

        ent_data['mentions'] = mentions

        return ent_data

    def get_replies(self, top_level_id):
        replies_count = 0
        comments_iter = self.youtube.get_replies(top_level_id, max_results=max_replies - replies_count)

        all_replies = []
        while replies_count < max_replies:
            reply_items = next(comments_iter, None)
            if reply_items is None or reply_items['items'] is None:
                print("No more replies..")
                return all_replies

            replies = reply_items['items']
            replies_count += len(replies)
            print("Processing", len(replies), "replies for comment thread")
            all_replies.extend(replies)

        print("Reached max replies:", replies_count)

        return all_replies
