import base64
import datetime
import sys

from jobs import Jobs
from yt_sent_analysis import YoutubeSentimentAnalysis

def fn_main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    if event and 'data' in event:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        print(pubsub_message)

    sent = YoutubeSentimentAnalysis()
    jobs = Jobs(sent.bq)

    new_jobs = jobs.load_new_jobs()

    print(new_jobs.total_rows, "new job(s)")

    for job in new_jobs:
        start = datetime.datetime.now()
        job_id = job['job_id']
        channel_name = job['channel_id']
        channel_resp = sent.youtube.get_channel(cid=channel_name)

        if channel_resp is None:
            jobs.complete_job(job_id, status="No channel found")
            continue

        try:
            sent.perform_analysis(channel_name)
            jobs.complete_job(job_id)
            print("Completed:", channel_name, 'from job:', job_id)
        except Exception as e:
            print("Failed to process:", channel_name, 'from job:', job_id)
            print(str(e), file=sys.stderr, flush=True)
            jobs.complete_job(job_id, status="Processing Failed. Check logs.")
        end = datetime.datetime.now()

        print("Elapsed Time: " + str(end - start))

def test_comments(cid):
    sent = YoutubeSentimentAnalysis()
    channel_resp = sent.youtube.get_channel(cid=cid)

    video_count = 0
    comment_count = 0

    if channel_resp is not None:
        uploads_iter = sent.youtube.get_channel_uploads(channel_resp, max_results=100)

        while True:
            upload_items = next(uploads_iter, None)
            if upload_items is None or upload_items['items'] is None:
                print("No more uploads..")
                return

            upload_videos = upload_items['items']
            video_count += len(upload_videos)

            for upload in upload_videos:
                video_id = upload['contentDetails']['videoId']
                video_title = upload['snippet']['title']

                print("Obtaining comments for:", video_title, '(', video_id, ')')


                comments_iter = sent.youtube.get_comment_threads(video_id,
                                                                 max_results=100)
                while True:
                    comment_items = next(comments_iter, None)
                    if comment_items is None or comment_items['items'] is None:
                        print("No more comments..")
                        break

                    video_comments = comment_items['items']
                    comment_count += len(video_comments)
                print(video_count)
                print(comment_count)
            print("Total")
            print(video_count)
            print(comment_count)

if __name__ == '__main__':
    # test_comments("UC_mYaQAE6-71rjSN6CeCA-g")
    fn_main({}, None)
