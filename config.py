import os
import secret

project = os.environ["PROJECT"]
dataset = os.environ["DATASET"]

# Load secrets from env if testing
api_key = os.environ.get("API_KEY")

# Rate limits:
max_videos = int(os.environ.get("MAX_VIDEOS", 100))
max_comment_threads = int(os.environ.get("MAX_COMMENT_THREADS_PER_VIDEO", 100))
max_replies = int(os.environ.get("MAX_REPLIES_PER_THREAD", 100))

print(f"Rate limits -- Videos: {max_videos}, Comment Threads: {max_comment_threads}, Replies: {max_replies}")


print("Configuration loaded.")