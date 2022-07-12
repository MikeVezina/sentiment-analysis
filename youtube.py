import googleapiclient.discovery

'''
# Using OAUTH:
credentials = flow.run_local_server()
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, credentials=credentials)
'''

MAX_RESULT_SIZE = 100

api_service_name = "youtube"
api_version = "v3"




class PageIterator:
    def __init__(self, retrieve_fn, init_page='') -> None:
        '''
        Create page iterator
        :param retrieve_fn: a function that accepts the current page, and returns a tuple (data, next_page)
        :param init_page:
        '''
        self.retrieve_fn = retrieve_fn
        self.init_page = init_page
        self.next_page = init_page

    def __iter__(self):
        self.next_page = self.init_page
        return self

    def __next__(self):
        # Stop iteration if no next page given by prev request
        if self.next_page is None:
            raise StopIteration

        data = self.retrieve_fn(self, self.next_page).execute()

        if data is not None:
            self.next_page = data.get('nextPageToken', None)
        else:
            raise StopIteration

        return data


class YoutubeConnector:
    def __init__(self):
        self.youtube = self.connect_youtube()

    def connect_youtube(self):
        return googleapiclient.discovery.build(api_service_name, api_version)

    def list_playlist_fn(self, playlistId, part, maxResults):
        return lambda other, page: self.youtube.playlistItems().list(playlistId=playlistId, part=part, pageToken=page,
                                                                     maxResults=maxResults)

    def list_threads_fn(self, videoId, part, maxResults):
        return lambda other, page: self.youtube.commentThreads().list(videoId=videoId, part=part, pageToken=page,
                                                                      maxResults=maxResults)

    def list_replies_fn(self, top_level_id, part, max_results):
        return lambda other, page: self.youtube.comments().list(parentId=top_level_id, part=part, pageToken=page,
                                                                maxResults=max_results)

    def get_channel(self, cid=None, name='GoogleDevelopers'):

        if cid is not None:
            request = self.youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=cid
            )
        else:
            request = self.youtube.channels().list(
                part="snippet,contentDetails,statistics",
                forUsername=name
            )
        response = request.execute()

        if response['pageInfo']['totalResults'] == 0:
            print("No results for", name)
            return

        return response

    def get_channel_uploads(self, channel_resp, max_results=MAX_RESULT_SIZE) -> PageIterator:
        if max_results < 0:
            max_results = 0

        max_results = min(max_results, MAX_RESULT_SIZE)

        # Get uploads playlist
        upload_list_id = channel_resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        return PageIterator(
            self.list_playlist_fn(upload_list_id, part="id,snippet,contentDetails,status", maxResults=max_results))

    def get_comment_threads(self, video_id, max_results=MAX_RESULT_SIZE) -> PageIterator:
        if max_results < 0:
            max_results = 0

        max_results = min(max_results, MAX_RESULT_SIZE)

        return PageIterator(
            self.list_threads_fn(video_id, part="id,snippet,replies", maxResults=max_results))

    def get_replies(self, top_level_id, max_results=MAX_RESULT_SIZE) -> PageIterator:
        if max_results < 0:
            max_results = 0

        max_results = min(max_results, MAX_RESULT_SIZE)

        return PageIterator(
            self.list_replies_fn(top_level_id, part="id,snippet", max_results=max_results))
