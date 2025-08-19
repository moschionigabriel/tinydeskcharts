from dotenv import load_dotenv
import os
from googleapiclient.discovery import build
load_dotenv()

key = os.environ.get("YOUTUBE_API_KEY")
if not key:
    raise RuntimeError("YOUTUBE_API_KEY is not set")

playlist_id = "PL1B627337ED6F55F0"
youtube = build('youtube','v3',developerKey=key)
maxResults = 50

def getVideosIds():
    nextPage_token = None
    videos_list = []
    #max_pages = 3 
    #page_count = 0

    while True:
        res = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            pageToken=nextPage_token,
            maxResults=maxResults
        ).execute()

        videos_list += list(map(lambda x: x['snippet']['resourceId']['videoId'], res['items']))
    
        nextPage_token = res.get('nextPageToken')
        #page_count += 1
    
        if not nextPage_token: #or page_count >= max_pages
            break
    return videos_list

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def getVideosInfo(lst):
    videos_info = []
    for chunk in chunks(lst,maxResults):
        res = youtube.videos().list(
            part="contentDetails,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails"
            ,id=",".join(chunk)
        ).execute()

        videos_info += res['items']
    return videos_info

def getYoutubeData():
    videos_ids = getVideosIds()
    videos_info = getVideosInfo(videos_ids)
    return videos_info