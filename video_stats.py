import requests
import json
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv(dotenv_path = "./.env" )

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
max_results = 50



def get_playlist_id():
    """Look up the channel's "uploads" playlist ID via the YouTube Data API.

    Uses the CHANNEL_HANDLE and API_KEY globals to query the channels
    endpoint, then digs the uploads playlist ID out of the response.

    Returns:
        str: The playlist ID for the channel's uploaded videos.

    Raises:
        requests.exceptions.RequestException: If the API request fails.
    """
    try:
        #url for youtube channel from API
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        #gets the response from api
        response = requests.get(url)
        response.raise_for_status()

        #parse the data
        data = response.json()

        #print(json.dumps(data, indent = 4))

        #python dictionary items from json crack
        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        
        #print(channel_playlistId)
        return channel_playlistId

    #if any errors, return exception
    except requests.exceptions.RequestException as e:
        raise e
    


def get_video_ids(playlistId):
    """Fetch every video ID contained in a YouTube playlist.

    Pages through the playlistItems endpoint using nextPageToken until
    all pages have been retrieved.

    Args:
        playlistId (str): The playlist ID to list videos from.

    Returns:
        list[str]: Video IDs for every item in the playlist.

    Raises:
        requests.exceptions.RequestException: If the API request fails.
    """

    video_ids = []

    pageToken = None


    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlistId}&key={API_KEY}"
    
    try:
        while True:

            url = base_url

            if pageToken:
                #if there is a page token, append it to the url string
                url += f"&pageToken={pageToken}"

            #gets the response from api
            response = requests.get(url)
            response.raise_for_status()

            #parse the data
            data = response.json()

            #add video ids if list is not empty
            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            #get the next batch of videos
            pageToken = data.get("nextPageToken")

            if not pageToken:
                break
        
        return video_ids
    
    except requests.exceptions.RequestException as e:
        raise e



def extract_video_data(video_ids):
    """Fetch title, publish date, duration, and stats for a list of videos.

    Batches video IDs (the videos endpoint accepts up to max_results IDs
    per request) and calls the videos endpoint for each batch.

    Args:
        video_ids (list[str]): Video IDs to fetch data for.

    Returns:
        list[dict]: One dict per video with video_id, title, publishedAt,
        duration, viewCount, likeCount, and commentCount.

    Raises:
        requests.exceptions.RequestException: If the API request fails.
    """
    extracted_data = []


    #splits a list into successive chunks of at most batch_size items
    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size]

    try:
        for batch in batch_list(video_ids, max_results):
            video_ids_str = ",".join(batch)
            
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            #gets the response from api
            response = requests.get(url)
            response.raise_for_status()

            #parse the data
            data = response.json()
    
            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                #dictionary of video data
                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount', None),     #possibly hidden fields
                    "likeCount": statistics.get('likeCount', None),
                    "commentCount": statistics.get('commentCount', None),
                }

                extracted_data.append(video_data)

        return extracted_data
   
    except requests.exceptions.RequestException as e:
        raise e

def save_to_json(extracted_data):
    """Write extracted video data to a date-stamped JSON file in ./data.

    Args:
        extracted_data (list[dict]): Video data as produced by
            extract_video_data.
    """
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent = 4, ensure_ascii = False)

if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)