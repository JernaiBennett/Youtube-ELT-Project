import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path = "./.env" )

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
max_results = 50



#function to get youtube channel playlist 
def get_playlist_id():
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
    

#function to get videos from channel playlist
def get_video_ids(playlistId):

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


#extracts the data from each video from the above list
def extract_video_data(video_ids):
    extracted_data = []

    
    #function to split the video ids into batches
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


if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    print(extract_video_data(video_ids))