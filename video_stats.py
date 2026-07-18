import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path = "./.env" )

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"

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
    
if __name__ == "__main__":
    get_playlist_id()