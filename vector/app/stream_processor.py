import os
import redis
import numpy as np
from dotenv import load_dotenv
from redisvl.schema import IndexSchema
from redisvl.index import SearchIndex
from sentence_transformers import SentenceTransformer

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

import multiprocessing

load_dotenv()

def create_index(r):
    # Create Search Index
    index_name = "idx:lyrics"

    schema = IndexSchema.from_dict({
    "index": {
        "name": index_name,
        "prefix": "track:track_id:",
        "storage_type" : "json"
    },
    "fields": [
        {"name": "track_id", "type": "numeric"},
        {"name": "name", "type": "text"},
        {"name": "composer", "type": "text"},
        {"name": "band", "type": "text"},
        {"name": "lyrics", "type": "text"},
        {"name": "genre", "type": "text"},
        {"name": "album", "type": "text"},
        {
            "name": "vector",
            "type": "vector",
            "attrs": {
                "dims": 768,
                "distance_metric": "cosine",
                "algorithm": "flat",
                "datatype": "float32"
            }
        }
    ]
    })

    try:
        index = SearchIndex.from_existing(index_name, r)
        return True
    except:
        index = SearchIndex(schema, r)
        index.create(overwrite=True, drop=True)
        return False

def get_index_stats(r):
    try:
        info = r.ft("idx:lyrics").info()
        print(f"--> Indexed Percent: {info['percent_indexed']}")
        print(f"--> Number of Records: {info['num_records']}")
        return True
    except:
        return False

def do_the_thing(i):

    consumer_name = f"consumer_{i}"

    # Gets env vars (.env file) from lyrics_extractor directory
    REDIS_HOST = os.getenv("TARGET_DB_URL")
    REDIS_PORT = os.getenv("TARGET_DB_PORT")
    REDIS_PASSWORD = os.getenv("TARGET_DB_PASSWORD")

    # Redis Connection
    REDIS_URL = f"redis://default:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"

    r = redis.from_url(REDIS_URL, decode_responses=True)

    if r.ping():
        print(f"--> Redis Connection successful! -> consumer {consumer_name}")
    else:
        print(f"--> Redis Connection issue! -> consumer {consumer_name}")

    # Create the Search Index if it doesn't exist
    try: 
        if not get_index_stats(r):
            create_index(r)
    except Exception as ex:
        print("--> Index already exists")

    # Embedding Model
    model = SentenceTransformer("redis/langcache-embed-v3")

    # LLM
    llm = ChatOpenAI(   
        # model="gpt-4.1",
        # model="gpt-5-mini",
        # model="gpt-4o-mini",
        # model="gpt-5-nano",
        model="gpt-4.1-nano",    
        temperature=0.5,
        top_p=0.95,
        max_tokens=2048
    )

    # Create Consumer Group
    try:
        print(r.xgroup_create("track:events", "lyricsGroup", 0))
    except Exception as ex:
        print(f"--> Consumer Group Already exists: {ex}")


    while True:
        # Check the Stream for New Messages
        results = r.xreadgroup("lyricsGroup", consumername=consumer_name, count=1, streams={"track:events":'>'})
        if len(results) > 0:
            message_id = results[0][1][0][0]
            track_id = results[0][1][0][1]['track_id']
            track_name = results[0][1][0][1]['name']
            album = results[0][1][0][1]['album'].replace("\"","")
            artist = results[0][1][0][1]['artist'].replace("\"","").replace("\\","")
            print(f"--> Getting lyrics for track {track_id}: {track_name} ({artist}/{album})")
            if len(message_id) > 0:
                # Get the lyrics
                try:

                    system_template = """
                    You are a music expert, serving at an educational capacity.
                    Your job is to help people understand what a song is about in terms of theme, message, etc.
                    Provide a description of the song, the style, the genre, the type of energy this music transmits.
                    You can also include snippets of the lyrics as appropriate.
                    Write your response in a way that will help people find a song they are looking for.
                    For instance, 'what is a good song for a workout', or 'what is a good calming song for end of the workday'.
                    If the value of song, album and artist is null, disregard the value and try to find the best response.
                    Avoid any preambles in your response, like 'certainly', or 'happy to help'.
                    %SONG%
                    {song}
                    %ARTIST%
                    {artist}
                    %ALBUM%
                    {album}
                    """
                    messages = [
                        SystemMessage(content=system_template.format(song=track_name, artist=artist, album=album)),
                        HumanMessage(content=f"please describe song {track_name} by {artist}")
                    ]

                    llm_response = llm.invoke(messages)
                    song_lyrics = llm_response.content

                    # Update Key in Redis
                    r.json().set(f"track:track_id:{track_id}", "$.band", artist)
                    r.json().set(f"track:track_id:{track_id}", "$.lyrics", song_lyrics)

                    # Create vector from the lyrics and save it to Redis
                    lyrics_vector = model.encode([song_lyrics])[0].astype(np.float32).tolist()
                    r.json().set(f"track:track_id:{track_id}", "$.vector", lyrics_vector)

                    # Acknowledge Message
                    result = r.xack("track:events", "lyricsGroup", message_id)
                    result = r.xdel("track:events", message_id)

                    print(f"--> Track {track_id} - Message {message_id} processed and acknowledged by consumer {consumer_name}.")
                
                except Exception as ex:
                    print(f"--> Track {track_id} - LYRICS NOT FOUND {ex}")

if __name__ == "__main__":

    jobs = []
    workers = 4

    print(f"--> Initializing {workers} workers")
    
    for i in range(workers):
        proc = multiprocessing.Process(target=do_the_thing, args=(i,))
        jobs.append(proc)
        proc.start()
    
    for job in jobs:
        job.join()

