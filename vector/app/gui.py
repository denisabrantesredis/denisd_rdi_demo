import os
import json
import time
import redis
import pandas as pd
import numpy as np
import streamlit as st
from streamlit import session_state as ss

from redisvl.query import VectorQuery
from redisvl.query.filter import Text
from redisvl.index import SearchIndex
from redisvl.query import AggregateHybridQuery
from redisvl.extensions.router import Route
from redisvl.extensions.router import SemanticRouter
from redisvl.extensions.llmcache import SemanticCache
from redisvl.utils.vectorize import HFTextVectorizer

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

from streamlit_extras.metric_cards import style_metric_cards

from dotenv import load_dotenv
load_dotenv()

os.environ["TOKENIZERS_PARALLELISM"] = "false"

# Load environment variables
redis_host = os.getenv("TARGET_DB_URL")
redis_port = os.getenv("TARGET_DB_PORT")
redis_user = "default"
redis_pass = os.getenv("TARGET_DB_PASSWORD")

st.set_page_config(layout="wide", page_title="Redis-Chat-Go")

time_parse, time_save, time_search, time_llm = 0, 0, 0, 0

main_sidebar = st.sidebar
with main_sidebar:
    center_img = f"""
        <div style='text-align:center; margin-top: 0px; min-height:80px;'>
            <img src='https://redis.io/wp-content/uploads/2024/04/Redis_Desktop_15_FeatureStores_M6_Icon01.svg?&auto=webp&quality=85,75&width=80'/>
        </div>
        <div style='font-family: \"Space Grotesk\";font-weight: 400; letter-spacing: 0; font-size: 22px; margin-top: 20px; margin-bottom: 20px; text-align:center;'>
            The Redis Difference
        </div>
    """
    st.markdown(center_img, unsafe_allow_html=True)


style = """
<style>
@font-face {
    font-family: 'Space Grotesk';
    src: url('https://redis.io/wp-content/themes/redislabs-glide/assets/dist/fonts/SpaceGrotesk-Regular.woff') format("woff");
}

html,
body,
[class*='css'] {
    font-family: 'Space Grotesk';
    color: #0a6309;
}
p, ol, ul, dl {
    font-family: 'Space Grotesk';
    margin:0px 0px 1rem;
    padding: 0px;
    font-size: 1rem;
    font-weight: 400;
}
h1, h2, h3, h4 {
    font-family: 'Space Grotesk';
    font-weight: 400;
}
.top-bar {
    font-family: 'Space Grotesk';
    background-color: #FFFFFF;
    padding: 15px;
    color: white;
    margin-top: -20px;
}

</style>
"""

st.markdown(style, unsafe_allow_html=True)

input_panel = st.container()
with input_panel:
    panel1, panel2 = st.columns([0.85,0.15])
    with panel1:
        st.markdown(
            """
            <div class="top-bar">
                <img src="https://redis.io/wp-content/uploads/2024/04/Logotype.svg?auto=webp&quality=85,75&width=80" alt="Redis Logo" height="40">
            </div>
            """,
            unsafe_allow_html=True,
        )


st.title("Chinook Song Finder™")

REDIS_URL = f"redis://default:{redis_pass}@{redis_host}:{redis_port}"
r = redis.from_url(REDIS_URL, decode_responses=True)


# Setup semantic router
songs_route = Route(
    name="songs",
    references=[
        "help me find a song about",
        "what is a good song for this mood",
        "recommend a song",
        "music that makes me feel like",
        "song like", "song that looks like",
        "great songs for",
        "lyrics", "song", "music"
    ],
    metadata={"category": "songs", "priority": 1},
    distance_threshold=0.8
)

aliens_route = Route(
    name="aliens",
    references=[
        "aliens",
        "UFO sightings",
        "alien", "extraterrestrial",
        "UFO, flying saucers"
    ],
    metadata={"category": "aliens", "priority": 2},
    distance_threshold=0.3
)

# Embedding Model
hf_vectorizer = HFTextVectorizer("redis/langcache-embed-v3")

router = SemanticRouter(
    name="topic-router",
    vectorizer=hf_vectorizer,
    routes=[songs_route, aliens_route],
    redis_url=REDIS_URL,
    overwrite=True
)


# LLM
llm = ChatOpenAI(   
    # model="gpt-4.1",
    # model="gpt-5-mini",
    # model="gpt-4o-mini",
    # model="gpt-5-nano",
    model="gpt-4.1-nano",    
    temperature=0.5,
    top_p=0.95,
    max_tokens=4096
)

llmcache = SemanticCache(
    name="songscache",    # underlying search index name
    redis_url=REDIS_URL,      # redis connection url string
    distance_threshold=0.4,    # semantic cache distance threshold
    vectorizer=hf_vectorizer,
    overwrite=True
)

index = SearchIndex.from_existing("idx:tracks", r)

def ask_llm(query, text_list):
    timer_start = time.perf_counter()
    system_template = """
    You are a music expert.
    Your task is to help users find the right songs.
    You will be given a list of songs, along with their descriptions.
    Find the most appropriated song(s) based on the user's request.
    You can reply with more than one song suggestion.
    In your response, include the description, highlighting aspects that relate to the user's request.
    Don't recommend songs outside of the ones you have received.
    Include the Track ID in your response for each song.
    If no songs match the request, respond with 'I do not know'.
    %CONTEXT%
    {context}
    """
    messages = [
        SystemMessage(content=system_template.format(context=text_list)),
        HumanMessage(content=query)
    ]

    llm_response = llm.invoke(messages)
    timer_end = time.perf_counter()
    time_search = round(timer_end - timer_start, 4)
    return time_search, llm_response.content

def run_search(user_query, search_type):

    embedded_user_query = hf_vectorizer.embed(user_query)

    if search_type == "vector":
        vec_query = VectorQuery(
            vector=embedded_user_query,
            vector_field_name="vector",
            num_results=3,
            return_fields=["track_id", "name", "composer", "album", "genre", "description"],
            return_score=True
        )
    else:
        vec_query = AggregateHybridQuery(
            text=user_query,
            text_field_name="description",
            vector=embedded_user_query,
            vector_field_name="vector",
            return_fields=["track_id", "name", "composer", "album", "genre", "description"]
        )

    result = index.query(vec_query)
    return pd.DataFrame(result)   


def get_answer(user_query, use_cache):
    used_cache = False
    # check if question should be answered (description vs aliens)
    route_match = router(user_query, distance_threshold=0.7)
    # print(f"--> ROUTE: {route_match.json()}")
    if json.loads(route_match.json())['name'] != 'aliens':
        doc_count = 0
        time_search = 0
        llm_timer = 0
        hasAnswer = False
        if use_cache != "No Cache":
            print(f"--> Checking the Cache for prompt: {user_query}")
            llm_response = llmcache.check(prompt=user_query, return_fields=["prompt", "response", "metadata"])
            timer_start = time.perf_counter()
            if len(llm_response) > 0:
                response = llm_response[0]['response']
                hasAnswer = True
                used_cache = True
            timer_end = time.perf_counter()
            llm_timer = round(timer_end - timer_start, 4)
            print(f"--> Cache search matched {len(llm_response)} document(s)")

        if not hasAnswer:
            timer_start = time.perf_counter()
            result = run_search(user_query, "hybrid")
            timer_end = time.perf_counter()
            time_search = round(timer_end - timer_start, 4)                             

            doc_count = len(result)
            llm_timer, response = ask_llm(user_query, result)

            # store response in cache
            if use_cache != "No Cache":
                llmcache.store(
                    prompt=user_query,
                    response=response,
                    metadata={"type": "all"}
                )
        
        return used_cache, time_search, llm_timer, doc_count, response
    else:
        return 0, 0, 0, 0, "As a Music Assistant Model, I should not be answering questions like this."

def get_document_count():
      info = r.ft("idx:tracks").info()
      return info['num_docs']


## INPUT FOR QUESTION

input_panel = st.container()
with input_panel:

    st.text(f"[{get_document_count()} documents available in Knowledge Base]")

    panel1, na = st.columns([0.99,0.01])
    with panel1:
        use_cache = st.radio(
            "No Cache",
            ["No Cache", "Use Semantic Cache (20%)"],
            index=0,
            key=None,
            help=None,
            on_change=None,
            args=None,
            kwargs=None,
            disabled=False,
            horizontal=True,
            captions=None,
            label_visibility="hidden")


    col1, na = st.columns([0.7, 0.3])
    with col1:
        user_input = st.text_input(label="Ask your question", key="input")

    if user_input:
        with st.spinner("Getting the answer for you"):
            used_cache, time_search, llmtimer, total_docs, llm_response = get_answer(user_input, use_cache)
            cache_time = 0
            st_message = "No Documents Found"
            if used_cache:
                st_message = "Cached Response"
                cache_time = llmtimer
                llmtimer = 0

            with main_sidebar:

                dash_2 = st.container()
                with dash_2:
                    panel1, na = st.columns([0.99,0.01])
                    panel1.metric(label="Cache Check Time (sec)", value=cache_time, delta=None)
                    style_metric_cards()

                dash_3 = st.container()
                with dash_3:
                    panel1, na = st.columns([0.99,0.01])
                    panel1.metric(label="Vector DB Search Time (sec)", value=time_search, delta=None)
                    style_metric_cards()

            if total_docs == 0:
                st.text(st_message)
            else:
                st.text(f"[Found {total_docs} results in the Vector Database]")

            text_list = []
            distance_list = []

            # print(f"LLM Response: {llm_response}")
            st.markdown(llm_response)

            with main_sidebar:
                dash_4 = st.container()
                with dash_4:
                    panel1, na = st.columns([0.99,0.01])
                    panel1.metric(label="LLM Time (sec)", value=llmtimer, delta=None)
                    style_metric_cards()

            st.divider()
