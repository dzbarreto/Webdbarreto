import os
import csv
import http.client
import httplib2
import logging
import sys
import time
import pymysql
import numpy as np

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import argparser, run_flow

CLIENT_SECRETS_FILE = "./client_secret_58839841649-5gm9fau8qkr0ljpfa6o60t7mg0u303fn.apps.googleusercontent.com.json"

MISSING_CLIENT_SECRETS_MESSAGE = os.path.abspath(
    os.path.join(os.path.dirname(__file__), CLIENT_SECRETS_FILE))

YOUTUBE_SCOPES = ("https://www.googleapis.com/auth/youtube", "https://www.googleapis.com/auth/youtube.force-ssl",
                  "https://www.googleapis.com/auth/youtube.readonly", "https://www.googleapis.com/auth/youtubepartner")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


def get_authenticated_services(args):
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=" ".join(
        YOUTUBE_SCOPES), message=MISSING_CLIENT_SECRETS_MESSAGE)
    storage = Storage("%s-oauth2.json" % sys.argv[0])
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage, args)

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                    http=credentials.authorize(httplib2.Http()))

    # return (youtube, youtube_partner)
    return youtube


def search_id(youtube_partner, page_token=""):
    search_response = youtube_partner.search().list(
        q='gospel',
        part='id',
        maxResults=50,
        type='video',
        order='viewCount',
        pageToken=page_token

    ).execute()

    return search_response


def get_video_info(youtube_partner, video_id):
    video_response = youtube_partner.videos().list(
        part="statistics,snippet",
        id=video_id,
    ).execute()
    return video_response


args = argparser.parse_args()

youtube_partner = get_authenticated_services(args)


video_csv = 'Video_Id;Video_Title;Dt_Video_Pub;Channel_Title;Channel_ID;ViewCount \n'


i = 0
pageToken = ""
while i < 20:
    resp = search_id(youtube_partner, pageToken)

    pageToken = resp['nextPageToken']

    for item in resp["items"]:
        video_id = item["id"]["videoId"]
        video_info = get_video_info(youtube_partner, video_id)
        video_csv+=video_id + ';' + video_info["items"][0]["snippet"]["title"] + ';' + video_info["items"][0]["snippet"]["publishedAt"] + ';' + video_info["items"][0]["snippet"]["channelTitle"] + ';' + video_info["items"][0]["snippet"]["channelId"] + ';' + video_info["items"][0]["statistics"]["viewCount"] + '\n'

    i = i+1

with open('saida.csv', 'a+', encoding='utf-8') as saida:
    saida.write(video_csv)

