from googleapiclient.discovery import build 
import mysql.connector
from mysql.connector import Error
import streamlit as st
import pandas as pd 
import re


# Youtube_data_retrieve

# API_key_connection 

def Api_connect():

    Api_key = "Your_APIkey"
    youtube = build("youtube", "v3", developerKey=Api_key)
    return youtube

youtube = Api_connect()


# Get_channel_information

def get_channel_info(channel_id):
    
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    channel_data=request.execute()

    for i in channel_data['items']:
        channel_info =dict(
                            Channel_Name = i['snippet']['title'],
                            Channel_Id = i['id'],
                            Channel_Subcribers = i['statistics']['subscriberCount'],
                            Channel_Views = i['statistics']['viewCount'],
                            Channel_Description = i['snippet']['description'],
                            Total_Videos = i["statistics"]["videoCount"],
                            Upload_Id = i["contentDetails"]["relatedPlaylists"]["uploads"]
                          )
    return channel_info

# Get_videos_id 

def get_videos_id(channel_id):

    video_ids=[]

    # Get_playlist_id

    response = youtube.channels().list(
                                    part="snippet,contentDetails,statistics",
                                    id=channel_id).execute()

    playlist_id= response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None

    while True:
        
        response1 = youtube.playlistItems().list(
                                            part="snippet",
                                            maxResults=50,
                                            playlistId=playlist_id,
                                            pageToken = next_page_token).execute()
        
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

# Get_videos_information

def get_videos_info(video_ids):

    video_details = []

    for video_id in video_ids:
        response = youtube.videos().list(
              part="snippet,contentDetails,statistics",
              id=video_id).execute()

        for video in response['items']:
            video_data = dict(  
                                Channel_Name = video['snippet']['channelTitle'],
                                Channel_Id = video['snippet']['channelId'],
                                Video_Id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_Date = video['snippet']['publishedAt'],
                                Duration = convert_to_minutes(video['contentDetails']['duration']),
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favourites = video['statistics']['favoriteCount'],
                                #Definition = video['contentDetails']['definition'],
                                Caption_Status = video['contentDetails']['caption'])
            video_details.append(video_data)
    
    return video_details

# Get_comment_information

def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            response=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
                ).execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                comment_data.append(data)
            
    except:
        pass
    return comment_data

# Converting the Duration of the video in minutes for easy processing of the data in database

def convert_to_minutes(time_string):
    hour_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_min_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M', time_string)
    min_sec_match = re.match(r'PT(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_sec_match = re.match(r'PT(?P<hours>\d+)H(?P<seconds>\d+)S', time_string)
    hour_only_match = re.match(r'PT(?P<hours>\d+)H', time_string)
    minute_match = re.match(r'PT(?P<minutes>\d+)M', time_string)
    sec_match = re.match(r'PT(?P<seconds>\d+)S', time_string)

    if hour_match:
        hours = int(hour_match.group('hours')) if hour_match.group('hours') else 0
        minutes = int(hour_match.group('minutes')) if hour_match.group('minutes') else 0
        seconds = int(hour_match.group('seconds')) if hour_match.group('seconds') else 0
        return hours * 60 + minutes + seconds / 60
    elif hour_min_match:
        hours = int(hour_min_match.group('hours')) if hour_min_match.group('hours') else 0
        minutes = int(hour_min_match.group('minutes')) if hour_min_match.group('minutes') else 0
        return hours * 60 + minutes
    elif min_sec_match:
        minutes = int(min_sec_match.group('minutes')) if min_sec_match.group('minutes') else 0
        seconds = int(min_sec_match.group('seconds')) if min_sec_match.group('seconds') else 0
        return minutes + seconds / 60
    elif hour_sec_match:
        hours = int(hour_sec_match.group('hours')) if hour_sec_match.group('hours') else 0
        seconds = int(hour_sec_match.group('seconds')) if hour_sec_match.group('seconds') else 0
        return hours * 60 + seconds / 60
    elif hour_only_match:
        hours = int(hour_only_match.group('hours')) if hour_only_match.group('hours') else 0
        return hours * 60
    elif minute_match:
        minutes = int(minute_match.group('minutes')) if minute_match.group('minutes') else 0
        return minutes
    elif sec_match:
        seconds = int(sec_match.group('seconds')) if sec_match.group('seconds') else 0
        return seconds / 60
    else:
      return 0

# Extract_all_ youtube_data

def youtube_data_extraction(channel_id):

    id = channel_id
    channel_info = get_channel_info(id)
    video_ids = get_videos_id(id)
    video_detail = []
    comment_detail = []
    video_detail = get_videos_info(video_ids)
    comment_detail = get_comment_info(video_ids)
    video_data = pd.DataFrame(video_detail)
    comment_data = pd.DataFrame(comment_detail)

    return (channel_info,video_data,comment_data)

# Table_creation_for_data_visualization

# Channels_table

def show_channels_table(channel_id):
    channel_info =channel_id
    channel_table=get_channel_info(channel_info)

    df=st.dataframe(channel_table)

    return df

# Videos_Table

def show_videos_table(channel_id):
    id = channel_id
    video_ids = get_videos_id(id)
    video_detail = []
    video_detail = get_videos_info(video_ids)
    df = st.dataframe(video_detail)
    
    return df

# Comments_Table

def show_comments_table(channel_id):
    id = channel_id
    video_ids = get_videos_id(id)
    comment_detail = []
    comment_detail = get_comment_info(video_ids)
    df = st.dataframe(comment_detail)

    return df

# Data_transfer_to_MySql

def insert_youtube_data(channel_data, video_data, comment_data):
    try:
        print("Storing the Data in SQL Warehouse")
        
        mydb = mysql.connector.connect(
                                host = "hostname",
                                user = "username",
                                password = "password",
                                database = "schemaname"
                              )

        
        mycursor = mydb.cursor()

        # Table creation for channels , videos ,comments in SQL 

        mycursor.execute('''CREATE TABLE IF NOT EXISTS channels(
                                                                Channel_Name VARCHAR(100), 
                                                                Channel_Id VARCHAR(80) NOT NULL, 
                                                                Channel_Subscribers BIGINT, 
                                                                Channel_Views BIGINT, 
                                                                Channel_Description TEXT,
                                                                Total_Videos VARCHAR(50),
                                                                Upload_Id VARCHAR(80), 
                                                                PRIMARY KEY (Channel_Id) 
                                                                )''')
        
        mycursor.execute('''CREATE TABLE IF NOT EXISTS videos (
                                                                Channel_Name VARCHAR(255),
                                                                Channel_Id VARCHAR(100),
                                                                Video_Id VARCHAR(50) NOT NULL,
                                                                Title VARCHAR(255),
                                                                Tags VARCHAR(255),
                                                                Thumbnail VARCHAR(255),
                                                                Video_Description TEXT, 
                                                                Published_Date DATETIME, 
                                                                Duration INT,
                                                                View_Count BIGINT, 
                                                                Like_Count BIGINT,
                                                                Comment_Count INT,
                                                                Favorite_Count INT, 
                                                                Caption_Status VARCHAR(100),
                                                                PRIMARY KEY (Video_ID)
                                                              )''')
        
        mycursor.execute('''CREATE TABLE IF NOT EXISTS comments(
                                                                Comment_Id VARCHAR(50) NOT NULL, 
                                                                Video_Id VARCHAR(50), 
                                                                Comment_Text TEXT, 
                                                                Comment_Author VARCHAR(100), 
                                                                Published_At DATETIME,
                                                                PRIMARY KEY (Comment_Id)
                                                                )''')
        
        # Insert_channel_data

        mycursor.execute('''INSERT INTO channels
                            (Channel_Name, Channel_Id, Channel_Subscribers, Channel_Views, Channel_Description, Total_Videos, Upload_Id)
                         
                            VALUES (%s, %s, %s, %s, %s, %s, %s)''',

                            (channel_data["Channel_Name"],channel_data["Channel_Id"],channel_data["Channel_Subcribers"],channel_data["Channel_Views"],
                             channel_data["Channel_Description"],channel_data["Total_Videos"],channel_data["Upload_Id"]))
        mydb.commit()

        # Insert_video_data

        for _, row1 in video_data.iterrows():
                video_datetime_str = row1['Published_Date'].replace('T', ' ').replace('Z', '')
                mycursor.execute('''INSERT INTO videos 
                                (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Video_Description, Published_Date, Duration, View_Count, Like_Count,
                                Comment_Count, Favorite_Count, Caption_Status) 
                            
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',

                                (row1['Channel_Name'], row1['Channel_Id'], row1['Video_Id'], row1['Title'], row1['Tags'],row1['Thumbnail'],
                                 row1['Description'], video_datetime_str, row1['Duration'], row1['Views'], row1['Likes'],
                                 row1['Comments'], row1['Favourites'], row1['Caption_Status']))
                mydb.commit()

        # Insert_comment_data

        for _, row2 in comment_data.iterrows():
                cmt_datetime_str = row2['Comment_Published'].replace('T', ' ').replace('Z', '')
                mycursor.execute('''INSERT INTO comments  
                                (Comment_Id, Video_Id, Comment_Text, Comment_Author, Published_At) 
                            
                                VALUES (%s, %s, %s, %s, %s)''',

                                (row2["Comment_Id"], row2["Video_Id"], row2["Comment_Text"], row2["Comment_Author"],cmt_datetime_str))
                mydb.commit()
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed")

# Streamlit_part    

st.title("YOUTUBE DATAHARVESTING AND WAREHOUSING")

with st.sidebar:
    st.title(":green[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("SKILLS :")
    st.caption(":grey[API Integration]")
    st.caption(":grey[Data collection]")
    st.caption(":grey[Data management using SQL]")
    st.caption(":grey[Python scripting]")
channel_id = st.text_input("Enter YouTube Channel ID:")

if st.button("Data Extraction and storage",type = "primary"): 
        channel, video, comment = youtube_data_extraction(channel_id)
        insert_youtube_data(channel, video, comment)
        st.success("Data migrated to sql")

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    st.success("Channel_information")
    show_channels_table(channel_id)

elif show_table=="VIDEOS":
    st.success("Video_information")
    show_videos_table(channel_id)

elif show_table=="COMMENTS":
    st.success("Comment_information")
    show_comments_table(channel_id)



# Query section    

mydb = mysql.connector.connect(
                                host = "hostname",
                                user = "username",
                                password = "password",
                                database = "schemaname"
                               
                              )

query_options = [
                "What are the names of all the videos and their corresponding channels?",
                "Which channels have the most number of videos, and how many videos do they have?",
                "What are the top 10 most viewed videos and their respective channels?",
                "How many comments were made on each video, and what are their corresponding video names?",
                "Which videos have the highest number of likes, and what are their corresponding channel names?",
                "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                "What is the total number of views for each channel, and what are their corresponding channel names?",
                "What are the names of all the channels that have published videos in the year 2022?",
                "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "Which videos have the highest number of comments, and what are their corresponding channel names?"
                 ]
selected_query = st.selectbox("Select Question:", query_options)

if st.button("Execute", type='primary'):
    
    if selected_query == query_options[0]:
        query_result = pd.read_sql_query("SELECT videos.Title, channels.Channel_Name FROM videos INNER JOIN channels ON videos.Channel_Id = channels.Channel_Id", mydb)
    elif selected_query == query_options[1]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name, COUNT(Video_ID) AS Num_Videos FROM channels INNER JOIN videos ON channels.Channel_ID = videos.Channel_ID GROUP BY Channel_Name ORDER BY Num_Videos DESC LIMIT 1", mydb)
    elif selected_query == query_options[2]:
        query_result = pd.read_sql_query("SELECT videos.Title, channels.Channel_Name FROM videos INNER JOIN channels ON videos.Channel_Id = channels.Channel_Id ORDER BY View_Count DESC LIMIT 10;", mydb)
    elif selected_query == query_options[3]:
        query_result = pd.read_sql_query("SELECT Title, COUNT(Comment_Id) AS Number_of_Comments FROM videos INNER JOIN comments ON videos.Video_Id = comments.Video_Id GROUP BY Title ORDER BY Number_of_Comments DESC LIMIT 10;", mydb)
    elif selected_query == query_options[4]:
        query_result = pd.read_sql_query("SELECT videos.Title, videos.Channel_Name FROM videos INNER JOIN channels ON videos.Channel_Id = channels.Channel_Id ORDER BY Like_Count DESC LIMIT 1", mydb)
    elif selected_query == query_options[5]:
        query_result = pd.read_sql_query("SELECT videos.Title, SUM(Like_Count) AS Total_Likes FROM videos GROUP BY Title", mydb)
    elif selected_query == query_options[6]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name, SUM(channels.Channel_Views) AS Total_Views FROM channels INNER JOIN videos ON channels.Channel_Id = videos.Channel_Id GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[7]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name FROM channels INNER JOIN videos ON channels.Channel_Id = videos.Channel_Id WHERE SUBSTRING(videos.Published_Date, 1, 4) = '2022' GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[8]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name, AVG(videos.Duration) AS Average_Duration FROM channels INNER JOIN videos ON videos.Channel_Id = channels.Channel_Id GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[9]:
        query_result = pd.read_sql_query("SELECT videos.Title, videos.Channel_Name FROM videos INNER JOIN channels ON videos.Channel_Id = channels.Channel_Id ORDER BY COUNT(Comment_Id) DESC LIMIT 1", mydb)
    mydb.close()

    st.dataframe(query_result)



     



