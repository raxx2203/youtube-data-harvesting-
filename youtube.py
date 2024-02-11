from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


def api_connect() :
    Api_key="AIzaSyB0-pl5DVtihhJ7eF5SlF_fEkLLaK6K67k"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_key)
    return youtube
youtube=api_connect()



def channels_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response["items"]:
         data=dict(channel_Name=i['snippet']['title'],
             channel_id=i["id"],
             subscribers=i['statistics']['subscriberCount'],
             views=i['statistics']['viewCount'],
             Total_videos=i["statistics"]["videoCount"],
             channel_description=i['snippet']['description'],
             playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data

def get_videos_ids(channel_id):
    Video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            Video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return Video_ids



def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id  
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                channel_name=item['snippet']['channelTitle'],
                channel_id=item['snippet']['channelId'],
                video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                View=item['statistics'].get('viewCount'),
                Likes=item["statistics"].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data



def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:

         request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id, 
            maxResults=50
        )
        response = request.execute()

        for item in response['items']:
            data=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_Authour=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
            comment_data.append(data)

    except:
        pass
    return comment_data




# get_playlist_details
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,  # Use 'channel_id' consistently
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get('items'):
            data = dict(
                Playlist_Ids=item['id'],
                Title=item['snippet']['title'],
                Channel_id=item['snippet']['channelId'],
                channel_name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                video_count=item['contentDetails']['itemCount']
            )
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return All_data

     

#upload to mongodb

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]



def  channel_details(channel_id):
    ch_details=channels_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_informationn": com_details})
    
    return "upload completed successfully"





#table creation
 
def channel_tables():

    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="raveena",
                        database="youtube_data",
                        port="5432"

    )

    cursor=mydb.cursor()

    drop_qurey='''drop table if exists channel'''
    cursor.execute(drop_qurey)
    mydb.commit()


    try:
        create_query='''create table if not exists channel(channel_Name varchar(100),
                                                            channel_id varchar(80) primary key,
                                                            subscribers bigint,
                                                            views bigint,
                                                            Total_videos bigint,
                                                            channel_description text,
                                                            playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table already created") 


    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channel(channel_Name,
                        channel_id,
                        subscribers,
                        views,
                        Total_videos,
                        channel_description,
                        playlist_id )
                        
                        values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['channel_Name'],
                row['channel_id'],
                row['subscribers'],
                row['views'],
                row['Total_videos'],
                row['channel_description'],
                row['playlist_id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channel values are already inserted")



def playlist_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="raveena",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists playlists(
        Playlist_Ids varchar(100) primary key,
        Title varchar(100),
        Channel_id varchar(100),
        channel_name varchar(100),
        PublishedAt timestamp,
        video_count int
    )'''

    cursor.execute(create_query)
    mydb.commit()

    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data['playlist_information'][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''insert into playlists(
            Playlist_Ids,
            Title,
            Channel_id,
            channel_name,
            PublishedAt,
            video_count
        ) values(%s,%s,%s,%s,%s,%s)'''

        values = (
            row['Playlist_Ids'],
            row['Title'],
            row['Channel_id'],
            row['channel_name'],
            row['PublishedAt'],
            row['video_count']
        )

        cursor.execute(insert_query, values)
        mydb.commit()


def video_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="raveena",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists videos(
                        channel_name varchar(100),
                        channel_id varchar(100),
                        video_Id varchar(30) primary key,
                        Title varchar(150),
                        Tags text,
                        Thumbnail varchar(200),
                        Description text,
                        Published timestamp,
                        Duration interval,
                        View bigint,
                        Likes bigint,
                        Comments int,
                        Favorite_Count int,
                        Definition varchar(10),
                        Caption_status varchar(50)
                    )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data['video_information'][i])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''insert into videos(
                            channel_name,
                            channel_id,
                            video_Id,
                            Title,
                            Tags,
                            Thumbnail,
                            Description,
                            Published,
                            Duration,
                            View,
                            Likes,
                            Comments,
                            Favorite_Count,
                            Definition,
                            Caption_status
                        ) 
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (
            row['channel_name'],
            row['channel_id'],
            row['video_Id'],
            row['Title'],
            row['Tags'],
            row['Thumbnail'],
            row['Description'],
            row['Published'],
            row['Duration'],
            row['View'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_status']
        )

        cursor.execute(insert_query, values)
        mydb.commit()

def comment_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="raveena",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments(
                        Comment_id varchar(100) primary key,
                        video_Id varchar(50),
                        comment_Text text,
                        comment_Authour varchar(150),
                        comment_Published timestamp
                    )'''

    cursor.execute(create_query)
    mydb.commit()

    cm_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for cm_data in coll1.find({}, {"_id": 0, 'comment_informationn': 1}):
        for i in range(len(cm_data["comment_informationn"])):
            cm_list.append(cm_data['comment_informationn'][i])
    df3 = pd.DataFrame(cm_list)

    for index, row in df3.iterrows():
        insert_query = '''insert into comments(Comment_id,
                        video_Id,
                        comment_Text,
                        comment_Authour,
                        comment_Published
                    ) 
                    values (%s, %s, %s, %s, %s)'''

        values = (
            row['Comment_id'],
            row['video_Id'],
            row['comment_Text'],
            row['comment_Authour'],
            row['comment_Published']
        )

        cursor.execute(insert_query, values)
        mydb.commit()




def tabel():
    channel_tables()
    playlist_table()
    video_table()
    comment_table()
    
    return "table created succesfully"



def show_channel():
  ch_list=[]
  db=client["youtube_data"]
  coll1=db["channel_details"]
  for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
  df=st.dataframe(ch_list)

  return df


def show_playlists():
   pl_list = [] 
   db = client["youtube_data"]
   coll1 = db["channel_details"]
   for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
       for i in range(len(pl_data["playlist_information"])):
          pl_list.append(pl_data['playlist_information'][i])
       df1 = st.dataframe(pl_list)
       return df1



def show_comment():
    cm_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for cm_data in coll1.find({}, {"_id": 0, 'comment_informationn': 1}):
        for i in range(len(cm_data["comment_informationn"])):
         cm_list.append(cm_data['comment_informationn'][i])
        df3 = st.dataframe(cm_list)
        return df3


def show_video():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
       for i in range(len(vi_data["video_information"])):
          vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)

    return df2


#streamlit 

with st.sidebar:
    st.title(":black[YOUTUBE DATA HARVESTING AND WAREHOSUING]")
    st.title("Expertise")
    st.caption("python programming")
    st.caption("Data Harvesting")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data collection using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button(" click here "):
   ch_ids=[]
   db=client["youtube_data"]
   coll1=db["channel_details"]
   for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
       ch_ids.append(ch_data["channel_information"]['channel_id'])

   if channel_id in ch_ids:
       st.success("channel details already exists")
   else:
       insert=channel_details(channel_id)
       st.success(insert)

if st.button("migrate to sql"):
    Table=tabel()
    st.success(Table)


show_table=st.radio("select the table for view",("CHANNELS","PLAYLIST","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel()


elif show_table=="PLAYLIST":
    show_playlists()

elif show_table=="VIDEOS":
    show_video()

if show_table=="COMMENTS":
    show_comment()



#sql connection

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="raveena",
    database="youtube_data",
    port="5432"
)
cursor=mydb.cursor()


question=st.selectbox("SELECT YOUR QUESTION",("1.All the vidoes and the channel name",
                                              "2.channels with most number of videos",
                                              "3. 10 most viwed videos",
                                              "4.comments in ecah videos",
                                              "5. videos with hihgest likes",
                                              "6. likes of all video",
                                              "7. videos of each channel",
                                              "8. videos published in the yera of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))


if question=="1.All the vidoes and the channel name":


   query1='''Select title as videos,channel_name as channelname from videos'''
   cursor.execute(query1)
   mydb.commit()
   t1=cursor.fetchall()

   df=pd.DataFrame(t1,columns=["video title","channel name"])
   st.write(df)

elif question=="2.channels with most number of videos":


   query2='''Select channel_name as channelname ,total_videos as no_of from channel
        order by total_videos desc'''
   cursor.execute(query2)
   mydb.commit()
   t2=cursor.fetchall()

   df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
   st.write(df2)


elif question=="3. 10 most viwed videos":


 query3='''Select view as view,channel_name as channelname,title as videotitle from videos
            where view is not null order by view desc limit 10'''
 cursor.execute(query3)
 mydb.commit()
 t3=cursor.fetchall()

 df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
 st.write(df3)

elif question=="4.comments in ecah videos":


 query4='''Select comments as no_comments,title as videotitle from videos where comments is not null'''
 cursor.execute(query4)
 mydb.commit()
 t4=cursor.fetchall()

 df4=pd.DataFrame(t4,columns=["no_comments","videotitle"])
 st.write(df4)

elif question=="5. videos with hihgest likes":


 query5='''Select title as videotitle,channel_name as channelname, likes
        as likecount from videos where likes is not null order by likes desc'''
 cursor.execute(query5)
 mydb.commit()
 t5=cursor.fetchall()

 df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
 st.write(df5)

elif question=="6. likes of all video":


 query6='''Select likes as likecount,title as videotitle from videos'''
 cursor.execute(query6)
 mydb.commit()
 t6=cursor.fetchall()

 df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
 st.write(df6)


elif question=="7. videos of each channel":


 query7='''Select channel_name as channelname, views as totalviews from channel'''
 cursor.execute(query7)
 mydb.commit()
 t7=cursor.fetchall()

 df7=pd.DataFrame(t7,columns=["channel name","total views"])
 st.write(df7)


elif question=="8. videos published in the yera of 2022":


 query8 = '''
    SELECT title as video_title, published as videorelease, channel_name as channelname
    FROM videos
    WHERE EXTRACT(year FROM published) = 2022
'''

 cursor.execute(query8)
 t8 = cursor.fetchall()

 df8 = pd.DataFrame(t8, columns=["videotitle", "videorelease", "channelname"])
 st.write(df8)


elif question=="9. average duration of all videos in each channel":


 query9 = '''
    SELECT channel_name as channelname,AVG(duration) as avergeduration
    from videos group by channel_name
'''

 cursor.execute(query9)
 t9 = cursor.fetchall()

 df9 = pd.DataFrame(t9, columns=[ "channelname","averageduration"])
 st.write(df9)


elif question=="10. videos with highest number of comments":


 query10 = '''
    SELECT  title as videotitle,channel_name as channelname,comments as comments from videos 
    where comments is not null order by comments desc
    ''' 

 cursor.execute(query10)
 t10 = cursor.fetchall()

 df10 = pd.DataFrame(t10, columns=[ "video title","channelname","comments"])
 st.write(df10)


 


      


    



    


