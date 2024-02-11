# YOUTUBE DATAHARVESTING AND WAREHOUSING

      I've documented a project named "YouTube Data Harvesting" 

where I employed Python to extract data from YouTube through the YouTube API. The data retrieval focused on channel IDs, encompassing metrics such as the total number of videos, playlists, likes, comments, titles, captions, and descriptions.
Leveraging the Pandas library, I organized this data into data frames, subsequently transferring it to MongoDB. Within MongoDB, I established a database named 'youtube' and populated it with the collected data.
 To structure the data further, I connected MongoDB to SQL, creating four tables: 'channel_names,' 'playlists,' 'comments,' and 'videos.' 
 The final step involved implementing a Streamlit application for a user-friendly interface to explore and interact with the harvested YouTube data.


 ![youtube · Streamlit - Google Chrome 11-02-2024 20_38_42](https://github.com/raxx2203/youtube-data-harvesting-/assets/107266322/99700c63-74d6-4a61-9793-80493d384c76)

