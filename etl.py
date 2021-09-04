import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Processes and inserts relevant values from json song file to songs and artists tables.
            Parameters:
                    cur (cursor): A cursor object to execute postgresql queries.
                    filepath (str): a string that specifies the path of the json file.

            Returns:
                    None.
    
    '''
    # opening song file as dataframe
    df = pd.read_json(filepath ,lines=True)

    # inserting song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.flatten()
    cur.execute(song_table_insert, list(song_data))
    
    # inserting artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.flatten()
    cur.execute(artist_table_insert, list(artist_data))


def process_log_file(cur, filepath):
    '''
    Processes and inserts relevant values from json log file to time,users and songplays tables.
            Parameters:
                    cur (cursor): A cursor object to execute postgresql queries.
                    filepath (str): a string that specifies the path of the json file.

            Returns:
                    None.
    '''
    # opening log file as dataframe
    df = pd.read_json(filepath,lines=True)

    # filtering by NextSong action
    df = df[df['page']=='NextSong']

    # converting timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # inserting time data records
    time_data = (df['ts'],t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('timestamp','hour','day','week','month','year','weekday')
    timedict={}
    for i in range(len(column_labels)):
        timedict[column_labels[i]]=time_data[i]
    time_df = pd.DataFrame(timedict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # loading user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    user_df = user_df.drop_duplicates(['userId'])

    # inserting user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # inserting songplay records
    for index, row in df.iterrows():
        
        # getting songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # inserting songplay record
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Processes data from parameter filepath, gets all file extensions then iterates over them and
    executes the function specified in func parameter over each individual extension.
            Parameters:
                    cur (cursor): A cursor object to execute postgresql queries.
                    conn (connection): A connection object that establishes connection with a specific database.
                    filepath (str): a string that specifies the path of the directory.
                    func(function): A function that is to be executed over the iteration.

            Returns:
                    None.
    '''
    # getting all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # getting total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterating over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Establishes connection over a database.
    Creates a cursor object.
    Executes process_data function.
    Closes the connection
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()