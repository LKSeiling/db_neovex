import re
import csv
import sys
import pandas as pd
import datetime
import pytz
from decouple import Config, RepositoryEnv
from .file_utils import get_valid_filepaths, get_df, add_to_log, get_encoding, clean_table_cols

config = Config(RepositoryEnv('./.env'))
BASE_PATH = config.get('BASE_PATH')

csv.field_size_limit(sys.maxsize)

def preprocess_text(platform, df):
    if platform == "twitter":
        df["text_prep"] = df["text"].apply(lambda sub: re.sub(r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})", "", sub)).apply(lambda sub: re.sub(r"(^|[^@\w])@(\w{1,15})\b", "", sub)).apply(lambda x: x.strip())
    elif platform == "4chan":
        df["text_prep"] = df["text.x"].apply(lambda sub: re.sub(r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})", "", str(sub).replace(">","").replace("&gt;","")))
    elif platform == "legacy":
        def preprocess_text(sub):
            return re.sub(r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})", "", str(sub))
        df['text_prep'] = df.apply(lambda row: preprocess_text(row['Text']) if 'ArticleID' in row and (not pd.isna(row['ArticleID'])) else preprocess_text(row['textonly']), axis=1)
    elif platform == "reddit":
        def preprocess_text(sub):
            return re.sub(r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})", "", str(sub).replace(">","").replace("&gt;",""))
        df["text_prep"] = df.apply(lambda row: preprocess_text(row['selftext']) if row.type == "RS" else preprocess_text(row['body']), axis=1)
    else:
        if "title" in df.columns:
            df["text_prep"] = df["text"] + "\n" + df["title"]
        else:
            result = text
    return df

def fill_content(input_data, cursor):
    cursor.executemany("""
        INSERT INTO content (date, timestamp, text, title, text_prep, platform, subplatform, language, content_id, label_liwc, label_consp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, input_data)

def fill_consp_label(cursor, connection, row):
    label_consp_id = None
    cursor.execute("""
    INSERT INTO labels_consp (V1_bin, V1_prob, V2_GR_bin, V2_GR_prob, V2_NWO_bin, V2_NWO_prob)
    VALUES (%s, %s, %s, %s, %s, %s)
    RETURNING id;
    """,
    (bool(row['label_pred']), row['label_pred_probability'],
    bool(row['label_GR']), row['label_GR_probability'],
    bool(row['v2_NWO']), row['label_NWO_probability']))
    label_consp_id = cursor.fetchone()[0]
    connection.commit()

    return label_consp_id

def fill_liwc_label(cursor, connection, row):
    label_liwc_id = None
    if pd.notna(row['Segment']):
        cursor.execute("""
        INSERT INTO labels_liwc (BigWords, Segment, WC, allnone, cause, certitude, cogproc, differ, discrep, emo_anger, emo_anx, emo_neg, emo_pos, emo_sad, emotion, insight, prep, tentat)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (row['BigWords'], row['Segment'], row['WC'], row['allnone'], row['cause'], row['certitude'],
        row['cogproc'], row['differ'], row['discrep'], row['emo_anger'], row['emo_anx'], row['emo_neg'],
        row['emo_pos'], row['emo_sad'], row['emotion'], row['insight'], row['prep'], row['tentat']))
        label_liwc_id = cursor.fetchone()[0]
        connection.commit()

    return label_liwc_id

def fill_altnews(cursor, connection):  
    regex_lang = r"\/AlternativeMedia\/([^\/]*)"
    regex_subplat = r"fi_(enc_)?([^_]+)_"
    
    alt_news_path= "".join([BASE_PATH, "0_Full_Data_Classified/AlternativeMedia/"])
    all_files = get_valid_filepaths(alt_news_path)
    all_files = [filepath for filepath in all_files if "compact" not in filepath]

    path_liwc = "".join([BASE_PATH, "/3_EN_culturepaper_LIWC/en_alt_testdata_prep_liwc.csv"])
    liwc_df = get_df(path_liwc)
    liwc_red = liwc_df[['url', 'Segment', 'WC', 'BigWords', 'prep', 'allnone', 'cogproc', 'insight', 'cause', 'discrep', 'tentat', 'certitude', 'differ', 'emotion', 'emo_pos', 'emo_neg', 'emo_anx', 'emo_anger', 'emo_sad']]

    content_data = []
    for filepath in all_files:
        subplatform = re.search(regex_subplat, filepath).group(2)
        language = re.search(regex_lang, filepath).group(1)
        df = get_df(filepath)
        clean_df = clean_table_cols(df)
        prepped_df = preprocess_text("altnews", clean_df)
        write_df = pd.merge(prepped_df, liwc_red, on='url', how="left")
        
        for index, row in write_df.iterrows():
            if not pd.isna(row['date']):
                author = row['author'] if "author" in row else None
                timestamp = row['timestamp'] if "timestamp" in row else None
                try:
                    cursor.execute("""
                    INSERT INTO alt_news (url, author)
                    VALUES (%s, %s)
                    RETURNING id;
                    """, (row['url'], author))
                    alt_news_id = cursor.fetchone()[0]
                    connection.commit()

                    label_consp_id = fill_consp_label(cursor, connection, row)
                    label_liwc_id = fill_liwc_label(cursor, connection, row)
                    content_data.append((row['date'], timestamp, row['text'], row['title'], row["text_prep"], 'alt_news', subplatform, language, 
                                        alt_news_id, label_liwc_id, label_consp_id))

                except Exception as e:
                    connection.rollback()  # Roll back on error
                    add_to_log("alt_news", "Post insertion error: {e}")
            
    try:
        fill_content(content_data, cursor)
        connection.commit()
    except Exception as e:
            print(f"Error inserting Alternative News Content: {e}")
            add_to_log("alt_news", "Content insertion error: {e}")
            connection.rollback()  # Roll back on error

def fill_legnews(cursor, connection):
    
    regex_lang = r"leg_media_([^_]*)"
    
    leg_news_path = "".join([BASE_PATH, "0_Full_Data_Classified/LegacyMedia/"])
    all_files = get_valid_filepaths(leg_news_path)

    path_liwc = "".join([BASE_PATH, "3_EN_culturepaper_LIWC/legacym_testdata_prep_new_liwc.csv"])
    liwc_df = get_df(path_liwc)
    liwc_red = liwc_df[['id', 'Segment', 'WC', 'BigWords', 'prep', 'allnone', 'cogproc', 'insight', 'cause', 'discrep', 'tentat', 'certitude', 'differ', 'emotion', 'emo_pos', 'emo_neg', 'emo_anx', 'emo_anger', 'emo_sad']]

    content_data = []

    for filepath in all_files:
        language = "ger" if re.search(regex_lang, filepath).group(1) == "de" else "eng"

        chunksize = 10 ** 4
        enc = get_encoding(filepath)
        with pd.read_csv(filepath, chunksize=chunksize, encoding=enc, low_memory=False) as reader:
            for chunk in reader:
                # apply preprocessing to chunk
                prepped_chunk = preprocess_text("legacy", chunk)
                write_chunk = pd.merge(prepped_chunk, liwc_red, on='id', how="left")
                write_chunk['id'] = write_chunk.id.replace(r'Dokument', '', regex=True)
                # write to db

                for index, row in write_chunk.iterrows():
                    # if not faz
                    if ('ArticleID') not in row or (pd.isna(row['ArticleID'])):
                        info_tuple_legacy = (row['meta'], row['terms'], row['author'], None, row['id'], row['section'])
                    else: # if faz
                        url = row['Weblink'] if not pd.isna(row['Weblink']) else None
                        info_tuple_legacy = (row['meta'], row['terms'], row['Name'], url,  row['ArticleID'], row['Ressort'])
                    
                    try:
                        cursor.execute("""
                        INSERT INTO legacy_news (meta, terms, author, url, article_id, section)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id;
                        """, info_tuple_legacy)
                        alt_news_id = cursor.fetchone()[0]
                        label_consp_id = fill_consp_label(cursor, connection, row)
                        label_liwc_id = fill_liwc_label(cursor, connection, row) 
                        
                        # if not faz
                        if ('ArticleID') not in row or (pd.isna(row['ArticleID'])):
                            info_tuple_content = (row['time'], None, row['textonly'], row['title'], row["text_prep"], 'legacy_news', row["media"], language, 
                                                alt_news_id, label_liwc_id, label_consp_id)
                        else: # if faz
                            info_tuple_content = (row['time'], None, row['Text'], row['Titel'], row["text_prep"], 'legacy_news', "faz", language, 
                                                alt_news_id, label_liwc_id, label_consp_id)
                        
                        connection.commit()
                        content_data.append(info_tuple_content)
        
                    except Exception as e:
                        print(f"Error inserting Legacy News Post: {e}")
                        connection.rollback()  # Roll back on error
    
                        add_to_log("legacy_news", f"Post insertion error: {e}")
                    
    try:
        fill_content(content_data, cursor)
        connection.commit()
    except Exception as e:
            print(f"Error inserting Legacy News Content: {e}")
            add_to_log("leg_news", f"Content insertion error: {e}")
            connection.rollback()  # Roll back on error

def fill_4chan(cursor, connection): 
    def transform_num_timestamp(timestamp):
        berlin_tz = pytz.timezone('Europe/Berlin')
        berlin_time = datetime.datetime.fromtimestamp(timestamp, berlin_tz)

        return berlin_time.strftime('%Y-%m-%d %H:%M:%S%z')

    filepath = "".join([BASE_PATH, "0_Full_Data_Classified/4chan/classified_fi_4chan_all_data_prepro.csv"])
    path_liwc = "".join([BASE_PATH, "3_EN_culturepaper_LIWC/4chan_testdata_prep_new_liwc.csv"])
    liwc_df = get_df(path_liwc)
    liwc_red = liwc_df[['thread_id', 'doc_id', 'num', 'Segment', 'WC', 'BigWords', 'prep', 'allnone', 'cogproc', 'insight', 'cause', 'discrep', 'tentat', 'certitude', 'differ', 'emotion', 'emo_pos', 'emo_neg', 'emo_anx', 'emo_anger', 'emo_sad']]


    chunksize = 10 ** 4
    enc = get_encoding(filepath)
    with pd.read_csv(filepath, chunksize=chunksize, encoding=enc, low_memory=False) as reader:
        for chunk in reader:
            content_data = []
            # apply preprocessing to chunk
            clean_chunk = clean_table_cols(chunk, check_string=False)
            prepped_chunk = preprocess_text("4chan", clean_chunk)
            write_chunk = pd.merge(prepped_chunk, liwc_red, on=['thread_id', 'doc_id', 'num'], how="left")

            for index, row in write_chunk.iterrows():
                try:
                    cursor.execute("""
                    INSERT INTO fourchan (media_link, author, nreplies, num, doc_id, op, poster_country, referencing_comment, searchterm, subnum, thread_id, comments)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """, (row['media_link'], row['name'], row['nreplies'], row['num'],  row['doc_id'], row['op'], row['poster_country'], row['referencing_comment'], row['searchterm'], row['subnum'], row['thread_id'], row['comments']))
                    alt_news_id = cursor.fetchone()[0]
                    connection.commit()

                    label_consp_id = fill_consp_label(cursor, connection, row)
                    label_liwc_id = fill_liwc_label(cursor, connection, row) 
                    content_data.append((row['fourchan_date'], transform_num_timestamp(row['timestamp']), row['text.x'], row['title'], row["text_prep"], '4chan', 'pol', "eng", 
                                     alt_news_id, label_liwc_id, label_consp_id))
    
                except Exception as e:
                    print(f"Error inserting 4chan Post: {e}")
                    connection.rollback()  # Roll back on error

                    add_to_log("4chan", f"Post insertion error: {e}")
                
            try:
                fill_content(content_data, cursor)
                connection.commit()
            except Exception as e:
                    print(f"Error inserting 4chan Content: {e}")
                    add_to_log("4chan", f"Content insertion error: {e}")
                    connection.rollback()  # Roll back on error

def fill_reddit(cursor, connection):
    def create_reddit_url(row):
        return "".join(["reddit.com/r/", str(row['subreddit']), "/comments/", str(row['parent_id']), "/comment/", str(row['id'])])
    
    regex_lang = r"_reddit_([^_]*)"
    
    leg_news_path = "".join([BASE_PATH, "0_Full_Data_Classified/Reddit/"])
    all_files = get_valid_filepaths(leg_news_path)

    path_liwc = "".join([BASE_PATH, "3_EN_culturepaper_LIWC/reddit_testdata_prep_liwc.csv"])
    liwc_df = get_df(path_liwc)
    liwc_red = liwc_df[['permalink', 'Segment', 'WC', 'BigWords', 'prep', 'allnone', 'cogproc', 'insight', 'cause', 'discrep', 'tentat', 'certitude', 'differ', 'emotion', 'emo_pos', 'emo_neg', 'emo_anx', 'emo_anger', 'emo_sad']]

    content_data = []

    for filepath in all_files:
        language = "ger" if re.search(regex_lang, filepath).group(1) == "de" else "eng"

        chunksize = 10 ** 3
        enc = get_encoding(filepath)
        with pd.read_csv(filepath, chunksize=chunksize, encoding=enc, low_memory=False) as reader:
            for chunk in reader:
                # apply preprocessing to chunk
                prepped_chunk = preprocess_text("reddit", chunk)
                write_chunk = pd.merge(prepped_chunk, liwc_red, on=['permalink'], how="left")
                # write to db

                for index, row in write_chunk.iterrows():
                    if row.type == "RC": # reddit comment
                        info_tuple_legacy = (row['author'], row['id'], row['parent_id'], row['link_id'], row['searchterm'], row['terms'], "RC", row['terms'])
                    else: # reddit submission
                        info_tuple_legacy = (row['author'], row['id'], None, row['parent_id'],  row['searchterm'], row['terms'], "RS", create_reddit_url(row))
                    
                    try:
                        cursor.execute("""
                        INSERT INTO author, post_id, link_id, parent_id, searchterm, selftext, terms, type, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id;
                        """, info_tuple_legacy)
                        alt_news_id = cursor.fetchone()[0]
                        connection.commit()

                        label_consp_id = fill_consp_label(cursor, connection, row)
                        label_liwc_id = fill_liwc_label(cursor, connection, row) 
                        # reddit comment
                        if row.type == "RC":
                            info_tuple_content = (row['time_utc'], row['created_utc'], row['body'], row["text_prep"], None, 'reddit', row['subreddit'], language, 
                                                alt_news_id, label_liwc_id, label_consp_id)
                        else: # reddit submission
                            info_tuple_content = (row['time_utc'], row['created_utc'], row['selftext'], row["text_prep"], row['title'], 'reddit', row['subreddit'], language, 
                                                alt_news_id, label_liwc_id, label_consp_id)

                        content_data.append(info_tuple_content)
        
                    except Exception as e:
                        print(f"Error inserting Reddit Post: {e}")
                        connection.rollback()  # Roll back on error
    
                        add_to_log("reddit", f"Post insertion error: {e}")
                    
    try:
        fill_content(content_data, cursor)
        connection.commit()
    except Exception as e:
            print(f"Error inserting Reddit Content: {e}")
            add_to_log("reddit", f"Content insertion error: {e}")
            connection.rollback()  # Roll back on error

def fill_twitter(cursor, connection):
    print("Populating twitter users...")
    fill_twitter_users(cursor, connection)
    print("Populating tweets...")
    fill_tweets(cursor, connection)

def fill_tweets(cursor, connection):
    def get_filtered_liwc(key_df, key_column, chunk_size=10 ** 4):
        filtered_rows = []
        
        path_liwc = "".join([BASE_PATH, "/3_EN_culturepaper_LIWC/kilian_testweeks_r1full_r2call_en_fi_prep_new_liwc.csv"])    
        # Iterate over each chunk of the file
        for chunk in pd.read_csv(path_liwc, chunksize=chunk_size):
            # Filter rows where the key_column value is in the key_df
            filtered_chunk = chunk[chunk[key_column].isin(key_df[key_column])]
            # Reduce to relevant columns
            filtered_chunk = filtered_chunk[['id', 'Segment', 'WC', 'BigWords', 'prep', 'allnone', 'cogproc', 'insight', 'cause', 'discrep', 'tentat', 'certitude', 'differ', 'emotion', 'emo_pos', 'emo_neg', 'emo_anx', 'emo_anger', 'emo_sad']]
            filtered_rows.append(filtered_chunk)
        
        # Concatenate all filtered rows into a single DataFrame
        filtered_df = pd.concat(filtered_rows, ignore_index=True)
        return filtered_df
    
    regex_lang = r"TwitterTweets\/([^\/]*)"

    tweets_path= "".join([BASE_PATH, "0_Full_Data_Classified/TwitterTweets/"])
    all_files = get_valid_filepaths(tweets_path)

    content_data = []
    for filepath in all_files:
        language = "ger" if re.search(regex_lang, filepath).group(1) == "ger" else "eng"
        df = get_df(filepath)
        clean_df = clean_table_cols(df)
        prepped_df = preprocess_text("twitter", clean_df)
        liwc_red = get_filtered_liwc(prepped_df, "id")
        write_df = pd.merge(prepped_df, liwc_red, on='url', how="left")
        
        for index, row in write_df.iterrows():
            author = row['author'] if "author" in row else None
            timestamp = row['timestamp'] if "timestamp" in row else None
            sampled = True
            try:
                cursor.execute("""
                INSERT INTO twitter (tweet_id, ref, refid , author_id, sampled)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id;
                """, (row['id'], row['refid'], row['id'], row['id'],sampled))
                tweet_id = cursor.fetchone()[0]
                connection.commit()
                
                label_consp_id = fill_consp_label(cursor, connection, row) 
                label_liwc_id = fill_liwc_label(cursor, connection, row)
                content_data.append((row['time'], row['time'], row['text'], None, row["text_prep"], 'twitter', None, language, 
                                     tweet_id, label_liwc_id, label_consp_id))

            except Exception as e:
                print(f"Error inserting Twitter Post: {e}")
                connection.rollback()  # Roll back on error
                add_to_log("twitter", f"Post insertion error: {e}")
            
    try:
        fill_content(content_data, cursor)
        connection.commit()
    except Exception as e:
            print(f"Error inserting Twitter Content: {e}")
            add_to_log("twitter", f"Content insertion error: {e}")
            connection.rollback()  # Roll back on error

def fill_twitter_users(cursor, connection):
    tweets_path= "".join([BASE_PATH, "0_Full_Data_Classified/TwitterUsernames/"])
    all_files = get_valid_filepaths(tweets_path)

    for filepath in all_files:
        df = pd.read_csv(all_files[0], header=None, index_col=0, names=['author_id','username'])
        input_data = list(df.itertuples(index=False, name=None))

        try:
            cursor.executemany("""
            INSERT INTO twitter_user (author_id, username)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
            """, (input_data))
            
            connection.commit()

        except Exception as e:
                print(f"Error inserting Twitter User: {e}")
                connection.rollback()  # Roll back on error
                msg = "\n".join([f"User insertion error :{e}"])
                add_to_log("twitter_user", f"Post insertion error: {e}")