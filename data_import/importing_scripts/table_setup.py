def create_table(SQL_STATEMENT, cursor):
    cursor.execute(SQL_STATEMENT)

def create_tables(cursor, connection):
    create_types(cursor, connection)
    create_altnews_table(cursor, connection)
    create_legacy_table(cursor, connection)
    create_4chan_table(cursor, connection)
    create_reddit_table(cursor, connection)
    create_twitteruser_table(cursor, connection)
    create_tweets_table(cursor, connection)
    create_consplabels_table(cursor, connection)
    create_liwclabels_table(cursor, connection)
    create_content_table(cursor, connection)

def create_types(cursor, connection):
    def create_platform_type():
        try:
            SQL_STATEMENT = """
            CREATE TYPE platform_type AS ENUM ('alt_news', 'legacy_news', '4chan', 'reddit', 'twitter');
            """
            create_table(SQL_STATEMENT, cursor)
            connection.commit()
        except Exception as e:
            print(f"Type 'platform_type' might already exist: {e}")
            connection.rollback()  # Roll back on error

    def create_language_type():
        try:
            SQL_STATEMENT = """
            CREATE TYPE language_type AS ENUM ('eng', 'ger');
            """
            create_table(SQL_STATEMENT, cursor)
            connection.commit()
        except Exception as e:
            print(f"Type 'language_type' might already exist: {e}")
            connection.rollback()  # Roll back on error

    create_platform_type()
    create_language_type()
    

def create_content_table(cursor, connection):
    try:
        SQL_STATEMENT = """ CREATE TABLE IF NOT EXISTS content (
        id BIGSERIAL PRIMARY KEY,
        date DATE NOT NULL,
        timestamp TIMESTAMP,
        text TEXT NOT NULL,
        text_prep TEXT,
        title TEXT,
        platform platform_type NOT NULL,
        subplatform VARCHAR(50),
        language language_type NOT NULL,
        content_id BIGINT NOT NULL,
        label_liwc BIGINT UNIQUE,
        label_consp BIGINT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (content_id, platform),
        CONSTRAINT fk_label_liwc FOREIGN KEY (label_liwc) REFERENCES labels_liwc(id) ON DELETE CASCADE,
        CONSTRAINT fk_label_consp FOREIGN KEY (label_consp) REFERENCES labels_consp(id) ON DELETE CASCADE
        );"""
        create_table(SQL_STATEMENT, cursor)
        connection.commit()
    except Exception as e:
        print(f"Error creating Content Table: {e}")
        connection.rollback()  # Roll back on error

    try:
        # Create trigger function to set updated_at
        cursor.execute("""
        -- Trigger Function for validation and timestamp update
        CREATE OR REPLACE FUNCTION validate_and_manage_content()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Check content_id
            IF NEW.platform = 'alt_news' THEN
                IF NOT EXISTS (SELECT 1 FROM alt_news WHERE id = NEW.content_id) THEN
                    RAISE EXCEPTION 'content_id % does not exist in alt_news', NEW.content_id;
                END IF;
            ELSIF NEW.platform = 'legacy_news' THEN
                IF NOT EXISTS (SELECT 1 FROM legacy_news WHERE id = NEW.content_id) THEN
                    RAISE EXCEPTION 'content_id % does not exist in legacy_news', NEW.content_id;
                END IF;
            ELSIF NEW.platform = '4chan' THEN
                IF NOT EXISTS (SELECT 1 FROM fourchan WHERE id = NEW.content_id) THEN
                    RAISE EXCEPTION 'content_id % does not exist in fourchan', NEW.content_id;
                END IF;
            ELSIF NEW.platform = 'reddit' THEN
                IF NOT EXISTS (SELECT 1 FROM reddit WHERE id = NEW.content_id) THEN
                    RAISE EXCEPTION 'content_id % does not exist in reddit', NEW.content_id;
                END IF;
            ELSIF NEW.platform = 'twitter' THEN
                IF NOT EXISTS (SELECT 1 FROM twitter WHERE id = NEW.content_id) THEN
                    RAISE EXCEPTION 'content_id % does not exist in twitter', NEW.content_id;
                END IF;
            END IF;

            -- Update timestamp
            NEW.updated_at = CURRENT_TIMESTAMP;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        -- Trigger Function for deletion cascade
        CREATE OR REPLACE FUNCTION delete_content_cascade()
        RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM content WHERE content_id = OLD.id AND platform = TG_TABLE_NAME;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        """)
        connection.commit()
    except Exception as e:
        print(f"Error creating trigger function: {e}")
        connection.rollback()  # Roll back on error

    try:
        # Add trigger
        cursor.execute("""
        -- Trigger for Insert and Update
        CREATE TRIGGER content_validate_and_update
        BEFORE INSERT OR UPDATE ON content
        FOR EACH ROW
        EXECUTE FUNCTION validate_and_manage_content();

        -- Triggers for Delete on platform tables
        CREATE TRIGGER delete_alt_news_content
        AFTER DELETE ON alt_news
        FOR EACH ROW
        EXECUTE FUNCTION delete_content_cascade();

        CREATE TRIGGER delete_legacy_news_content
        AFTER DELETE ON legacy_news
        FOR EACH ROW
        EXECUTE FUNCTION delete_content_cascade();

        CREATE TRIGGER delete_fourchan_content
        AFTER DELETE ON fourchan
        FOR EACH ROW
        EXECUTE FUNCTION delete_content_cascade();

        CREATE TRIGGER delete_reddit_content
        AFTER DELETE ON reddit
        FOR EACH ROW
        EXECUTE FUNCTION delete_content_cascade();

        CREATE TRIGGER delete_twitter_content
        AFTER DELETE ON twitter
        FOR EACH ROW
        EXECUTE FUNCTION delete_content_cascade();
        """)
        connection.commit()
    except Exception as e:
        print(f"Error creating trigger: {e}")
        connection.rollback()  # Roll back on error

def create_altnews_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE alt_news (
        id BIGSERIAL PRIMARY KEY,
        url TEXT NOT NULL UNIQUE,
        author VARCHAR(255)
        );
        """
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating Alt News Table: {e}")
        connection.rollback()  # Roll back on error
    
def create_legacy_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE legacy_news (
        id BIGSERIAL PRIMARY KEY,
        meta TEXT,
        terms TEXT,
        author TEXT,
        url TEXT UNIQUE,
        article_id VARCHAR(50) NOT NULL UNIQUE,
        section TEXT
        );"""
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating Legacy News Table: {e}")
        connection.rollback()  # Roll back on error
    
def create_4chan_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE fourchan (
        id BIGSERIAL PRIMARY KEY,
        media_link TEXT,
        author VARCHAR(255),
        nreplies DOUBLE PRECISION,
        num BIGINT,
        doc_id BIGINT,
        op DOUBLE PRECISION,
        poster_country VARCHAR(5),
        referencing_comment DOUBLE PRECISION,
        searchterm TEXT,
        subnum BIGINT,
        thread_id BIGINT,
        comments TEXT,
        UNIQUE (thread_id, doc_id, num)
        );"""
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating 4chan Table: {e}")
        connection.rollback()  # Roll back on error
    
def create_reddit_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE reddit (
        id BIGSERIAL PRIMARY KEY,
        author VARCHAR(225),
        post_id VARCHAR(225),
        link_id VARCHAR(225),
        parent_id VARCHAR(225),
        searchterm TEXT,
        selftext TEXT,
        terms TEXT,
        type VARCHAR(225),
        url TEXT NOT NULL UNIQUE, 
        coded BOOL
        );"""
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating Reddit Table: {e}")
        connection.rollback()  # Roll back on error
    
def create_tweets_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE twitter (
        id BIGSERIAL PRIMARY KEY,
        tweet_id BIGINT NOT NULL UNIQUE,
        ref VARCHAR(225),
        refid VARCHAR(225),
        author_id BIGINT NOT NULL,
        sampled BOOL,
        FOREIGN KEY (author_id) REFERENCES twitter_user(author_id)
        );"""
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating Tweet Table: {e}")
        connection.rollback()  # Roll back on error
    
def create_twitteruser_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE twitter_user (
        author_id BIGSERIAL PRIMARY KEY,
        username VARCHAR(50)
        );"""
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating Twitter User Table: {e}")
        connection.rollback()  # Roll back on error
    
def create_consplabels_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE labels_consp (
        id BIGSERIAL PRIMARY KEY,
        V1_bin BOOL NOT NULL,
        V1_prob DOUBLE PRECISION NOT NULL,
        V2_GR_bin BOOL NOT NULL,
        V2_GR_prob DOUBLE PRECISION NOT NULL,
        V2_NWO_bin BOOL NOT NULL,
        V2_NWO_prob DOUBLE PRECISION NOT NULL
        );"""
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating Conspiracy Labels Table: {e}")
        connection.rollback()  # Roll back on error
    
def create_liwclabels_table(cursor, connection):
    try:
        SQL_STATEMENT = """CREATE TABLE labels_liwc (
        id BIGSERIAL PRIMARY KEY,
        BigWords DOUBLE PRECISION NOT NULL,
        Segment INT NOT NULL,
        WC INT NOT NULL,
        allnone DOUBLE PRECISION NOT NULL,
        cause DOUBLE PRECISION NOT NULL,
        certitude DOUBLE PRECISION NOT NULL,
        cogproc DOUBLE PRECISION NOT NULL,
        differ DOUBLE PRECISION NOT NULL,
        discrep DOUBLE PRECISION NOT NULL,
        emo_anger DOUBLE PRECISION NOT NULL,
        emo_anx DOUBLE PRECISION NOT NULL,
        emo_neg DOUBLE PRECISION NOT NULL,
        emo_pos DOUBLE PRECISION NOT NULL,
        emo_sad DOUBLE PRECISION NOT NULL,
        emotion DOUBLE PRECISION NOT NULL,
        insight DOUBLE PRECISION NOT NULL,
        prep DOUBLE PRECISION NOT NULL,
        tentat DOUBLE PRECISION NOT NULL
        );
    """
        create_table(SQL_STATEMENT, cursor)
    except Exception as e:
        print(f"Error creating LIWC Labels Table: {e}")
        connection.rollback()  # Roll back on error
    