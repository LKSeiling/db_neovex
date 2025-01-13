from decouple import Config, RepositoryEnv
import psycopg2 as pg
from psycopg2 import sql
import pandas as pd
import numpy as np
import warnings


warnings.simplefilter(action='ignore', category=UserWarning)



class NEOVEXQueryWrapper:
    def __init__(self, dbname, user, password, host, port=5432, 
    label_inclusion=None, label_exclusion=None, platform=None, subplatform=None,
    search_text = "all", string_match=None, case_sensitivity=False, language=None, 
    daterange=None, author=None, merge_platform_data=False, merge_label_data=False):
        """
        Initialize the DatabaseWrapper with connection details.

        :param dbname: Name of the database
        :param user: Database user
        :param password: User's password
        :param host: Host of the database, default is 'localhost'
        :param port: Port of the database, default is '5432'
        :param label_inclusion:
        :param label_exclusion:
        :param platform: 
        :param subplatform: 
        :param search_text: 
        :param string_match: 
        :param case_sensitivity: 
        :param language: 
        :param daterange: 
        :param author: 
        :param merge_platform_data: 
        :param merge_label_data:
        """
        self.conn_dat = {
            'dbname': dbname,
            'user' : user,
            'password' : password,
            'host' : host,
            'port' : port
        }

        self.criteria = {
            'label_inclusion': label_inclusion,
            'label_exclusion': label_exclusion,
            'platform': None,
            'subplatform': None,
            'search_text' : "all",
            'case_sensitivity': case_sensitivity,
            'string_match': string_match,
            'language': language,
            'daterange': daterange,
            'author': author,
            'merge_platform_data' : merge_platform_data,
            'merge_label_data' : merge_platform_data
        }

        self.set_platform(platform)
        self.set_subplatform(subplatform)

    def set_label_inclusion(self, labels):
        """
        Set labels for inclusion in the query. Currently only "liwc" or "consp" are accepted.

        :param labels: List of labels to include
        """
        if isinstance(labels, str):
            labels = [labels]
        self.criteria['label_inclusion'] = labels

    def set_label_exclusion(self, labels):
        """
        Set labels for exclusion in the query. Currently only "liwc" or "consp" are accepted.

        :param labels: List of labels to exclude
        """
        if isinstance(labels, str):
            labels = [labels]
        self.criteria['label_exclusion'] = labels

    def set_platform(self, platform):
        """
        Set the platform for the query. Currently only "alt_news" (alternative news), "legacy_news",
        "4chan", "reddit", and "twitter". Per default ("None") all platforms are included.

        :param platform: Platform name, default is "None"
        """
        if isinstance(platform, str):
            platform = [platform]
        self.criteria['platform'] = platform

    def set_subplatform(self, subplatform):
        """
        Set the subplatform for the query.

        :param subplatform: Subplatform name
        """
        self.criteria['subplatform'] = subplatform

    def set_search_text(self, search_text):
        """
        Set the search_text setting, which determines the columns which should be searched for a string match. 
        Currently only "all", "text" or "title" are accepted. 

        :param search_text: Search_text setting, default is "all"
        """
        self.criteria['search_text'] = search_text

    def set_case_sensitivity(self, case_sensitivity):
        """
        Set the case_sensitivity setting, which determines if the string match should be case sensitive. The default is False (not case-sensitive).

        :param case_sensitivity: Case_sensitivity setting
        """
        self.criteria['case_sensitivity'] = case_sensitivity

    def set_string_match(self, match_string):
        """
        Set the string match for the query.

        :param match_string: String to match in the text field
        """
        self.criteria['string_match'] = match_string

    def set_language(self, language):
        """
        Set the language for the query.

        :param language: Language code
        """
        self.criteria['language'] = language

    def set_daterange(self, start_date, end_date):
        """
        Set the date range for the query.

        :param start_date: Start date in 'YYYY-MM-DD' format
        :param end_date: End date in 'YYYY-MM-DD' format
        """
        self.criteria['daterange'] = (start_date, end_date)

    def set_author(self, author):
        """
        Set the author for the query.

        :param author: Author name
        """
        self.criteria['author'] = author
    
    def set_merge_platform_data(self, merge_bool):
        """
        Set the merge_platform_data setting. If true it will include additional platform data 
        from the platform-specific tables.

        :param merge_bool: merge_platform_data setting, default is False
        """
        self.criteria['merge_platform_data'] = merge_bool

    def set_merge_label_data(self, merge_bool):
        """
        Set the merge_label_data setting. If true it will include additional label data 
        from the label-specific tables.

        :param merge_bool: merge_label_data setting, default is False
        """
        self.criteria['merge_label_data'] = merge_bool

    def get_criteria(self):
        """
        Return currently specified criteria.

        :return: SQL query criteria.
        """
        print(self.criteria)


    def query_db(self, sql_query=None, str_query=None):
        """
        Execute a query and fetch all results.

        :param sql_query: Query constructed using the python sql package
        :param str_query: passed directly as string

        :return: Dataframe of query results
        """
        if sql_query:
            with pg.connect("host='{}' port={} dbname='{}' user={} password={}".format(self.conn_dat['host'], self.conn_dat['port'], self.conn_dat['dbname'], self.conn_dat['user'], self.conn_dat['password'])) as conn:
                dat = pd.read_sql_query(sql_query.as_string(conn), conn)
        elif str_query:
            with pg.connect("host='{}' port={} dbname='{}' user={} password={}".format(self.conn_dat['host'], self.conn_dat['port'], self.conn_dat['dbname'], self.conn_dat['user'], self.conn_dat['password'])) as conn:
                dat = pd.read_sql_query(str_query, conn)
        clean_dat = (
            dat.T
            .groupby(level=0)  # Group by original column names
            .agg(lambda group: group.dropna().iloc[0] if not group.dropna().empty else np.nan)  # Handle NaN values explicitly
            .T  # Transpose back to the original orientation
        )

        return pd.DataFrame(clean_dat)
    
    
    def execute_query(self):
        """
        Execute the constructed query and fetch all results.

        :return: Dataframe of query results
        """
        self.check_query()
        query = self.build_base_query()
        return self.query_db(sql_query=query)


    def check_query(self):
        """
        Checks if the query meets the minimal criteria to be valid and throws an assertion error otherwise.
        """

        if self.criteria['platform']:
            allowed_platforms = ["alt_news","legacy_news","4chan", "reddit","twitter"]
            if len(self.criteria['platform']) == 1:
                assert self.criteria['platform'][0] in allowed_platforms
            elif len(self.criteria['platform']) > 1:
                for platform in self.criteria['platform']:
                    assert platform in allowed_platforms

        if self.criteria['label_exclusion']:
            assert type(self.criteria['label_exclusion']) == list
            assert "liwc" in self.criteria['label_exclusion'] or "consp" in self.criteria['label_exclusion']

        if self.criteria['label_inclusion']:
            assert type(self.criteria['label_inclusion']) == list
            assert "liwc" in self.criteria['label_inclusion'] or "consp" in self.criteria['label_inclusion']
        
        if self.criteria['string_match']:
            assert self.criteria['search_text'] in ['all', 'text', 'title']

        assert type(self.criteria['case_sensitivity']) == bool

    def build_base_query(self, custom_select=None):
        """
        Build the base SQL query based on the set criteria, including platform-specific and label-specific information if required.

        :return: Constructed SQL query
        """
        join_clauses, selected_fields = self.add_platform_and_label_query()

        if custom_select:
            select_clause = custom_select
        else:
            select_clause = sql.SQL("SELECT {}").format(sql.SQL(", ").join(selected_fields))
            
        from_clause = sql.SQL("FROM content")

        where_clause = sql.SQL(" WHERE 1=1")

        if self.criteria['label_inclusion']:
            if len(self.criteria['label_inclusion']) == 1:
                where_clause += sql.SQL(" AND {} IS NOT NULL").format(sql.Identifier(f"label_{self.criteria['label_inclusion'][0]}"))
            else:
                for label in self.criteria['label_inclusion']:
                    column_name = f"label_{label}"
                    where_clause += sql.SQL(" AND {} IS NOT NULL").format(sql.Identifier(column_name))
        if self.criteria['label_exclusion']:
            if len(self.criteria['label_exclusion']) == 1:
                where_clause += sql.SQL(" AND {} IS NULL").format(sql.Identifier(f"label_{self.criteria['label_exclusion'][0]}"))
            else:
                for label in self.criteria['label_exclusion']:
                    column_name = f"label_{label}"
                    where_clause += sql.SQL(" AND {} IS NULL").format(sql.Identifier(column_name))
        if self.criteria['platform']:
            if len(self.criteria['platform']) == 1:
                where_clause += sql.SQL(" AND platform = {}").format(sql.Literal(self.criteria['platform'][0]))
            else:
                where_clause += sql.SQL(" AND platform IN ({})").format(
                    sql.SQL(',').join(map(sql.Literal, self.criteria['platform']))
                )
        if self.criteria['subplatform']:
            if len(self.criteria['subplatform']) == 1:
                where_clause += sql.SQL(" AND subplatform = {}").format(sql.Literal(self.criteria['subplatform'][0]))
            else:
                where_clause += sql.SQL(" AND subplatform IN ({})").format(
                    sql.SQL(',').join(map(sql.Literal, self.criteria['subplatform']))
                )
        if self.criteria['string_match']:
            string_match = self.criteria['string_match']
            match_operator = "LIKE" if self.criteria['case_sensitivity'] else "ILIKE"
            if self.criteria['search_text'] == "all":
                where_clause += sql.SQL(f" AND (text {match_operator} {{}} OR text_prep {match_operator} {{}} OR title {match_operator} {{}})").format(
                    sql.Literal(f"%{string_match}%"),
                    sql.Literal(f"%{string_match}%"),
                    sql.Literal(f"%{string_match}%")
                )
            elif self.criteria['search_text'] == "title":
                where_clause += sql.SQL(f" AND title {match_operator} {{}}").format(sql.Literal(f"%{string_match}%"))
            elif self.criteria['search_text'] == "text":
                where_clause += sql.SQL(f" AND (text {match_operator} {{}} OR text_prep {match_operator} {{}})").format(
                    sql.Literal(f"%{string_match}%"),
                    sql.Literal(f"%{string_match}%")
                )
        if self.criteria['language']:
            where_clause += sql.SQL(" AND language = {}").format(sql.Literal(self.criteria['language']))
        if self.criteria['daterange']:
            start_date, end_date = self.criteria['daterange']
            where_clause += sql.SQL(" AND date BETWEEN {} AND {}").format(sql.Literal(start_date), sql.Literal(end_date))

        where_clause = self.add_author_query(where_clause)

        full_query = select_clause + sql.SQL(" ") + from_clause
        if join_clauses:
            full_query += sql.SQL(" ").join(join_clauses)

        full_query += where_clause

        return full_query

    def add_platform_and_label_query(self):
        platform_table_mapping = {
            "alt_news": {"table": "alt_news", "fields": ["url", "author"]},
            "legacy_news": {"table": "legacy_news", "fields": ["meta", "terms", "author", "url", "section", "article_id"]},
            "4chan": {"table": "fourchan", "fields": ["media_link", "author", "nreplies", "num", "doc_id", "op", "poster_country", "referencing_comment", "searchterm", "subnum", "thread_id", "comments"]},
            "reddit": {"table": "reddit", "fields": ["author", "post_id", "link_id", "parent_id", "searchterm" ,"selftext", "terms", "type", "url"]},
            "twitter": {"table": "twitter", "fields": ["tweet_id", "ref", "refid", "author_id", "sampled"]}
        }

        label_table_mapping = {
            "consp": {"table": "labels_consp", "fields": ["V1_bin", "V1_prob", "V2_GR_bin", "V2_GR_prob", "V2_NWO_bin", "V2_NWO_prob"]},
            "liwc": {"table": "labels_liwc", "fields": ['bigwords', 'segment', 'wc', 'allnone', 'cause', 'certitude', 'cogproc', 
                   'differ', 'discrep', 'emo_anger', 'emo_anx', 'emo_neg', 'emo_pos', 
                   'emo_sad', 'emotion', 'insight', 'prep', 'tentat']}
        }

        selected_fields = [sql.SQL("content.*")]
        join_clauses = []

        if self.criteria['merge_platform_data']:
            if self.criteria['platform']:
                for platform in self.criteria['platform']:
                    platform = platform.lower()
                    if platform in platform_table_mapping:
                        table_info = platform_table_mapping[platform]
                        table_name = table_info["table"]
                        fields = table_info["fields"]

                        selected_fields.extend(
                            [sql.SQL("{}.{}").format(sql.Identifier(table_name), sql.Identifier(field)) for field in fields]
                        )

                        join_clauses.append(
                            sql.SQL(" LEFT JOIN {} ON {}.id = content.id").format(
                                sql.Identifier(table_name),
                                sql.Identifier(table_name)
                            )
                        )

                        if  self.criteria['platform']==None or 'twitter' in self.criteria['platform']:
                            join_clauses.append(
                                sql.SQL(" LEFT JOIN twitter_user tu ON {}.author_id = tu.author_id").format(
                                    sql.Identifier(table_name)
                                )
                            )

        if self.criteria['merge_label_data']:
            if self.criteria['label_inclusion']:
                for label in self.criteria['label_inclusion']:
                    label = label.lower()
                    if label in label_table_mapping:
                        table_info = label_table_mapping[label]
                        table_name = table_info["table"]
                        fields = table_info["fields"]

                        selected_fields.extend(
                            [sql.SQL("{}.{}").format(sql.Identifier(table_name), sql.Identifier(field)) for field in fields]
                        )

                        join_clauses.append(
                            sql.SQL(" LEFT JOIN {} ON {}.id = content.label_{}").format(
                                sql.Identifier(table_name),
                                sql.Identifier(table_name),
                                sql.SQL(label)
                            )
                        )

        return join_clauses, selected_fields
    
    def add_author_query(self, input_query):
        if self.criteria['author']:
            author_name = self.criteria['author']
            platform = self.criteria.get('platform', [])

            author_condition = sql.SQL(" AND (")
            subconditions = []

            if platform==None or 'twitter' in platform:
                subconditions.append(sql.SQL("""
                    (platform = 'twitter' AND EXISTS (
                        SELECT 1
                        FROM twitter t
                        JOIN twitter_user tu ON t.author_id = tu.author_id
                        WHERE t.id = content.content_id AND tu.username = {}
                    ))
                """).format(sql.Literal(author_name)))

            for p in ['alt_news', 'legacy_news', '4chan', 'reddit']:
                if platform==None or p in platform:
                    p_table = p if p!="4chan" else "fourchan"
                    subconditions.append(sql.SQL("""
                        (platform = {} AND EXISTS (
                            SELECT 1
                            FROM {} p
                            WHERE p.id = content.content_id AND p.author = {}
                        ))
                    """).format(
                        sql.Literal(p),
                        sql.Identifier(p_table),
                        sql.Literal(author_name)
                    ))

            if subconditions:
                author_condition += sql.SQL(" OR ").join(subconditions)
                author_condition += sql.SQL(")")
            query = input_query + author_condition
            return query
        else:
            return input_query

    def sum_rows(self, group_by=None):
        if group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            
            group_by_clause = sql.SQL(', ').join(map(sql.Identifier, group_by))
            custom_select = sql.SQL("SELECT {}, COUNT(*)").format(group_by_clause)
            query = self.build_base_query(custom_select=custom_select)
            query += sql.SQL(" GROUP BY {}").format(group_by_clause)
        else:
            custom_select = sql.SQL("SELECT COUNT(*)")
            query = self.build_base_query(custom_select=custom_select)
        
        return self.query_db(query)


    def sum_per_time_unit(self, time_unit):
        """
        Execute the query and return the count of rows grouped by the specified time unit.

        :param time_unit: Time unit for aggregation (e.g., 'MONTH', 'DAY')
        :return: List of tuples containing the time unit and count of rows
        """
        custom_select = sql.SQL(f"SELECT {time_unit}(date), COUNT(*)")
        query = self.build_base_query(custom_select=custom_select)
        query += sql.SQL(" GROUP BY {}(date)").format(sql.Identifier(time_unit))
        return self.query_db(query)

def get_config_dict(config_path='./.env'):
    """
    Read database config from config file and return information as python dict-

    :param config_path: Filepath of the .env file, default is './.env'
    :return: python dictionary with keys HOST, UNAME, PW, DB_NAME, and BASE_PATH
    """
    config_dict = {}
    config = Config(RepositoryEnv(config_path))
    config_dict['HOST'] = config.get('REMOTE_HOST')
    config_dict['UNAME'] = config.get('UNAME')
    config_dict['PW'] = config.get('PASSWORD')
    config_dict['DB_NAME'] = config.get('DB_NAME')
    return config_dict

def get_query_wrapper(label_inclusion=None, label_exclusion=None, platform=None, subplatform=None,
    string_match=None, language=None, daterange=None, author=None):
    """
    Initialize the NEOVEXQueryWrapper with provided criteria and return the instance.

    :param label_inclusion: Labels to include
    :param label_exclusion: Labels to exclude
    :param platform: Platform name(s)
    :param subplatform: Subplatform name
    :param string_match: String to match
    :param language: Language code
    :param daterange: Tuple of (start_date, end_date)
    :param author: Author name
    :return: Initialized NEOVEXQueryWrapper instance
    """
    config_dict = get_config_dict()
    query_wapper = NEOVEXQueryWrapper(host=config_dict['HOST'], dbname=config_dict['DB_NAME'], user=config_dict['UNAME'], password=config_dict['PW'],
    label_inclusion=label_inclusion, label_exclusion=label_exclusion, platform=platform, subplatform=subplatform,
    string_match=string_match, language=language, daterange=daterange, author=author)
    return query_wapper