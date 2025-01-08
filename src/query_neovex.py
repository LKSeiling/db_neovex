from decouple import Config, RepositoryEnv
import psycopg2 as pg
from psycopg2 import sql
import pandas as pd
import warnings


warnings.simplefilter(action='ignore', category=UserWarning)



class NEOVEXQueryWrapper:
    def __init__(self, dbname, user, password, host, port=5432, 
    label_inclusion=None, label_exclusion=None, platform=None, subplatform=None,
    string_match=None, language=None, daterange=None, author=None):
        """
        Initialize the DatabaseWrapper with connection details.

        :param dbname: Name of the database
        :param user: Database user
        :param password: User's password
        :param host: Host of the database, default is 'localhost'
        :param port: Port of the database, default is '5432'
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
            'string_match': string_match,
            'language': language,
            'daterange': daterange,
            'author': author
        }

        self.set_platform(platform)
        self.set_subplatform(subplatform)

    def set_label_inclusion(self, labels):
        """
        Set labels for inclusion in the query.

        :param labels: List of labels to include
        """
        if isinstance(labels, str):
            labels = [labels]
        self.criteria['label_inclusion'] = labels

    def set_label_exclusion(self, labels):
        """
        Set labels for exclusion in the query.

        :param labels: List of labels to exclude
        """
        if isinstance(labels, str):
            labels = [labels]
        self.criteria['label_exclusion'] = labels

    def set_platform(self, platform):
        """
        Set the platform for the query.

        :param platform: Platform name
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

    def get_criteria(self):
        """
        Return currently specified criteria.

        :return: SQL query criteria.
        """
        print(self.criteria)

    def build_base_query(self):
        """
        Build the base SQL query based on the set criteria.

        :return: Constructed SQL query
        """
        query = sql.SQL(" WHERE 1=1")

        if self.criteria['label_inclusion']:
            if len(self.criteria['label_inclusion']) == 1:
                query += sql.SQL(" AND {} IS NOT NULL").format(sql.Identifier(f"label_{self.criteria['label_inclusion'][0]}"))
            else:
                for label in self.criteria['label_inclusion']:
                    column_name = f"label_{label}"
                    query += sql.SQL(" AND {} IS NOT NULL").format(sql.Identifier(column_name))
        if self.criteria['label_exclusion']:
            if len(self.criteria['label_exclusion']) == 1:
                query += sql.SQL(" AND {} IS NULL").format(sql.Identifier(f"label_{self.criteria['label_exclusion'][0]}"))
            else:
                for label in self.criteria['label_exclusion']:
                    column_name = f"label_{label}"
                    query += sql.SQL(" AND {} IS NULL").format(sql.Identifier(column_name))
        if self.criteria['platform']:
            if len(self.criteria['platform']) == 1:
                query += sql.SQL(" AND platform = {}").format(sql.Literal(self.criteria['platform'][0]))
            else:
                query += sql.SQL(" AND platform IN ({})").format(
                    sql.SQL(',').join(map(sql.Literal, self.criteria['platform']))
                )
        if self.criteria['subplatform']:
            if len(self.criteria['subplatform']) == 1:
                query += sql.SQL(" AND subplatform = {}").format(sql.Literal(self.criteria['subplatform'][0]))
            else:
                query += sql.SQL(" AND subplatform IN ({})").format(
                    sql.SQL(',').join(map(sql.Literal, self.criteria['subplatform']))
                )
        if self.criteria['string_match']:
            query += sql.SQL(" AND text ILIKE {}").format(sql.Literal(f"%{self.criteria['string_match']}%"))
        if self.criteria['language']:
            query += sql.SQL(" AND language = {}").format(sql.Literal(self.criteria['language']))
        if self.criteria['daterange']:
            start_date, end_date = self.criteria['daterange']
            query += sql.SQL(" AND date BETWEEN {} AND {}").format(sql.Literal(start_date), sql.Literal(end_date))
        if self.criteria['author']:
            query += sql.SQL(" AND author = {}").format(sql.Literal(self.criteria['author']))

        return query

    def check_query(self):
        """
        Checks if the query meets the minimal criteria to be valid and throws an assertion error otherwise.
        """

        if self.criteria['label_exclusion']:
            assert type(self.criteria['label_exclusion']) == list
            assert "liwc" in self.criteria['label_exclusion'] or "consp" in self.criteria['label_exclusion']

        if self.criteria['label_inclusion']:
            assert type(self.criteria['label_inclusion']) == list
            assert "liwc" in self.criteria['label_inclusion'] or "consp" in self.criteria['label_inclusion']

    def construct_query(self):
        """
        Construct the SQL query based on the set criteria.

        :return: Constructed SQL query
        """
        query = sql.SQL("SELECT * FROM content") + self.build_base_query()
        return query

    def query_db(self, sql_query):
        """
        Execute the constructed query and fetch all results.

        :return: List of query results
        """
        with pg.connect("host='{}' port={} dbname='{}' user={} password={}".format(self.conn_dat['host'], self.conn_dat['port'], self.conn_dat['dbname'], self.conn_dat['user'], self.conn_dat['password'])) as conn:
            dat = pd.read_sql_query(sql_query.as_string(conn), conn)
            return dat
    
    
    def execute_query(self):
        """
        Execute the constructed query and fetch all results.

        :return: List of query results
        """
        self.check_query()
        query = self.construct_query()
        return self.query_db(query)

    def sum_rows(self, group_by=None):
        """
        Execute the query and return the count of rows matching the criteria.
        Optionally, group the results by a specified column.

        :param group_by: Column name to group by (optional)
        :return: DataFrame of counts, optionally grouped by the specified column
        """
        if group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            
            group_by_clause = sql.SQL(', ').join(map(sql.Identifier, group_by))
            select_clause = sql.SQL(', ').join([group_by_clause, sql.SQL("COUNT(*)")])
            
            query = sql.SQL("SELECT {} FROM content").format(select_clause) + self.build_base_query()
            query += sql.SQL(" GROUP BY {}").format(group_by_clause)
        else:
            query = sql.SQL("SELECT COUNT(*) FROM content") + self.build_base_query()

        return self.query_db(query)

    def sum_per_time_unit(self, time_unit):
        """
        Execute the query and return the count of rows grouped by the specified time unit.

        :param time_unit: Time unit for aggregation (e.g., 'MONTH', 'DAY')
        :return: List of tuples containing the time unit and count of rows
        """
        query = self.construct_query().replace(sql.SQL("SELECT *"), sql.SQL(f"SELECT {time_unit}(date), COUNT(*)"))
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