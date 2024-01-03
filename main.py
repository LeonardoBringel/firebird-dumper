import fdb
import os
from dotenv import load_dotenv
from alive_progress import alive_bar
from decimal import Decimal
import datetime


load_dotenv()

DATABASE = os.environ.get('DATABASE')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')

try:
    con = fdb.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
    )
except Exception as e:
    print(f'(ERROR) Failed to establish connection with database:{DATABASE} - using user:{USER} and password:{PASSWORD}')
    exit(1)


def fetch_tables():
    try:
        cur = con.cursor()
        
        cur.execute('SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$VIEW_BLR IS NULL ORDER BY RDB$RELATION_NAME')
        result = cur.fetchall()

        tables = []
        for table in result:
            tables.append(table[0].strip())

        cur.close()
        return tables
    except Exception as e:
        print(f'(ERROR) Failed to fetch tables from database - {e}')


def fetch_table_description(table):
    try:
        # special thanks to Pablo T. for inspiring this query at https://stackoverflow.com/a/23303393
        query = f"""
        SELECT
        RF.RDB$FIELD_NAME FIELD_NAME,
        CASE F.RDB$FIELD_TYPE
            WHEN 7 THEN
            CASE F.RDB$FIELD_SUB_TYPE
                WHEN 0 THEN 'SMALLINT'
                WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                WHEN 2 THEN 'DECIMAL'
            END
            WHEN 8 THEN
            CASE F.RDB$FIELD_SUB_TYPE
                WHEN 0 THEN 'INTEGER'
                WHEN 1 THEN 'NUMERIC('  || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                WHEN 2 THEN 'DECIMAL'
            END
            WHEN 9 THEN 'QUAD'
            WHEN 10 THEN 'FLOAT'
            WHEN 12 THEN 'DATE'
            WHEN 13 THEN 'TIME'
            WHEN 14 THEN 'CHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ') '
            WHEN 16 THEN
            CASE F.RDB$FIELD_SUB_TYPE
                WHEN 0 THEN 'BIGINT'
                WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'
                WHEN 2 THEN 'DECIMAL'
            END
            WHEN 27 THEN 'DOUBLE'
            WHEN 35 THEN 'TIMESTAMP'
            WHEN 37 THEN
            IIF (COALESCE(f.RDB$COMPUTED_SOURCE,'')<>'',
            'COMPUTED BY ' || CAST(f.RDB$COMPUTED_SOURCE AS VARCHAR(250)),
            'VARCHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')')
            WHEN 40 THEN 'CSTRING' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')'
            WHEN 45 THEN 'BLOB_ID'
            WHEN 261 THEN 'BLOB SUB_TYPE ' || F.RDB$FIELD_SUB_TYPE
            ELSE 'RDB$FIELD_TYPE: ' || F.RDB$FIELD_TYPE || '?'
        END FIELD_TYPE,
        IIF(COALESCE(RF.RDB$NULL_FLAG, 0) = 0, '0', '1') FIELD_REQUIRED,
        COALESCE(RF.RDB$DEFAULT_SOURCE, F.RDB$DEFAULT_SOURCE) FIELD_DEFAULT
        FROM RDB$RELATION_FIELDS RF
        JOIN RDB$FIELDS F ON (F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE)
        LEFT OUTER JOIN RDB$CHARACTER_SETS CH ON (CH.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID)
        LEFT OUTER JOIN RDB$COLLATIONS DCO ON ((DCO.RDB$COLLATION_ID = F.RDB$COLLATION_ID) AND (DCO.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID))
        WHERE (COALESCE(RF.RDB$SYSTEM_FLAG, 0) = 0) AND (RF.RDB$RELATION_NAME = '{table}')
        ORDER BY RDB$RELATION_NAME, RF.RDB$FIELD_POSITION;
        """

        cur = con.cursor()

        cur.execute(query)
        result = cur.fetchall()

        cur.close()

        result = [(r[0].strip(), r[1], r[2], r[3]) for r in result]

        return result
    except Exception as e:
        print(f'(ERROR) Failed to describe table {table} - {e}')


def dump_table(table):
    try:
        cur = con.cursor()

        cur.execute(f'SELECT * FROM {table}')
        results = cur.fetchall()
        cur.close()
        if not results:
            print(f'(INFO) Skipping empty table {table}')
            return False

        for result in results:
            result = tuple(convert_element(value) for value in result) # convert data into an usefull format
            make_insert_statement(table, result)

        print(f'(INFO) Generated {len(results)} insert {"statements" if len(results) > 1 else "statement"} for {table} table')

        return True
    except Exception as e:
        print(f'(ERROR) Something whent wrong while dumping {table} table - {e}')


def make_create_table_statement(table):
    statement = f'CREATE TABLE `{table}` ('
    for column in fetch_table_description(table):
        statement += f'`{column[0]}` {column[1]} {"NOT NULL " if column[2] == 1 else ""}{f"{column[3]}" if column[3] is not None else "DEFAULT NULL"}, '
    statement = statement[:-2] # remove unnecessary ,
    statement += ');'
    print(f'(INFO) Generated statement to create {table} table')
    # print(statement) # TODO


def make_insert_statement(table, values):
    statement = f'INSERT INTO {table} VALUES ('
    for value in values:
        statement += f"""{f"'{value}'" if value != "NULL" else "NULL"}, """
    statement = statement[:-2] # remove unnecessary ,
    statement += ');'
    # print(statement) # TODO


def convert_element(value):
    if not value:
        return 'NULL'
    elif isinstance(value, Decimal):
        # Convert Decimal to float or int as needed
        return float(value)
    elif isinstance(value, datetime.datetime):
        # Format datetime as a string (or convert as needed)
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, str):
        # '\' is a special character in SQL, it should be replaced by '\\' in the INSERT statement to avoid errors
        return value.replace('\\', '\\\\')
    else:
        return value


tables = fetch_tables()
with alive_bar(len(tables)) as bar:
    bar.title('DUMPING DATABASE')
    for table in tables:
        if dump_table(table): # skip empty tables
            make_create_table_statement(table)
        bar()

con.close()
