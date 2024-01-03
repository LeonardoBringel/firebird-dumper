import fdb
import os
from dotenv import load_dotenv


load_dotenv()

DATABASE = os.environ.get('DATABASE')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')

try:
    print(f'Connecting to {DATABASE} as user:{USER} password:{PASSWORD}')
    con = fdb.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
    )
except Exception as e:
    print('ERROR: Failed to establish database connection, try checking your env variables and try again.')
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
        print('ERROR: Failed to fetch tables from database - {e}')


def dump_table(table):
    try:
        cur = con.cursor()

        cur.execute(f'SELECT * FROM {table}')
        result = cur.fetchall()
        cur.close()
        if not result:
            print(f'WARNING: skipping empty table {table}')
            return
        # print(result)
    except Exception as e:
        print(f'ERROR: Failed to dump table {table} - {e}')


for table in fetch_tables():
    dump_table(table)
con.close()
