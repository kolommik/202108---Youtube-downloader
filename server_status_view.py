"""Shows database all jobs statuses"""
import os
from contextlib import closing
from psycopg2 import connect
import pandas as pd

# --------------------------------------------------------
SQL_DBNAME = "youtube_dl"
SQL_USER = "youtube_dl_user"
SQL_PASSWORD = "youtube_dl_pass"
SQL_HOST = "127.0.0.1"
# --------------------------------------------------------
DISPLAY_STATUS_SQL = """
SELECT status, AVG(percent_done) AS percent, COUNT(*) 
FROM download_queue 
GROUP BY status 
ORDER BY status DESC;
"""
# --------------------------------------------------------


if __name__ == "__main__":
    with closing(
        connect(dbname=SQL_DBNAME, user=SQL_USER, password=SQL_PASSWORD, host=SQL_HOST)
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(DISPLAY_STATUS_SQL)
            rez = cursor.fetchall()

    print(pd.DataFrame(rez))

    os.system('pause')
    