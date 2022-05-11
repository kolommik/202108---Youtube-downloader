"""Клиент, работающий с БД"""

from typing import Tuple, Optional, Dict, Any
import os
from functools import partial
from multiprocessing import Process
from contextlib import closing
from psycopg2 import connect, DatabaseError
import numpy as np
import youtube_dl

# --------------------------------------------------------
SQL_DBNAME = "youtube_dl"
SQL_USER = "youtube_dl_user"
SQL_PASSWORD = "youtube_dl_pass"
SQL_HOST = "127.0.0.1"

NUM_WORKERS = 12
# --------------------------------------------------------
SQL_SELECT_ONE_ROW = """
SELECT id, url, path
FROM download_queue
WHERE status = 0
ORDER BY id
LIMIT 1
FOR UPDATE"""

SQL_UPDATE_ONE_ROW = """
UPDATE download_queue
SET status = 1, worker_id = %s
WHERE id = %s
"""

SQL_CLOSE_ONE_ROW = """
UPDATE download_queue
SET status = %s
WHERE id = %s
"""

SQL_UPDATE_FILENAME_ONE_ROW = """
UPDATE download_queue
SET filename = %s
WHERE id = %s
"""

SQL_UPDATE_PERCENT_ONE_ROW = """
UPDATE download_queue
SET percent_done = %s
WHERE id = %s
"""
# --------------------------------------------------------


def get_task(worker_id: int = 0) -> Optional[Tuple[int, str, str]]:
    """Returns job from PostgreSQL database queue table
    Parameters
    ----------
    worker_id: int
        worker id for taking job. Log into worker_id column table. 0 for Unknown

    Returns
    -------
    None if no job in queue
    or
    job_id: int
        id (key) of job in queue
    url: str
        source url
    path: str
        destination path, where we save data
    """
    with closing(
        connect(dbname=SQL_DBNAME, user=SQL_USER, password=SQL_PASSWORD, host=SQL_HOST)
    ) as conn:
        with conn.cursor() as cursor:
            try:
                # read job
                cursor.execute(SQL_SELECT_ONE_ROW)
                rez = cursor.fetchone()

                if rez is None:
                    return None
                else:
                    job_id, url, path = rez
                    # update job status
                    cursor.execute(SQL_UPDATE_ONE_ROW, (str(worker_id), str(job_id)))
                    # updated_rows = cursor.rowcount
                    conn.commit()
            except DatabaseError as error:
                print(error)
                conn.rollback()
                return None
    return job_id, url, path


def close_task(job_id: int, status: int = 2) -> None:
    """Closes job at PostgreSQL database queue table
    Parameters
    ----------
    job_id: int
        id (key) of job in queue
    status: int
        status for job (0 - not started, 1 - in progress, 2 - done, 3 - error)

    Returns
    -------
    None
    """
    with closing(
        connect(dbname=SQL_DBNAME, user=SQL_USER, password=SQL_PASSWORD, host=SQL_HOST)
    ) as conn:
        with conn.cursor() as cursor:
            try:
                # update job status
                cursor.execute(SQL_CLOSE_ONE_ROW, (str(status), str(job_id)))
                # updated_rows = cursor.rowcount
                conn.commit()
            except DatabaseError as error:
                print("Error close_task error: ",error)
                conn.rollback()


def update_task_percent(job_id: int, percent: int = 0) -> None:
    """Updates job percent at PostgreSQL database queue table
    Parameters
    ----------
    job_id: int
        id (key) of job in queue
    percent: int
        percent of done for job

    Returns
    -------
    None
    """
    with closing(
        connect(dbname=SQL_DBNAME, user=SQL_USER, password=SQL_PASSWORD, host=SQL_HOST)
    ) as conn:
        with conn.cursor() as cursor:
            try:
                # update job status
                cursor.execute(SQL_UPDATE_PERCENT_ONE_ROW, (str(percent), str(job_id)))
                # updated_rows = cursor.rowcount
                conn.commit()
            except DatabaseError as error:
                print("Error update_task_percent:", error)
                conn.rollback()


def update_task_filename(job_id: int, filename: str) -> None:
    """Updates job filename at PostgreSQL database queue table
    Parameters
    ----------
    job_id: int
        id (key) of job in queue
    filename: str
        filename for job

    Returns
    -------
    None
    """
    with closing(
        connect(dbname=SQL_DBNAME, user=SQL_USER, password=SQL_PASSWORD, host=SQL_HOST)
    ) as conn:
        with conn.cursor() as cursor:
            try:
                # update job status
                cursor.execute(
                    SQL_UPDATE_FILENAME_ONE_ROW, (str(filename), str(job_id))
                )
                # updated_rows = cursor.rowcount
                conn.commit()
            except DatabaseError as error:
                print("Error update_task_filename:", error)
                conn.rollback()


def update_status(item: Dict[Any, Any], job_id: int) -> None:
    """Updates status of job
    Parameters
    ----------
    item:  Dict[Any, Any]
       item from youtube_dl task
    job_id: int
        id (key) of job in queue

    Returns
    -------
    None
    """
    # print(item)
    # {
    # 'status': 'downloading',
    # 'downloaded_bytes': 441858,
    # 'total_bytes': 73196583,
    # 'tmpfilename': 'D:\\__1\\3 Урок - ранжирования.f137.mp4.part',
    # 'filename': 'D:\\__1\\3 Урок - ранжирования.f137.mp4',
    # 'eta': 919,
    # 'speed': 79154.73166264818,
    # 'elapsed': 5.979184627532959,
    # '_eta_str': '15:19',
    # '_percent_str': '  0.6%',
    # '_speed_str': '77.30KiB/s',
    # '_total_bytes_str': '69.81MiB'
    # }
    percent = np.round(item["downloaded_bytes"] / item["total_bytes"], 4) * 100
    update_task_percent(job_id=job_id, percent=percent)


def worker() -> None:
    """Worker. Get task from SQL. Process it. Close after all.
    Parameters
    ----------

    Returns
    -------
    None
    """
    worker_id = os.getpid()
    print(f"{worker_id} Start.")

    while True:
        # start job ===========================================================
        job_task = get_task(worker_id=worker_id)
        if job_task is None:
            print(f"{worker_id} Done.")
            return None
        else:
            job_id, url, path = job_task

        print(f"{worker_id} starting: {job_id} || {path}, {url}")

        # get info ============================================================
        # video_format = "bestaudio/bestvideo"
        video_format = "22[height=720]/17[height=720]/18[height=720]"
        output_filename = f"{path}/%(title)s.%(ext)s"

        ydl_opts = {
            "format": video_format,
            "outtmpl": output_filename,
            "noplaylist": True,
            "quiet": True,
            # "listformats": True,
        }
        try:
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                filename = ydl.prepare_filename(info)

                update_task_filename(job_id=job_id, filename=filename)
        except Exception as error:
            print("Exception in getting filename", error)
            status = -1
            close_task(
                job_id=job_id,
                status=status,
            )
            break

        print(f"{worker_id} {job_id} saving to: {filename}")

        # process =============================================================

        update_job_status = partial(update_status, job_id=job_id)

        ydl_opts = {
            "format": video_format,
            "outtmpl": output_filename,
            "noplaylist": True,
            "quiet": True,
            # "no_warnings": True,
            "progress_hooks": [update_job_status],
        }

        # job started
        status = 1

        try:
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                # print(dir(ydl))
                ydl.download([url])
                # info = ydl.extract_info(url, download=True)
            # jobe well done
            status = 2

        except Exception as error:
            print("Exception in job", error)
            # jobe fail with error
            status = -1

        # close job ===========================================================
        close_task(
            job_id=job_id,
            status=status,
        )


if __name__ == "__main__":

    procs = []
    for i in range(NUM_WORKERS):
        proc = Process(
            target=worker,
            args=(),
            daemon=True,
        )
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()
