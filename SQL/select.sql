-- select * FROM download_queue;


UPDATE download_queue SET status = 0, worker_id = NULL WHERE status=2 AND percent_done=0;

select * FROM download_queue;


