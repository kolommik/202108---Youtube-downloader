
DROP TABLE IF EXISTS download_queue;

CREATE TABLE IF NOT EXISTS download_queue (
  id SERIAL PRIMARY KEY,
  url VARCHAR NOT NULL,
  path VARCHAR NOT NULL,
  filename VARCHAR NULL,
  status SMALLINT DEFAULT 0 NOT NULL,
  percent_done FLOAT DEFAULT 0 NOT NULL,
  worker_id INT NULL
  );

GRANT ALL ON ALL TABLES IN SCHEMA public TO youtube_dl_user;
