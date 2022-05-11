CREATE DATABASE youtube_dl;
CREATE USER youtube_dl_user WITH ENCRYPTED PASSWORD 'youtube_dl_pass';
GRANT ALL PRIVILEGES ON DATABASE youtube_dl TO youtube_dl_user;

