# Youtube-downloader

Sample of using *youtube_dl* library for download youtube videos with several processes by using *multiprocessing*.
The list of youtube videos prepared in PostgresSQL with updatable statuses.

---
Used statuses for each job:  
* -1 - Error while downloading  
* 0 - New job  
* 1 - Job in Progress  
* 2 - Well done  
 
 
