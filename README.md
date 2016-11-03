# PaytmLabs_Weblog_Challenge
Sessionize web logs using Spark

The solution program will:

1. aggregate URLs of all page hits by distinct users (ie, IPs) for each session
2. determine the average session time
3. determine the number of unique URL visits per session
4. find the most engaged user (with the longest session time)

The solution also made the following assumptions:

1. The inactivity threshold (ie, time span between two hits to be considered as different sessions) is 15 minutes
2. The minimum session time is 1 minute (ie, if the user only visited 1 page, the time spent for that session is assumed to be 1 minute when calculating the average session time (2 above))

An example log entry has the following format:

*timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol*



(For the test data provided, the answers to 2 and 4 are 2.21 mins and 106.186.23.95 (with session time of 34 mins), respectively)
