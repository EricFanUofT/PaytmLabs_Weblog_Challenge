# PaytmLabs_Weblog_Challenge
Sessionize web logs using Spark

The solution program will:

1. aggregate URLs of all page hits by distinct users (ie, IPs) for each session
2. determine the average session time
3. determine the number of unique URL visits per session
4. find the most engaged users (with the longest session time)

The solution also made the following assumptions:

1. The inactivity threshold (ie, time span between two hits to be considered as different sessions) is 35 minutes.  This threshold is selected based on Fig. 1, which is a plot of frequency vs. time interval between successive page hits.

<p align="center">
  <img src="/images/interval length.png" >
</p>
*Fig. 1. Frequency vs. time interval between successive page hits.* 


2. To calculate the average session time, the sessions with only one page hit were ignored because there was not enough information to determine the length of time the user stayed on the page.  

An example log entry has the following format:

*timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol*



(For the test data provided, the answers to 2 and 4 are 3.68 mins and 220.226.206.7 (with session time of 54 mins), respectively)
