import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object sessionize_web_log{
  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)    

    val conf = new SparkConf().setAppName("SessionizeWebLog")
    val sc = new SparkContext(conf)
	
    //the data is stored in "hdfs://localhost:8020/user/paytmlabs/2015_07_22_mktplace_shop_web_log_sample.log"
    val file_name = "hdfs://localhost:8020/user/paytmlabs/2015_07_22_mktplace_shop_web_log_sample.log"
    

    //A function to parse each log entry
    def parse_log_entry(log: String) = {
	/* The entry is delimeted by space,
         * except for the "request" and "user_agent" fields
         * which are enclosed by quotations and can contain spaces
         */

	//locate the request and user_agent fields
        val request_start = log.indexOf("\"")
        val request_end = log.indexOf("\"", request_start + 1)
        val user_agent_start = log.indexOf("\"", request_end + 1)
        val user_agent_end = log.indexOf("\"", user_agent_start +1)

	//extract all fifteen fields from each log entry
        val first_eleven_fields = log.substring(0, request_start).split("\\s+")
	val request = log.substring(request_start + 1, request_end)
        val user_agent = log.substring(user_agent_start + 1, user_agent_end)
        val last_two_fields = log.substring(user_agent_end + 1).trim.split("\\s+")

        //return the fields as a list
        List(first_eleven_fields(0),first_eleven_fields(1),first_eleven_fields(2),
             first_eleven_fields(3),first_eleven_fields(4),first_eleven_fields(5),
             first_eleven_fields(6),first_eleven_fields(7),first_eleven_fields(8),
	     first_eleven_fields(9),first_eleven_fields(10),request,user_agent,
             last_two_fields(0),last_two_fields(1))
    }

    //a function to extract the ip address
    def extract_Ip(client: String): String = {
	val ip_end = client.indexOf(":")
	client.substring(0,ip_end)
    }

    //a function to convert the timestamp into seconds since a reference time (Jan 1, 1870, 00:00:00 GMT)
    def convert_timestamp_into_seconds(timestamp: String): Long = {
	//locate the end of the date and time from the timestamp
        val date_end = timestamp.indexOf("T")
        val time_end = timestamp.indexOf(".") //truncates the decimal seconds
        //rebuild the date and time into the desired format
        val date_time = timestamp.substring(0,date_end)+"-"+timestamp.substring(date_end+1,time_end)
        //define the SimpleDateFormat pattern for parsing date_time
        val date_time_format = new java.text.SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
        //return a value representing seconds since the reference time
        val seconds_from_reference = date_time_format.parse(date_time).getTime()/1000 
	seconds_from_reference.toLong
    }

    //a function to extract the URL from the request
    def extract_Url(request: String): String = {
	//the request is composed of Http_method + " " + URL + " " + Http_version
	request.split("\\s+")(1)
    }

    //a function used for determining the start time of each session
    //the function accepts a key value pair where the key is the unique IP and the value is an iterable of (time_in_seconds, url)
    def determine_session_start(ip_time_Url: (String, Iterable[(Long, String)]), inactivity_threshold: Int) = {

	val ip = ip_time_Url._1

	//convert the Iterable into a List and sort the value based on the time
        val time_Url = ip_time_Url._2.toList.sortBy(_._1)
        //count the total number of entries for each ip
        val ctr = time_Url.length
        
        //the return list will have one addition field for the session start time
        var session_start_time = time_Url(0)._1
	var ip_time_startTime_Url: List[(String, Long, Long, String)] = List((ip, time_Url(0)._1, session_start_time, time_Url(0)._2))        

        //go through the list of log entries to add the session_start_time field
	var i = 0        
	for (i <- 1 to ctr - 1){
		//a new session_start_time is indicated when two successive entries are separated by over the inactivity threshold
		if(time_Url(i)._1 - time_Url(i-1)._1 > inactivity_threshold*60){
			session_start_time = time_Url(i)._1		
		}
		ip_time_startTime_Url = ip_time_startTime_Url ++ List((ip, time_Url(i)._1, session_start_time, time_Url(i)._2))
	}
	ip_time_startTime_Url
    } 

    //read the data from input file and 	
    val log_entries = sc.textFile(file_name)

    //parse the log entries using the function parse_log_entry
    val parsed_log_entries = log_entries.map(log => parse_log_entry(log))

    //for this challenge, we only need the fields: "timestamp", "client", and "request" for the timestamp in seconds, user IP, and URL
    val ip_time_Url = parsed_log_entries.map(log => (extract_Ip(log(2)), (convert_timestamp_into_seconds(log(0)), extract_Url(log(11)))))

    //determine the session start time for each log entry using the function determine_session_start
    //add the session start time as a new field to ip_time_Url
    val inactivity_threshold = 35
    val ip_time_session_Url = ip_time_Url.groupByKey.flatMap(log => determine_session_start(log, inactivity_threshold))

    //aggregate all page hits by the key (IP, session)
    val ip_session_Url = ip_time_session_Url.map(log => ((log._1, log._3), log._4)).groupByKey
    ip_session_Url.saveAsTextFile("hdfs://localhost:8020/user/paytmlabs/aggregated_page_hits_output")


    //find session duration (Note the session field is the start time of the session)
    val ip_session_duration = ip_time_session_Url.map(log => ((log._1, log._3), log._2 - log._3)).reduceByKey((x,y) => Math.max(x,y))    
    
    //find average session duration per user 
    val ip_session_duration_filtered = ip_session_duration.filter(log => log._2 > 0)  //filter out sessions with only 1 hit (i.e. not enough information to determine duration)
    val ip_avg_duration = ip_session_duration_filtered.map(log => (log._1._1, (log._2, 1) )).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(kv => (kv._1, kv._2._1.toDouble/kv._2._2/60))
    ip_avg_duration.saveAsTextFile("hdfs://localhost:8020/user/paytmlabs/average_duration_per_user_output")

    //find the average duration of all sessions
    val sum_count_duration = ip_session_duration_filtered.map(log => (log._2, 1)).reduce((x,y) => (x._1 + y._1, x._2 + y._2))
    val avg_duration = sum_count_duration._1.toDouble/sum_count_duration._2/60
    println("The average session time is: " + avg_duration + " min")

    //determine the number of unique URL hits for each session
    val ip_session_uniqueUrl = ip_time_session_Url.map(log => ((log._1, log._3), log._4)).groupByKey.map(log => (log._1, log._2.toSet.size))
    ip_session_uniqueUrl.saveAsTextFile("hdfs://localhost:8020/user/paytmlabs/count_unique_hits_output")

    //determine the user with the longest session duration
    val longest_session = ip_session_duration.reduce((x,y) => {if(x._2 >= y._2){x}
							   else{y}})
    val most_engaged_user=longest_session._1._1
    println("The IP address of the most engaged user is: " + most_engaged_user + " with session time of "+ longest_session._2/60 + " min")

    //Alternatively, find the top 3 most engaged users
    val ip_longest_duration = ip_session_duration.map(log => (log._1._1, log._2)).reduceByKey((x,y) => Math.max(x,y))
    val ip_duration_sorted = ip_longest_duration.map(log => (log._2, log._1)).sortByKey(false)
    println("The IP address of the top 3 most engaged users are: ")
    ip_duration_sorted.map(log => log._2 + " (" + log._1/60 + " min)").take(3).foreach(println) 
  }
}


