import org.apache.spark.sql._
import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.util.Locale
import scala.util.control.Exception._
import java.util.regex.Matcher
import scala.util.{Try, Success, Failure}

//Represents one Row of the ELB dataset
case class ElbRow(
                 time:String,
                 elb:String,
                 client_ip:String,
                 client_port:String,
                 backend_ip:String,
                 backend_port:String,
                 req_time:String,
                 backend_time:String,
                 response_time:String,
                 elb_status_code:String,
                 backend_status_code:String,
                 recv_bytes:String,
                 sent_bytes:String,
                 request: String,
                 ua:String,
                 ssl_cipher:String,
                 ssl_protocol:String


                 )

//A class to read each row of the log and convert it to a DataFrame
class ElbParser extends Serializable {

  def elbFilter(logLine: String): Boolean = {
    val LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$"
    val p = Pattern.compile(LOG_ENTRY_PATTERN)
    val matchLine = p.matcher(logLine)
    return matchLine.find()

  }
  def parseElb(logLine:String):Row= {
    //ELB pattern
    //Please note we get rid of the ports from client side and backend side ie
    //0.0.0.1:1234 becomes 0.0.0.1 and 1234
    val LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$"
    val p = Pattern.compile(LOG_ENTRY_PATTERN)
    val matchLine = p.matcher(logLine)
    matchLine.find()
    getElbRow(matchLine)
//    if(matchLine.find())
//      Some(getElbRow(matchLine))
//    else
//      None
  }
  //test method can be ignored
  def analyze2(logLine:String):Row={
    val LOG_ENTRY_PATTERN = "^(\\S+) (\\S+)$";
    val p = Pattern.compile(LOG_ENTRY_PATTERN)
    val matcher = p.matcher(logLine)
    matcher.find()
    getElbRow2(matcher)
  }
  //test method can be ignored
  def getElbRow2(matcher: Matcher):Row={
    return Row (
      matcher.group(1),
      matcher.group(2)
    )

  }

  def getElbRow(matcher: Matcher):Row={
    return Row(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9),
      matcher.group(10),
      matcher.group(11),
      matcher.group(12),
      matcher.group(13),
      matcher.group(14),
      matcher.group(15),
      matcher.group(16),
      matcher.group(17),
    )
  }
}
