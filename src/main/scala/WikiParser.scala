import java.io._

import org.apache.spark.rdd.RDD
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.matching.Regex
import scala.xml.XML


case class Storage(key: String, value: List[String])

object WikiParser {

  val pattern: Regex = raw"\[\[(.+?)(\|.+)*\]\]".r // Any link
  implicit val defaultJsonProtocol: RootJsonFormat[Storage] = jsonFormat2(Storage)

  def main(argv: Array[String]): Unit = {
    val wikiname = "/user/ubuntu/wp/furwiki"
    println("Working on: " + wikiname)
    val wikiXML = XML.load(Utils.openStream(wikiname + ".xml"))
    val pages = for {
      page <- wikiXML \\ "page"
    } yield ((page \ "title").text, (page \\ "text").text)

    println("Pages found: " + pages.length.toString)

    val pageRDD: RDD[(String, String)] = Utils.session.sparkContext.parallelize(pages)


    val totalLink: RDD[Storage] = for {
      page <- pageRDD
      linkList <- List(pattern.findAllMatchIn(page._2).toList)
    } yield Storage(page._1, for (k <- linkList if passTest(k)) yield k.group(1))


    println("Links found: " + totalLink.map(x => x.value.length).reduce((x, y) => x + y))

    val output = Utils.createStream(wikiname + ".json")
    val objects =  totalLink map (x => x.toJson.toString)
    output.writeChars("[" + totalLink.collect().mkString(", ") + "]")
    println("Saved.")
  }

  def passTest(value: Regex.Match): Boolean = {
    val argumentName: String = value.group(1)
    !((argumentName startsWith "{{") || (argumentName startsWith "[[")  || (argumentName contains  ":") )
  }
}
