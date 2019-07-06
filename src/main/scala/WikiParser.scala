import java.io._

import org.apache.spark.rdd.RDD
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.matching.Regex
import scala.xml.{NodeSeq, XML}


case class Storage(key: String, value: List[String])

object WikiParser {

  val pattern: Regex = raw"\[\[(.+?)(\|.+)*\]\]".r // Any link
  val pat1: Regex = raw"\{\{.+\}\}".r // Magicstring
  val pat2: Regex = raw".*:.*".r // Namespaced
  implicit val defaultJsonProtocol: RootJsonFormat[Storage] = jsonFormat2(Storage)

  def main(argv: Array[String]): Unit = {
    val wikiname = "/user/ubuntu/wp/jvwiki"
    println("Working on: " + wikiname)
    val source = Utils.session.sparkContext.parallelize(Seq(Utils.openStream(wikiname + ".xml")))

    val pages: RDD[(NodeSeq, NodeSeq)] = for {
      file <- source
      wikiXML <- List(XML.load(file))
      page <- wikiXML \\ "page"
    } yield (page \ "title", page \\ "text")

    println("Pages found: " + pages.count())


    val totalLink: RDD[Storage] = for {
      page <- pages
      linkList <- List(pattern.findAllMatchIn(page._2.text).toList)
    } yield Storage(page._1.text, for (k <- linkList if passTest(k)) yield k.group(1))


    val output = Utils.createStream(wikiname + ".json")
    val objects =  totalLink.collect()
    output.writeChars(objects.toJson.toString)
    output.close()
    println("Saved.")
  }

  def passTest(value: Regex.Match): Boolean = {
    val argumentName: String = value.group(1)
    argumentName match {
      case pat1() | pat2() => false
      case _ => true
    }
  }
}
