import java.io._

import org.apache.spark.rdd.RDD
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.matching.Regex
import scala.xml.XML


case class Storage(key: String, value: List[String])

object WikiParser {

  val pattern: Regex = raw"\[\[(.+?)(\|.+)*\]\]".r // Any link
  val pat1: Regex = raw"\{\{.+\}\}".r // Magicstring
  val pat2: Regex = raw".*:.*".r // Namespaced

  def main(argv: Array[String]): Unit = {
    val wikiname = "/user/ubuntu/wp/furwiki"
    println("Working on: " + wikiname)
    val wikiXML = XML.load(Utils.openStream(wikiname + ".xml"))
    val pages = for {
      page <- wikiXML \\ "page"
    } yield (page \ "title", page \\ "text")
    println("Pages found: " + pages.length.toString)

    val pageRDD = Utils.session.sparkContext.parallelize(pages)


    val totalLink: RDD[Storage] = for {
      page <- pageRDD
      linkList <- List(pattern.findAllMatchIn(page._2.text).toList)
    } yield Storage(page._1.text, for (k <- linkList if passTest(k)) yield k.group(1))


    println("Links found: " + totalLink.map(x => x.value.length).sum)

    implicit val defaultJsonProtocol: RootJsonFormat[Storage] = jsonFormat2(Storage)



    val output = Utils.createStream(wikiname + ".json")
    val objects: RDD[String] = totalLink map (x => x.toJson.toString)
    output.writeChars("[" + objects.collect().mkString(", ") + "]")
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
