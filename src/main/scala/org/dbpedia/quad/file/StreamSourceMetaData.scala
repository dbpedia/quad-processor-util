package org.dbpedia.quad.file

import java.util.Date

import org.dbpedia.quad.utils.StringUtils

class StreamSourceMetaData (
  val dataset: String,
  val size: Int,
  val serialization: String,
  val date: Date = new Date(),
  val traits: Array[String] = Array()
) {
  override def toString: String = StreamSourceMetaData.unapply(this)
}

object StreamSourceMetaData{
  private val regex = """^\s*#?\s*dataset:\s*([a-zA-Z0-9-_]+)\s+size:\s*(\s+)\s+serialization:\s*([a-zA-Z0-9-]+)\s+started:\s*([^\s]+)\s+traits:\s*(.*)$""".r

  def apply(comment: String): Option[StreamSourceMetaData] =
    StreamSourceMetaData.regex.findFirstMatchIn(comment) match{
      case Some(m) =>
        val dataset = m.group(1)
        val size = m.group(2).toInt
        val serialization = m.group(3)
        val date = new Date(StringUtils.parseTimestamp(m.group(4)))
        val traits = m.group(5).split(",").map(_.trim)
        Some(new StreamSourceMetaData(dataset, size, serialization, date, traits))
      case None => None
    }

  def unapply(arg: StreamSourceMetaData): String = {
    val sb = new StringBuilder()
    sb.append("dataset:")
    sb.append(arg.dataset)
    sb.append("   ")
    sb.append("size:")
    sb.append(arg.size)
    sb.append("   ")
    sb.append("serialization:")
    sb.append(arg.serialization)
    sb.append("   ")
    sb.append("started:")
    sb.append(arg.date)
    sb.append("   ")
    sb.append("traits:")
    arg.traits.foreach(t => sb.append(t).append(","))
    sb.toString()
  }
}