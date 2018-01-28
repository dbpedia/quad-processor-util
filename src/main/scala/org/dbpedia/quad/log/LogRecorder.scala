package org.dbpedia.quad.log

import java.io.Writer
import java.text.DecimalFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{FileLike, IOUtils}
import org.dbpedia.quad.utils.StringUtils

import scala.collection.mutable

/**
  * Created by Chile on 11/3/2016.
  */
class LogRecorder[T](
   val logWriter: Writer = null,
   val reportInterval: Int = 100000,
   val preamble: String = null
 ) {

  def this(er: LogRecorder[T]) = this(er.logWriter, er.reportInterval, er.preamble)

  private var failedPageMap = Map[String, scala.collection.mutable.Map[(String, T), Throwable]]()
  private var successfulPagesMap = Map[String, scala.collection.mutable.Map[String, String]]()

  private val startTime = new AtomicLong()
  private var successfulPageCount = Map[String,AtomicLong]()

  private var defaulttag: String = "en"

  private val decForm = new DecimalFormat("#.##")

  private var datasets: Seq[String] = Seq()
  private var task: String = "transformation"

  private var writerOpen = if(logWriter == null) false else true
  private var initialized = false

  /**
    * A map for failed pages, which could be used for a better way to record extraction fails than just a simple console output.
    *
    * @return the failed pages (id, title) for every tag
    */
  def listFailedPages: Map[String, mutable.Map[(String, T), Throwable]] = failedPageMap

  /**
    * successful page count
    *
    * @param tag - for this tag
    * @return
    */
  def successfulPages(tag: String): Long = successfulPageCount.get(tag) match{
    case Some(m) => m.get()
    case None => 0
  }

  /**
    * get successful page count after increasing it by one
    *
    * @param tag - for this tag
    * @return
    */
  def increaseAndGetSuccessfulPages(tag: String): Long ={
    successfulPageCount.get(tag) match {
      case Some(ai) => ai.incrementAndGet()
      case None => {
        successfulPageCount += (tag -> new AtomicLong(1))
        1
      }
    }
  }

  /**
    * number of failed pages
    *
    * @param tag - for this tag
    * @return
    */
  def failedPages(tag: String): Long = failedPageMap.get(tag) match{
    case Some(m) => m.size
    case None => 0
  }

  /**
    * the current accumulated page number
    *
    * @param tag - for this tag
    * @return
    */
  def runningPageNumber(tag:String) = successfulPages(tag) + failedPages(tag)

  /**
    * prints a message of a RecordEntry if available and
    * assesses a RecordEntry for the existence of a Throwable and forwards
    * the record to the suitable method for a failed or successful extraction
    *
    * @param records - the RecordEntries for a WikiPage
    */
  def record(records: RecordEntry[T]*): Unit = {
    for(record <- records) {
      //val count = increaseAndGetSuccessfulPages(record.tag)
      record.page match{
        case quad: Quad =>{
          Option(record.error) match {
            case Some(ex) => failedRecord(quad.subject, runningPageNumber(record.tag).toString, record.page, ex, record.tag)
            case None => recordQuad(quad, record.severity, record.tag)
          }
        }
        case _  => {
          if (record.errorMsg != null)
            printLabeledLine(record.errorMsg, record.severity, record.tag, Seq(PrinterDestination.err, PrinterDestination.file))
          Option(record.error) match {
            case Some(ex) => failedRecord(record.title, record.id.toString, record.page, ex, record.tag)
            case None => recordExtractedRecord(record, record.logSuccessfulPage)
          }
          val msg = Option(record.errorMsg) match{
            case Some(m) => m
            case None => {
              if(record.error != null) record.error.getMessage
              else "an undefined error occurred at quad: " + successfulPages(record.tag)
            }
          }
          printLabeledLine(msg, record.severity, record.tag, null)
        }
      }
    }
  }

  /**
    * adds a new fail record for a wikipage which failed to extract; Optional: write fail to log file (if this has been set before)
    *
    * @param id - page id
    * @param node - PageNode of page
    * @param exception  - the Throwable responsible for the fail
    */
  def failedRecord(name: String, id: String, node: T, exception: Throwable, tag:String = null): Unit = synchronized{
    val tagi = if(tag != null) tag else defaulttag
    val instanceName = node match{
      case q: Quad => "quad"
      case _ => "instance"
    }
    failedPageMap.get(tagi) match{
      case Some(map) => map += ((id,node) -> exception)
      case None =>  failedPageMap += tagi -> mutable.Map[(String, T), Throwable]((id, node) -> exception)
    }
    printLabeledLine("{task} failed for " + instanceName + " " + id + ": " + name + ": " + exception.getMessage(), RecordSeverity.Exception, tagi, Seq(PrinterDestination.err, PrinterDestination.file))
    for (ste <- exception.getStackTrace)
      printLabeledLine("\t" + ste.toString, RecordSeverity.Exception, tagi, Seq(PrinterDestination.file), noLabel = true)
  }

  /**
    * adds a record of a successfully extracted page
    *
    * @param record - RecordEntry
    * @param logSuccessfulPage - indicates whether the event of a successful extraction shall be included in the log file (default = false)
    */
  def recordExtractedRecord(record: RecordEntry[T], logSuccessfulPage:Boolean = false): Unit = synchronized {
    if(logSuccessfulPage) {
      successfulPagesMap.get(record.tag) match {
        case Some(map) => map += (record.id -> record.title)
        case None => successfulPagesMap += record.tag -> mutable.Map[String, String](record.id.toString -> record.title)
      }
      printLabeledLine("record " + record.id + ": " + record.title + " successful", RecordSeverity.Info, record.tag, Seq(PrinterDestination.file))
    }
    val pages = increaseAndGetSuccessfulPages(record.tag)
    if(pages % reportInterval == 0)
      printLabeledLine("{page} records; {mspp} per page; {fail} failed record", RecordSeverity.Info, record.tag)
  }

  /**
    * record (successful) quad
    *
    * @param quad
    * @param tag
    */
  def recordQuad(quad: Quad, severity: RecordSeverity.Value, tag:String): Unit = synchronized {
    if(increaseAndGetSuccessfulPages(tag) % reportInterval == 0)
      printLabeledLine("processed {page} quads; {mspp} per quad; {fail} failed quads", severity, tag)
  }

    /**
    * print a line to std out, err or the log file
    *
    * @param line - the line in question
    * @param tag - tag of current page
    * @param print - enum values for printer destinations (err, out, file - null mean all of them)
    * @param noLabel - the initial label (tag: time passed) is omitted
    */
  def printLabeledLine(line:String, severity: RecordSeverity.Value, tag: String = null, print: Seq[PrinterDestination.Value] = null, noLabel: Boolean = false): Unit ={
    val tagi = if(tag != null) tag else defaulttag
    val printOptions = if(print == null) {
      if(severity == RecordSeverity.Exception || severity == RecordSeverity.Warning )
        Seq(PrinterDestination.err, PrinterDestination.out, PrinterDestination.file)
      else
        Seq(PrinterDestination.out, PrinterDestination.file)
    } else print

    val status = getStatusValues(tagi)
    val replacedLine = (if (noLabel) "" else severity.toString + "; " + tagi + "; {task} at {time} for {data}; ") + line
    val pattern = "\\{\\s*\\w+\\s*\\}".r
    var lastend = 0
    var resultString = ""
    for(matchh <- pattern.findAllMatchIn(replacedLine)){
      resultString += replacedLine.substring(lastend, matchh.start)
      resultString += (Option(matchh.matched) match{
        case Some(m) =>
          m match{
            case i if i == "{time}" => status("time")
            case i if i == "{mspp}" => status("mspp")
            case i if i == "{page}" => status("pages")
            case i if i == "{erate}" => status("erate")
            case i if i == "{fail}" => status("failed")
            case i if i == "{data}" => status("dataset")
            case i if i == "{task}" => status("task")
            case _ => ""
          }
        case None => ""
      })
      lastend = matchh.end
    }
    resultString += replacedLine.substring(lastend)

    for(pr <-printOptions)
      pr match{
        case PrinterDestination.err => System.err.println(resultString)
        case PrinterDestination.out => System.out.println(resultString)
        case PrinterDestination.file if writerOpen => logWriter.append(resultString + "\n")
        case _ =>
      }
  }

  def getStatusValues(tag: String): Map[String, String] = {
    val pages = successfulPages(tag)
    val time = System.currentTimeMillis - startTime.get
    val failed = failedPages(tag)
    val datasetss = if(datasets.nonEmpty && datasets.size <= 3)
      datasets.foldLeft[String]("")((x,y) => x + ", " + y).substring(2)
    else
      String.valueOf(datasets.size) + " datasets"

    Map("pages" -> pages.toString,
      "failed" -> failed.toString,
      "mspp" -> (decForm.format(time.toDouble / pages) + " ms"),
      "erate" -> (if(failed == 0) "0" else ((pages+failed) / failed).toString),
      "dataset" -> datasetss,
      "time" -> StringUtils.prettyMillis(time),
      "task" -> this.task
    )
  }

  def initialize(tag: String, task: String = "transformation", datasets: Seq[String] = Seq()): Unit ={
    failedPageMap = Map[String, scala.collection.mutable.Map[(String, T), Throwable]]()
    successfulPagesMap = Map[String, scala.collection.mutable.Map[String, String]]()
    successfulPageCount = Map[String,AtomicLong]()

    startTime.set(System.currentTimeMillis)
    defaulttag = tag
    this.datasets = datasets
    this.task = task

    if(preamble != null)
      printLabeledLine(preamble, RecordSeverity.Info, tag)
  }

  override def finalize(): Unit ={
    if(writerOpen){
      logWriter.close()
      writerOpen = false
    }

    val line = "Extraction finished for tag: " + defaulttag + " (" + defaulttag + ") " +
      (if(datasets.nonEmpty) ", extracted " + successfulPages(defaulttag) + " records for " + datasets.size + " datasets after " + StringUtils.prettyMillis(System.currentTimeMillis - startTime.get) + " minutes." else "")
    printLabeledLine(line, RecordSeverity.Info, defaulttag)

    super.finalize()
  }

  def resetFailedPages(tag: String): Unit = failedPageMap.get(tag) match{
    case Some(m) => {
      m.clear()
      successfulPageCount(tag).set(0)
    }
    case None =>
  }

  def resetSuccessfulPages(): Unit ={
    successfulPagesMap = Map[String, scala.collection.mutable.Map[String, String]]()
    successfulPageCount = Map[String,AtomicLong]()
  }

  def setTask(task: String): Unit = this.task = task

  object PrinterDestination extends Enumeration {
    val out, err, file = Value
  }

  /**
    * the following methods will post messages to a Slack webhook if the Slack-Cedentials are available in the config file
    */
  var lastExceptionMsg = new Date().getTime
}

object LogRecorder{
  def create[T](log: FileLike[_] = null, reportInterval: Int = 100000, preamble: String = null): LogRecorder[T] = Option(log) match{
    case Some(f) => new LogRecorder[T](IOUtils.writer(f, append = true), reportInterval, preamble)
    case None => new LogRecorder[T](null, reportInterval, preamble)
  }
}

/**
  * This class provides the necessary attributes to record either a successful or failed extraction
  *
  * @param page
  * @param tag
  * @param errorMsg
  * @param error
  * @param logSuccessfulPage
  */
class RecordEntry[T](
  val id : String,
  val title: String,
  val page: T,
  val severity: RecordSeverity.Value,
  val tag: String,
  val errorMsg: String= null,
  val error:Throwable = null,
  val logSuccessfulPage:Boolean = false
)

object RecordSeverity extends Enumeration {
  val Info, Warning, Exception = Value
}