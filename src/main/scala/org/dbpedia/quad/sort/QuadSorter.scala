package org.dbpedia.quad.sort

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, Paths}
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import org.dbpedia.quad.Quad
import org.dbpedia.quad.config.Config
import org.dbpedia.quad.destination.{Destination, FileDestination}
import org.dbpedia.quad.file.{FileLike, IOUtils, RichFile}
import org.dbpedia.quad.log.{LogRecorder, RecordSeverity}
import org.dbpedia.quad.processing.{PromisedWork, QuadReader}
import org.dbpedia.quad.sort.QuadSorter.{MergeResult, PrefixRecord}
import org.dbpedia.quad.utils.{FilterTarget, StringUtils}

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.util.{Failure, Success}


/**
  * Created by chile on 14.06.17.
  */
class QuadSorter(val target: FilterTarget.Value, val config: Config = Config.UniversalConfig) {

  if(!config.dumpDir.exists)
    throw new FileNotFoundException("The provided base directory does not exist.")
  if(!config.dumpDir.getFile.canWrite)
    throw new FileNotFoundException("The provided base directory does not allow write access.")

  private val buffer: ListBuffer[Promise[List[MergeResult]]] = new ListBuffer[Promise[List[MergeResult]]]()
  private val segmentMap = new mutable.HashMap[Int, RichFile]()
  private var numberOfQuads = 0
  private var numberOfSegments = 0
  private val tempFolder: File = Paths.get(config.dumpDir.getFile.getAbsolutePath, "tempsort").toFile
  private val buckedMap: concurrent.Map[Int, ListBuffer[Quad]] = new ConcurrentHashMap[Int, ListBuffer[Quad]]().asScala
  private val prefixMap = new PrefixMap()
  private var startTime = System.currentTimeMillis()
  private var recorder: LogRecorder[Quad] = new LogRecorder[Quad]()
  private val codePointComp = new CodePointComparator()


  /**
    * This worker does the initial compare of the most atomic segments (~100 Quads) using a simple sortWith
    */
  private val initialSorter = PromisedWork[List[Quad], List[MergeResult]](1.5, 1.5){ quads: List[Quad] =>

    val comp = new QuadComparator(target)
    val sorted = quads.sortWith((quad1, quad2) => {comp.compare(quad1, quad2) < 0})
    val ret = if(sorted.nonEmpty)
      for(m <- surveyPrefixes(sorted)) yield MergeResult(m._2, m._1)
    else
      List(MergeResult(new ListBuffer[Quad](), null))
    ret.toList
  }

  private def surveyPrefixes(sorted: List[Quad]): Map[String, ListBuffer[Quad]] = synchronized{
    var map = Map[String, ListBuffer[Quad]]()
    val comp = new CodePointComparator()

    for(i <- 1 until sorted.size){
      val resourcea = FilterTarget.resolveQuadResource(sorted(i-1), target)
      val resourceb = FilterTarget.resolveQuadResource(sorted(i), target)
      var prefix = StringUtils.getLongestPrefix(resourcea, resourceb)
      val pathLength = Math.max(resourcea.lastIndexOf("/"), resourcea.lastIndexOf("#"))+1
      if(pathLength > 0 && prefix.length > pathLength)
        prefix = prefix.substring(0, pathLength)
      if(prefix.length == resourcea.length || prefix.length == resourceb.length )
        prefix = prefix.substring(0, prefix.length-1)
      prefix = prefix.charAt(prefix.length-1) match{
        case '/' | '#' => prefix
        case _ => prefix.substring(0, Math.max(prefix.lastIndexOf("/"), prefix.lastIndexOf("#"))+1)
      }
      map.get(prefix) match {
        case Some(l) =>
          if (i == sorted.size - 1)
            l.append(sorted(i - 1), sorted(i))
          else
            l.append(sorted(i - 1))
        case None =>
          val zw = new ListBuffer[Quad]()
          if (i == sorted.size - 1)
            zw.append(sorted(i - 1), sorted(i))
          else
            zw.append(sorted(i - 1))
          map += prefix -> zw
      }
    }
    for(m <- map.filter(x => x._2.nonEmpty)) {
      val charMap: mutable.Map[Char, Int] = extractCharMap(m._1, m._2)
      prefixMap.addPrefix(m._1, charMap)
    }
    map
  }

  private def extractCharMap(prefix: String, quads: ListBuffer[Quad]) = {
    val charMap = mutable.Map[Char, Int]()
    for (quad <- quads) {
      val resource = FilterTarget.resolveQuadResource(quad, target)
      if(resource.length <= prefix.length)
        throw new IllegalStateException("A non-prefix was selected!")
      val char = resource.substring(prefix.length).toUpperCase.head
      charMap.get(char) match {
        case Some(c) => charMap.put(char, c + 1)
        case None => charMap.put(char, 1)
      }
    }
    charMap
  }

  private def evaluatePrefixes(): Unit = {
    val prefixes = prefixMap.keySet.toList.sortWith((p1, p2) => {codePointComp.compare(p1, p2) < 0})
    val average = prefixMap.values.map(x => x.count).sum.toDouble / prefixMap.size.toDouble

    var fixPrefix = true
    var fixedPrefix: PrefixRecord = null
    for(i <- prefixes.indices) {
      if(!fixPrefix) {
        if (StringUtils.getLongestPrefix(fixedPrefix.prefix, prefixes(i)) == fixedPrefix.prefix) {
          if (fixedPrefix.count > average * 2d) {
            val firstChar = prefixes(i).substring(fixedPrefix.prefix.length, fixedPrefix.prefix.length + 1)
            fixedPrefix.charMap.get(firstChar.toUpperCase.head) match {
              case Some(c) => fixedPrefix.charMap.put(firstChar.toUpperCase.head, c + prefixMap(prefixes(i)).charMap.values.sum)
              case None => fixedPrefix.charMap.put(firstChar.toUpperCase.head, prefixMap(prefixes(i)).charMap.values.sum)
            }
            //redirect unused prefixes
            prefixMap += prefixes(i) -> new PrefixRecord(prefixes(i), prefixMap(prefixes(i)).index, mutable.Map[Char, Int](), Some(fixedPrefix.prefix))
          }
          fixPrefix = false
        }
        else
          fixPrefix = true
      }
      else{
        fixedPrefix = prefixMap(prefixes(i))
        fixPrefix = false
      }
    }

    while(prefixMap.nonEmpty && prefixMap.count(x => x._2.count > 0) < PromisedWork.defaultThreads){
      val largest = prefixMap.maxBy(x => x._2.count)._2
      for(chr <- largest.charMap)
        prefixMap.addPrefix(largest.prefix + chr._1.toUpper, mutable.Map[Char, Int](chr._1 -> chr._2))

      //empty largest and toggle split indicator
      prefixMap += largest.prefix -> new PrefixRecord(largest.prefix, largest.index, mutable.Map[Char, Int](), None, split = true)

    }
  }

  /**
    * The merge method of two or more already sorted segments (using a quad-sink to digest the results)
    * @param mergees - a list of MergeResults - the already sorted segments to merge
    * @param sink - the Quad sink to which to forward the quads
    * @return - the longest common prefix of all instances detected (if ignorePrefix != null it is equal to ignorePrefix)
    */
  def mergeQuads (mergees: List[MergeResult], sink: (Quad) => Unit) : String = {
    val comp = new QuadComparator(target, prefixMap, mergees.head.longestPrefix)
    var treeMap :List[(Quad, List[Quad])] = List()
    for (i <- mergees.indices)
      if(mergees(i).quads.nonEmpty)
        treeMap = treeMap ::: List((mergees(i).quads.head, mergees(i).quads.toList.tail))

    treeMap = treeMap.sortWith((x,y) => comp.compare(x._1, y._1) < 0)

    while (treeMap.nonEmpty) {
      val head = treeMap.head
      head._2.headOption match{
        case Some(q) =>
          val spans = treeMap.tail.span(x => comp.compare(q, x._1) > 0)
          treeMap = spans._1 ::: List((q, head._2.tail)) ::: spans._2
        case None => treeMap = treeMap.tail
      }

      sink(head._1)
    }
    comp.getCommonPrefix
  }

  /**
    * This is a simple worker calling the merge method and storing the results in a list
    */
  private def mergeWorker() =
    PromisedWork[List[MergeResult], List[MergeResult]](1.5, 1.5) { mergees: List[MergeResult] =>
/*      val test = if(mergees.isEmpty) null
        else mergees.reduceLeft[MergeResult]((x,y) => if(x != null && x.longestPrefix == y.longestPrefix) y else null)
      if(test == null)
        throw new IllegalArgumentException("Attempt to merge bins with different prefixes.")*/
      //execute merger and collect the longest prefix
      if(mergees.isEmpty)
        List(MergeResult(new ListBuffer[Quad](), null))
      else if(mergees.size == 1)
        List(MergeResult(mergees.head.quads,
          prefixMap.resolvePrefix(mergees.head.longestPrefix, FilterTarget.resolveQuadResource(mergees.head.quads.head, target)).prefix))
      else{
        val prefix = mergees.head.longestPrefix
        var ret = Map[String, ListBuffer[Quad]]()
        mergeQuads(mergees, (q: Quad) => {
          val actualPrefix = prefixMap.resolvePrefix(prefix, FilterTarget.resolveQuadResource(q, target))
          ret.get(actualPrefix.prefix) match{
            case Some(l) => l.append(q)
            case None =>{
              val lb = new ListBuffer[Quad]()
              lb.append(q)
              ret += actualPrefix.prefix -> lb
            }
          }
        })
        ret.map(x => MergeResult(x._2, x._1)).toList
      }
    }

  /**
    * The writerworker offers a file-sink for the sorted quads if available (this.writerDestination != null)
    * @return else it returns the mergeWorker
    */
  private def writeWorker(dest: Destination) =
    PromisedWork[List[ListBuffer[Quad]], Unit](1.5, 1.5) { quads: List[ListBuffer[Quad]] =>
      mergeQuads(quads.map(quad => MergeResult(quad, null)), (q: Quad) => dest.write(Seq(q)))
    }

  private def simpleSinkWorker(quadSink: Traversable[Quad] => Unit) = {
    PromisedWork[Iterator[Quad], Unit](1.5, 1.5) { quads: Iterator[Quad] =>
      quads.foreach(x => quadSink(Seq(x)))
    }
  }

  /**
    * This will initialize the Quad merge sort with input files
    * @param inputFiles - the input files
    */
  def sortFile(inputFiles: FileLike[_] *): Unit = {
    if(config == null)
      throw new IllegalArgumentException("Please initialize this class with a Config file.")
    tempFolder.mkdir()

    for(inputFile <- inputFiles) {
      val reader = new QuadReader(recorder)
      startTime = System.currentTimeMillis()
      config.logDir match{
        case Some(dir) =>
          this.recorder = new LogRecorder[Quad](IOUtils.writer(new RichFile(new File(dir, inputFile.name.replace(config.inputSuffix, "") + "-sorted.log"))))
          this.recorder.initialize("", "sorting quads", Seq(inputFile.name))
        case None =>
      }
      segmentMap.clear()
      var fileQuadSize = 0
      val outFile =new File(config.dumpDir.getFile, inputFile.name.replace(config.inputSuffix, "") + "-sorted" + config.inputSuffix)
      val targetSize = QuadSorter.calculateFileSegmentation(inputFile)

      var fileFinished = false

      while(!fileFinished) {
        numberOfQuads = 0
        buffer.clear()
        buckedMap.clear()
        (1 to PromisedWork.defaultThreads).foreach(i => buckedMap.put(i, new ListBuffer[Quad]))

        var quads = new ListBuffer[Quad]()
        fileFinished = reader.readQuads("", inputFile, 50000000) { quad =>
          quads.append(quad)
          if (quads.size == QuadSorter.INITIALBUCKETSIZE) {
            buffer.append(initialSorter.work(quads.toList))
            quads = new ListBuffer[Quad]()
          }
          numberOfQuads += 1
        }
        buffer.append(initialSorter.work(quads.toList))
        PromisedWork.waitAll(buffer)

        sortBuffer()
        fileQuadSize += numberOfQuads

        writeCompressedFile(inputFile.name)
      }

      this.recorder.printLabeledLine("Merging all part-files to produce final, sorted {dataset}", RecordSeverity.Info)
      mergeTemporaryFiles(outFile, fileQuadSize)

      this.recorder.printLabeledLine("Sorting of {pages} quads completed after {time} for: {dataset}", RecordSeverity.Info)
    }
    Files.delete(tempFolder.toPath)
  }

  private def mergeTemporaryFiles(outFile: File, finalSize: Int): Unit = {
    val pfs = segmentMap.values.map(x => {
      val prefix = prefixMap.getPrefix(Integer.valueOf("prefix\\d+".r.findFirstIn(x.name).get.substring(6)))
      val order = prefixMap.getPrefixOrder(prefix)
      (order, prefix, x)
    })

    val prefixGroups = pfs.groupBy(x => x._1)

    val finalMergeSinkWorker = PromisedWork[Iterable[(Int, String, RichFile)], FileDestination](1.5, 1.5) { input: Iterable[(Int, String, RichFile)] =>
      val destination = if(prefixGroups.size == 1)
        getPureDestination(outFile.getName, 0)
      else
        getPureDestination(outFile.getName.replace(config.inputSuffix, "") + "-final-temp" + input.head._1 + config.inputSuffix, input.head._1)
      destination.open()
      new QuadReader(recorder).readSortedQuads("Merging part-files for prefix: " + input.head._2, input.map(x => x._3).toSeq, target) { quads =>
        destination.write(quads)
      }
      destination.close()
      destination
    }

    val finalPromise = finalMergeSinkWorker.work(prefixGroups.values.toList)
    val futureList = PromisedWork.waitAll(finalPromise)

    PromisedWork.waitAll(List(futureList.andThen {
      case Success(fileList) =>
        if (prefixGroups.size > 1) {
          val headerFooter = createHeaderFooter(finalSize)
          val temFiles = fileList.toList.sortWith((x, y) => Comparator.naturalOrder().compare(x.file.getName, y.file.getName) < 0).map(x => x.richFile)
          val files: List[FileLike[_]] = List(headerFooter._1) ::: temFiles ::: List(headerFooter._2)
          if (!IOUtils.concatFile(files, new RichFile(outFile)))
            throw new RuntimeException("Concatenating temporary files failed!")
          files.foreach(x => Files.delete(x.getFile.toPath))
        }
      case Failure(f) => throw new RuntimeException("Writing the output file failed: " + f.getMessage)
    }))
    for(tempFile <- segmentMap.values)
      Files.delete(tempFile.getFile.toPath)
  }

  private def createHeaderFooter(finalSize: Int) ={
    val headerFile = new RichFile(new File(tempFolder, "headerFile" + config.inputSuffix))
    val hWriter = IOUtils.writer(headerFile)
    hWriter.append("#RDF properties: " + finalSize + " quads, sorted by " + this.target + ", " + config.getFormatter.get.serialization + " serialization\n")
    hWriter.close()
    val footerFile = new RichFile(new File(tempFolder, "footerFile" + config.inputSuffix))
    val fWriter = IOUtils.writer(footerFile)
    fWriter.append("#sorting finished at " + StringUtils.formatCurrentTimestamp + " after " + StringUtils.prettyMillis(System.currentTimeMillis() - startTime))
    fWriter.close()
    (headerFile, footerFile)
  }

  /**
    * This will initialize the Quad merge sort with a given Traversable[Quad]
    *
    * @param input the Traversable[Quad] to sort
    * @return returns a sorted Iterator of Quads
    */
  def sort(input: Traversable[Quad]): Iterator[Quad] = {
    numberOfQuads = input.size
    for(i <- 0 until input.size by 100){
      buffer.append(initialSorter.work(input.slice(i, i+100).toList))
    }
    //sort the created buffer and merge the results into a single Iterator
    sortBuffer()
    mergeToOne()
  }

  /**
    * harvests the current buffer and resets it afterwards
    * @return the harvested bins of the last merge round
    */
  private def copyAndClearBuffer(): Map[String, List[MergeResult]] ={
    //collect and unbox content of buffer
    val ret = buffer.toList.flatMap(x => x.future.value match{
      case Some(l) => l match{
        case Success(s) => s
        case Failure(f) => List(MergeResult(new ListBuffer[Quad](), null))
      }
      case None => List(MergeResult(new ListBuffer[Quad](), null))
    })
    buffer.clear()

    //group by prefix and sort out empty results
    ret.groupBy(x => x.longestPrefix).filter(x => x._2.nonEmpty && x._1 != null)
  }

  /**
    * Once the internal buffer is loaded with data, this method will sort the ever decreasing
    * number of segments (bins) until there are (number of available cores) bins left
    * @return a sorted Iterator[Quad]
    */
  private def sortBuffer(): Unit ={
    evaluatePrefixes()
    //get the content of the current buffer and clear it for the next merge
    val bins = copyAndClearBuffer()
    val count = bins.values.flatten.map(x => x.quads.size).sum
    this.recorder.printLabeledLine("Sorting and merging " + count + " quads into temporary file.", RecordSeverity.Info)
    assert(numberOfQuads == bins.values.map(x => x.map(y => y.quads.size).sum).sum, "Amount of triples in sorted buckets did not match the input size!")

    //calculate the partitioning of the first merge
    //here we calculate the best distribution of prepared bins to so we will have X² bins after the first merge run
    // X = number of available cores (e.g. for a machine with 8 cores at least -> 8²: 64 bins or 8³: 512 etc)
    val partitioning = QuadSorter.calculateBestPartitioning(bins)

    //now we feed the merger with the calculated
    var posNow = 0
    for(part <- partitioning.partitioning){
      for(i <- 0 until part._1){
        for(slice <- bins.values) {
          val params = slice.slice(posNow + i * part._2, posNow + (i + 1) * part._2)
          if(params.nonEmpty)
            buffer.append(mergeWorker().work(params))
        }
      }
      posNow += part._1*part._2
    }
    PromisedWork.waitAll(buffer)




    //while buffer size > X^1 -> merge until X^1
    var bufferCopy = copyAndClearBuffer()
    var groupedBins = bufferCopy.values.flatMap(x => x.grouped(PromisedWork.defaultThreads))
    var gbs = groupedBins.map(x => x.head.longestPrefix).toList.distinct.size
    while(gbs < groupedBins.size || groupedBins.map(x => x.size).sum > groupedBins.size){
      assert(count == groupedBins.flatten.map(x => x.quads.size).sum, "Amount of triples in sorted buckets did not match the input size!")
      sqrMerge(groupedBins.toList, mergeWorker())
      bufferCopy = copyAndClearBuffer()
      groupedBins = bufferCopy.values.flatMap(x => x.grouped(PromisedWork.defaultThreads))
      gbs = groupedBins.map(x => x.head.longestPrefix).toList.distinct.size
    }

/*    groupedBins = copyAndClearBuffer().values.flatMap(x => x.grouped(PromisedWork.defaultThreads))

    assert(count == groupedBins.flatten.map(x => x.quads.size).sum, "Amount of triples in sorted buckets did not match the input size!")
    sqrMerge(groupedBins.toList, mergeWorker())*/

    // now we have X^0 bins -> we can calculate the best distribution of the writer buckets
    calculateWriterDistribution(bufferCopy)
  }


  /**
    * initiates a common merge step by providing the input bins and the worker to forward those to
    * @param input - the bins of sorted quads
    * @param promisedWork - the worker to work on these bins
    */
  private def sqrMerge(input: List[List[MergeResult]], promisedWork: PromisedWork[List[MergeResult], List[MergeResult]]) : Unit = {
    if(input.isEmpty)
      return
    input.foreach(x => buffer.append(promisedWork.work(x)))
    PromisedWork.waitAll(buffer)
  }

  /**
    * Will partition the final merge result into (near as) equal partitions for the temp files (which then can be concatenated together without loosing the sort)
    * Note: this method should only be called with prefix partition size ==1 -> so after having merged everything into their final sequences
    */
  private def calculateWriterDistribution(bufferMap: Map[String, List[MergeResult]]): Unit = {
    val buckets = Math.max(bufferMap.size, PromisedWork.defaultThreads)
    val bestCount = (numberOfQuads.toDouble / buckets).toInt
    var bucket = 1
    var countPartition = 1

    for (cd <- bufferMap) {
      val cdSize = cd._2.map(x => x.quads.size).sum
      var prefixPartitionBuckets = Math.max(1d, Math.round(cdSize.toDouble / bestCount.toDouble)).toInt
      //double check if this does not exceed the max buckets size (max buckets size - buckets already in use - buckets still needed)
      prefixPartitionBuckets = Math.min(buckets - bucket - (bufferMap.size - countPartition -1), prefixPartitionBuckets)
      val bestSizeForPrefixPartition = Math.ceil(cd._2.map(x => x.quads.size).sum.toDouble / prefixPartitionBuckets.toDouble).toInt
      countPartition += 1

      for(mergee <- cd._2){
        var quads = mergee.quads
        var restSize = mergee.quads.size
        while(restSize > bestSizeForPrefixPartition){
          val zw = quads.splitAt(bestSizeForPrefixPartition)
          quads = zw._2
          buckedMap.put(bucket, zw._1)
          restSize -= zw._1.size
          bucket +=1
        }
        if(restSize > 0) {
          buckedMap.put(bucket, quads)
          bucket += 1
        }
      }
    }
    assert(bucket == buckets+1)
  }

  /**
    * This will output the last X bins as temporary files and concatenate them afterwards to the target output file
    * The use of multiple output files is chosen for faster writing to the disc
    * @param template - file name template for input files
    */
  private def writeCompressedFile(template: String) = {
    val fnt = if(template == null || template.isEmpty)
      throw new IllegalArgumentException("No file name template provided!")
    else
      template.replace(config.inputSuffix, "")

    this.recorder.printLabeledLine("Writing temporary part-file.", RecordSeverity.Info)

    //create a worker for each writer and feet it with the sorted bucket
    var writerCount = 0
    val destinations = new ListBuffer[FileDestination]()
    val writerPromise= for(dest <- 1 to PromisedWork.defaultThreads) yield {
      val fileName = fnt + "-%s-num" + writerCount + config.inputSuffix
      val prefix = buckedMap(dest).headOption match{
        case Some(q) => prefixMap.keys.find(x => FilterTarget.resolveQuadResource(q, this.target).contains(x) && prefixMap.get(x).nonEmpty) match{
          case Some(s) => prefixMap.getPrefixIndex(s)
          case None => 0
        }
        case None => 0
      }
      val destination = getPureDestination(fileName, prefix)
      val worker = simpleSinkWorker((q: Traversable[Quad]) => destination.write(q))
      destinations.append(destination)
      writerCount += 1
      worker.work(buckedMap(dest).toIterator)
    }

    //wait and close writers
    PromisedWork.waitAll(writerPromise)
    destinations.foreach(_.close())

    //group by prefixes (as tag) and concat output files
    for(prefixGroup <- destinations.groupBy(x => x.getTag)){
      val tempNumber = org.apache.commons.lang3.StringUtils.leftPad(String.valueOf(numberOfSegments), 3, '0')
      val outFile = new RichFile(new File(tempFolder, fnt + "-" + prefixGroup._1 + "-temp" + tempNumber + config.inputSuffix))
      outFile.getFile.createNewFile()
      segmentMap.put(numberOfSegments, outFile)

      //concatinate the temp files to the result file (use only non negative key - negative entries are fo split files)
      if(false == IOUtils.concatFile(prefixGroup._2.toList.sortBy(x => x.file.getName).map( x => new RichFile(x.file)), outFile))
        throw new RuntimeException("Concatenating temporary files failed!")

      numberOfSegments += 1
    }
    destinations.foreach(x => x.file.delete())
    this.recorder.printLabeledLine("Finished writing part-files, moving on to the next section.", RecordSeverity.Info)
  }

  private def getPureDestination(file: String, prefix: Int): FileDestination ={
    val prefixNumber = org.apache.commons.lang3.StringUtils.leftPad(String.valueOf(prefix), 3, '0')
    val destination = new FileDestination(new File(tempFolder, String.format(file, "prefix" + prefixNumber)), config.getFormatter.get)
    destination.setHeader("")
    destination.setFooter("")
    destination.setTag("prefix" + prefixNumber)
    destination.open()
    destination
  }

  /**
    * write to a given destination (could be a file
    * @param destination - the destination
    * @return
    */
  private def writeToDestination(destination: Destination) = {

/*    val buckets = buckedMap.values.toList.map(x => x.asScala.iterator).grouped(PromisedWork.defaultThreads)
    sqrMerge(buckets.toList, mergeWorker())
    PromisedWork.waitAll(buffer.map(x => x.future))
    val writerPromise = writeWorker(destination).work(buffer.toList.map(x => x.future.value match {
      case Some(l) => l match {
        case Success(s) => s.quads
        case Failure(f) => Iterator.empty
      }
      case None => Iterator.empty
    }))
    PromisedWork.waitAll(Seq(writerPromise.future))*/
  }

  /**
    * this is used to merge the last X bins and return the iterator over all entries
    * @return
    */
  private def mergeToOne(): Iterator[Quad] = {
    /*    val buckets = buckedMap.values.toList.map(x => x.asScala.iterator).grouped(PromisedWork.defaultThreads)
    val writerPromise = redirectMergeWorker().work(buffer.toList.map(x => x.future.value match {
      case Some(l) => l match {
        case Success(s) => s
        case Failure(f) => QuadSorter.MergeResult(List.empty, null)
      }
      case None => QuadSorter.MergeResult(List.empty, null)
    }))
    PromisedWork.waitAll(Seq(writerPromise.future))

    writerPromise.future.map(i => i.quads.toIterator).recover{
      case t: Throwable =>
        t.printStackTrace()
        Iterator.empty
    }.result(Duration.Zero)
  */
    Iterator.empty
  }
}

object QuadSorter{

  private val INITIALBUCKETSIZE = 100
  private val MAXMEMUSAGE = 500000000l

  private val BestThreadTargets = for(i <- 0 to 20)
    yield Math.pow(PromisedWork.defaultThreads, i).toLong

  def getNextThreadtarget(in: Int): Long = {
    for(i <- 4 to 20)
      if(in < BestThreadTargets(i))
        return BestThreadTargets(i - 2)
    0
  }

  /**
    * Will calculate the best distributions of an initial set of bins to reach a target size of bins after a merge
    * The target size is (number of available cores) to the power of 3 (or more) depending on the number of current bins
    * @param bins - the copy of the current buffer
    * @return the best BinDistribution
    */
  def calculateBestPartitioning(bins: Map[String, List[MergeResult]]) : BinDistribution = {

    val actualSIze = bins.values.flatten.size
    val partitioning = getNextThreadtarget(actualSIze)
    val bestSize = Math.ceil(actualSIze.toDouble / partitioning.toDouble)
    if(bins.size == 1) {
      val upper = partitioning - (bestSize * partitioning - actualSIze)
      BinDistribution(List((upper.toInt, bestSize.toInt), (partitioning.toInt - upper.toInt, bestSize.toInt - 1)))
    }
    else {
      val zw = new ListBuffer[(Int, Int)]()
      for(bin <- bins){
        val parts = Math.ceil(bin._2.size.toDouble/bestSize.toDouble).toInt
        val uto = parts * bestSize
        val upper = uto - bin._2.size
        val fac = Math.min(bin._2.size, bestSize.toInt)
        val lower = Math.max(parts-upper.toInt, 1)
        zw.append((lower, fac))
        if(lower*fac == bin._2.size)
          zw.append((0 , fac - 1))
        else
          zw.append((upper.toInt , fac - 1))
      }
      BinDistribution(zw.toList)
    }
  }


  /**
    * If fileSize exceeds heap space, fragment the file into x segments á y bytes
    * @param file the file
    * @return number of fragment to split in
    */
  def calculateFileSegmentation(file: FileLike[_]): (Int, Long) ={
    val length = file.getFile.length()
    val fileCompressionFactor = IOUtils.estimateCompressionRatio(file)
    val presumableFreeMemory = (Runtime.getRuntime.maxMemory - (Runtime.getRuntime.totalMemory()-Runtime.getRuntime.freeMemory)) * 0.6
    if(length*fileCompressionFactor > presumableFreeMemory){
      val segments = Math.max(Math.ceil(length.toDouble*fileCompressionFactor*2d / presumableFreeMemory.toDouble) +1d, PromisedWork.defaultThreads).toInt
      (segments, (length.toDouble*fileCompressionFactor / segments).toLong)
    }
    else
      (1, Math.min(MAXMEMUSAGE, length.toDouble*fileCompressionFactor).toLong)
  }

  def main(args: Array[String]): Unit ={
    assert(args.length > 0, "Please provide a properties file.")
    val config = new Config(args(0))

    val target = FilterTarget.subject
    val inputFile = config.inputDatasets.map(in => new RichFile(new File(config.dumpDir.getFile, in + config.inputSuffix)))

    val sorter = new QuadSorter(target, config)
    sorter.sortFile(inputFile: _*)

    PromisedWork.shutdownExecutor()
  }

  case class MergeResult(quads: ListBuffer[Quad], longestPrefix: String )
  case class BinDistribution(partitioning: List[(Int, Int)])
  class PrefixRecord(val prefix: String, val index: Int, val charMap: mutable.Map[Char, Int], val redirect: Option[String] = None, var split: Boolean = false){

    def count: Int = charMap.values.sum

    def toggleSplit() = if(charMap.nonEmpty) split = !split else throw new IllegalStateException("Trying to split an empty CharPrefixMap!")
  }
}