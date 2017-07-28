package org.dbpedia.quad.sort

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet}

import org.dbpedia.quad.Quad
import org.dbpedia.quad.config.Config
import org.dbpedia.quad.destination.{Destination, DestinationUtils}
import org.dbpedia.quad.file.{FileLike, IOUtils, RichFile}
import org.dbpedia.quad.processing.{PromisedWork, QuadReader}
import org.dbpedia.quad.sort.QuadSorter.MergeResult
import org.dbpedia.quad.utils.{FilterTarget, StringUtils}

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.collection.{concurrent, mutable}
import scala.concurrent.Promise
import scala.util.{Failure, Success}

/**
  * Created by chile on 14.06.17.
  */
class QuadSorter(val target: FilterTarget.Value, val config: Config = Config.UniversalConfig) {
  private val buffer: ListBuffer[Promise[MergeResult]] = new ListBuffer[Promise[MergeResult]]()
  private var sourceReader: QuadReader = _
  private val segmentMap = new mutable.HashMap[Int, RichFile]()
  private val charDistributionComparisonLength = 2
  private val charDistribution: concurrent.Map[String, concurrent.Map[String, Int]] = new ConcurrentHashMap[String, concurrent.Map[String, Int]]().asScala
  private val generalQuadComp = new QuadComparator(target)
  private var recordCharDistribution = false
  private var numberOfQuads = 0
  private var tempFolder: File = _
  private val buckedMap: concurrent.Map[Int, ConcurrentSkipListSet[Quad]] = new ConcurrentHashMap[Int, ConcurrentSkipListSet[Quad]]().asScala

  /**
    * This worker does the initial compare of the most atomic segments (~100 Quads) using a simple sortWith
    */
  private val initialSorter = PromisedWork[List[Quad], MergeResult](1.5, 1.5){ quads: List[Quad] =>
    val comp = new QuadComparator(target)
    val iter = quads.sortWith((quad1, quad2) => {
      comp.compare(quad1, quad2) < 0
    })
    if(iter.nonEmpty) {
      val longestPrefix = quads.tail.foldLeft(comp.getNodeFunction(quads.head))((q1, q2) => StringUtils.getLongestPrefix(q1, comp.getNodeFunction(q2)))
      MergeResult(iter, longestPrefix)
    }
    else
      MergeResult(List(), null)
  }

  /**
    * a simple map add for the charDistributionMap
    * @param str the chars to add
    */
  private def addCharDistributionRecord(prefix: String, str: String): Unit ={
    if(str.length < charDistributionComparisonLength)
      return
    val key = str.substring(0, charDistributionComparisonLength)
    this.charDistribution.get(prefix) match {
      case Some(t) => t.get(key) match {
        case Some(m) => t.put(key, m + 1)
        case None => t.put(key, 1)
      }
      case None => {
        val zw = new ConcurrentHashMap[String, Int]()
        zw.put(key, 1)
        this.charDistribution.put(prefix, zw.asScala)
      }
    }
  }

  /**
    * The merge method of two or more already sorted segments (using a quad-sink to digest the results)
    * @param quads - 1: a list of Iterator[Quad] - aka. the already sorted segments to merge
    *              2: the Quad sink to which to forward the quads
    * @return - the longest common prefix of all instances detected (if ignorePrefix != null it is equal to ignorePrefix)
    */
  def mergeQuads (quads: List[MergeResult], sink: (Quad) => Unit) : String = {
    val comp = new QuadComparator(target, quads.head.longestPrefix)
    var treeMap :List[(Quad, List[Quad])] = List()
    for (i <- quads.indices)
      if(quads(i).quads.nonEmpty)
        treeMap = treeMap ::: List((quads(i).quads.head, quads(i).quads.tail))

    treeMap = treeMap.sortWith((x,y) => comp.compare(x._1, y._1) < 0)

    while (treeMap.nonEmpty) {
      val head = treeMap.head
      val next = head._2.headOption match{
        case Some(q) => Some((q, head._2.tail))
        case None => None
      }
      if(next.isDefined) {
        val spans = treeMap.tail.span(x => comp.compare(next.get._1, x._1) > 0)
        treeMap = spans._1 ::: List(next.get) ::: spans._2
      }
      else
        treeMap = treeMap.tail

      if(recordCharDistribution && quads.head.longestPrefix != null)        //only positions after the prefix are of interest
        addCharDistributionRecord(quads.head.longestPrefix, comp.getNodeFunction(head._1).substring(quads.head.longestPrefix.length))
      sink(head._1)
    }
    comp.getCommonPrefix
  }

  /**
    * This is a simple worker calling the merge method and storing the results in a list
    */
  private def mergeWorker() =
    PromisedWork[List[MergeResult], MergeResult](1.5, 1.5) { mergees: List[MergeResult] =>
      //val test = mergees.reduceLeft[MergeResult]((x,y) => if(x != null && x.longestPrefix == y.longestPrefix) y else null)
      //if(test == null)
      //  throw new IllegalArgumentException("Attempt to merge bins with different prefixes.")
      //execute merger and collect the longest prefix
      if(mergees.isEmpty)
        MergeResult(List(), "")
      else if(mergees.size == 1)
        mergees.head
      else{
        var ret = new ListBuffer[Quad]()
        mergeQuads(mergees, (q: Quad) => ret.append(q))
        val iter = ret.toList
        ret = null
        MergeResult(iter, mergees.head.longestPrefix)
      }
    }

  private def redirectMergeWorker(): PromisedWork[List[MergeResult], MergeResult] = {


    def redirectQuadToBucked(q: Quad, longestPrefix: String): Unit = {
      val key = try {
         generalQuadComp.getNodeFunction(q).substring(longestPrefix.length, longestPrefix.length + 2)
      } catch {
        case t: Throwable => ""
      }
      charDistribution.get(longestPrefix) match {
        case Some(b) => b.get(key) match{
          case Some(c) => buckedMap(c).add(q)
          case None =>
        }
        case None =>
      }
    }

    PromisedWork[List[MergeResult], MergeResult](1.5, 1.5) { mergees: List[MergeResult] =>

      //val test = mergees.reduceLeft[MergeResult]((x,y) => if(x != null && x.longestPrefix == y.longestPrefix) y else null)
      //if(test == null)
      //  throw new IllegalArgumentException("Attempt to merge bins with different prefixes.")
      //execute merger and collect the longest prefix
      val lp = mergees.head.longestPrefix
      mergeQuads(mergees, (q: Quad) => redirectQuadToBucked(q, lp))
      MergeResult(List(), lp)
    }
  }

  /**
    * The writerworker offers a file-sink for the sorted quads if available (this.writerDestination != null)
    * @return else it returns the mergeWorker
    */
  private def writeWorker(dest: Destination) =
    PromisedWork[List[List[Quad]], Unit](1.5, 1.5) { quads: List[List[Quad]] =>
      mergeQuads(quads.map(quad => MergeResult(quad, "")), (q: Quad) => dest.write(Seq(q)))
    }

  private def generalMergeWorker(quadSink: Traversable[Quad] => Unit) = {
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
    tempFolder = Paths.get(config.dumpDir.getFile.getAbsolutePath, "tempsort").toFile
    tempFolder.mkdir()


    for(inputFile <- inputFiles) {
      segmentMap.clear()
      var fileQuadSize = 0
      val outFile = new RichFile(new File(config.dumpDir.getFile, inputFile.name.replace(config.inputSuffix, "") + "-sorted" + config.inputSuffix))
      val targetSize = QuadSorter.calculateFileSegmentation(inputFile)
      this.sourceReader = new QuadReader(null, 100000)

      var fileFinished = false
      var segmentCount = 0

      while(!fileFinished) {
        //longestPrefix = ""
        numberOfQuads = 0
        buffer.clear()
        charDistribution.clear()
        buckedMap.clear()
        (1 to PromisedWork.defaultThreads).foreach(i => buckedMap.put(i, new ConcurrentSkipListSet[Quad]()))

        val f = new RichFile(new File(tempFolder, inputFile.name.replace(config.inputSuffix, "") + "-temp" + String.valueOf(segmentCount) + config.inputSuffix))
        f.getFile.createNewFile()
        segmentMap.put(segmentCount, f)

        var quads = new ListBuffer[Quad]()
        fileFinished = this.sourceReader.readQuads("sort quads", inputFile, 500000000) { quad =>
          quads.append(quad)
          if (quads.size == 100) {
            buffer.append(initialSorter.work(quads.toList))
            quads = new ListBuffer[Quad]()
          }
          numberOfQuads += 1
        }
        buffer.append(initialSorter.work(quads.toList))
        PromisedWork.waitAll(buffer.map(x => x.future))

        sortBuffer()
        fileQuadSize += numberOfQuads

        if(segmentCount == 0 && fileFinished)  //if only one segment this is our out-file
          writeCompressedFile(outFile)
        else                                    //more than one segment
          writeCompressedFile(segmentMap(segmentCount))

        segmentCount += 1
      }

      if(segmentCount > 0){
        System.err.println("Merge " + segmentMap.size + " part-files to produce final, sorted file")
        val destination = DestinationUtils.getWriterDestination(outFile, config.getFormatter.get)
        destination.open()
        new QuadReader(null, 10000).readSortedQuads("Merging part-files", segmentMap.values.toList, target){ quads =>
          destination.write(quads)
        }
        destination.close()
      }

      System.err.println("Sorting of " + fileQuadSize + " quads completed for: " + outFile.name)

      for(tempFile <- segmentMap.values)
        Files.delete(tempFile.getFile.toPath)
    }
    Files.delete(tempFolder.toPath)
  }

  /**
    * This will initialize the Quad merge sort with a given Traversable[Quad]
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
    val ret = buffer.toList.map(x => x.future.value match{
      case Some(l) => l match{
        case Success(s) => s
        case Failure(f) => MergeResult(List(), null)
      }
      case None => MergeResult(List(), null)
    })
    buffer.clear()
    //group by prefix and sort out empty results
    var rrr = ret.groupBy(x => x.longestPrefix).filter(x => x._2.nonEmpty && x._1 != null)
    if(rrr.size > PromisedWork.defaultThreads){
      val lp = rrr.keySet.tail.foldLeft(rrr.keySet.head)((q1, q2) => StringUtils.getLongestPrefix(q1, q2))
      rrr = Map(lp -> rrr.values.flatten.toList)
    }
    rrr
  }

  /**
    * Once the internal buffer is loaded with data, this method will sort the ever decreasing
    * number of segments (bins) until there are (number of available cores) bins left
    * @return a sorted Iterator[Quad]
    */
  private def sortBuffer(): Unit ={
    //in the first round of merges we are recording the char distribution
    recordCharDistribution = true
    //get the content of the current buffer and clear it for the next merge
    val bins = copyAndClearBuffer()
    val count = bins.values.flatten.map(x => x.quads.size).sum

    //calculate the partitioning of the first merge
    //here we calculate the best distribution of prepared bins to so we will have X² bins after the first merge run
    // X = number of available cores (e.g. for a machine with 8 cores at least -> 8²: 64 bins or 8³: 512 etc)
    val partitioning = QuadSorter.calculateBestPartitioning(bins)

    System.err.println("Merging " + bins.values.flatten.size + " sorted quad bins to " + partitioning.partitioning.map(x => x._1).sum)
    //now we feed the merger with the calculated
    var posNow = 0
    for(part <- partitioning.partitioning){
      for(i <- 0 until part._1){
        val params = bins.values.flatten.slice(posNow+i*part._2, posNow+(i+1)*part._2).toList
        buffer.append(mergeWorker().work(params))
      }
      posNow += part._1*part._2
    }

    PromisedWork.waitAll(buffer.map(x => x.future))
    recordCharDistribution = false

    //while buffer size > X² -> merge until X²
    while(buffer.size > Math.pow(PromisedWork.defaultThreads, 2)){
      val groupedBins = copyAndClearBuffer().values.flatMap(x => x.grouped(PromisedWork.defaultThreads))

      if(count != groupedBins.flatten.map(x => x.quads.size).sum)
        throw new Exception("Count does not match!")

      System.err.println("Merging " + groupedBins.flatten.size + " sorted quad bins to " + groupedBins.size)
      sqrMerge(groupedBins.toList, mergeWorker())
    }

    // now we have X² bins -> we can calculate the best distribution of the X¹ Char agnostic buckets (each of these buckets have all entries of the files for their char range)
    calculateBucketDistribution()
    val groupedBins = copyAndClearBuffer().values.flatMap(x => x.grouped(PromisedWork.defaultThreads))
    System.err.println("Merging " + groupedBins.flatten.size + " sorted quad bins to " + groupedBins.size)

    if(count != groupedBins.flatten.map(x => x.quads.size).sum)
       throw new Exception("Count does not match!")
    //feed the merger with the redirect worker which forwards the resulting quads into their respective Char agnostic bin
    sqrMerge(groupedBins.toList, redirectMergeWorker())
    // now we have X bins
  }


  /**
    * initiates a common merge step by providing the input bins and the worker to forward those to
    * @param input - the bins of sorted quads
    * @param promisedWork - the worker to work on these bins
    */
  private def sqrMerge(input: List[List[MergeResult]], promisedWork: PromisedWork[List[MergeResult], MergeResult]) : Unit = {

    input.foreach(x => buffer.append(promisedWork.work(x)))
    PromisedWork.waitAll(buffer.map(x => x.future))
  }

  /**
    * After initially filling the char distribution table, we use this to allocate a target bin to each combination of initial characters
    * So that each resulting bin has a similar ammount of entries
    * This is then used to redirect each quad to the allotted bin in the redirect worker
    */
  private def calculateBucketDistribution(): Unit ={
    val buckets = PromisedWork.defaultThreads
    val bestCount = (numberOfQuads.toFloat/buckets.toFloat).toInt
    var bucket = 1
    var count = 0

    for(charDist <- this.charDistribution.values) {
      val minFirst = charDist.keys.foldLeft(Char.MaxValue)((int, str) => if (int > str.charAt(0)) str.charAt(0) else int)
      val maxFirst = charDist.keys.foldLeft(Char.MinValue)((int, str) => if (int < str.charAt(0)) str.charAt(0) else int)
      val minSecond = charDist.keys.foldLeft(Char.MaxValue)((int, str) => if (int > str.charAt(1)) str.charAt(1) else int)
      val maxSecond = charDist.keys.foldLeft(Char.MinValue)((int, str) => if (int < str.charAt(1)) str.charAt(1) else int)

      for (i <- minFirst to maxFirst)
        for (j <- minSecond to maxSecond)
          charDist.get(Array(i, j).mkString) match {
            case Some(n) =>
              if (count + (n / 2) > (bucket * bestCount) && bucket < buckets)
                bucket += 1
              charDist.put(Array(i, j).mkString, bucket)
              count += n
            case None =>
          }
    }
  }

  /**
    * This will output the last X bins as temporary files and concatenate them afterwards to the target output file
    * The use of multiple output files is chosen for faster writing to the disc
    * @param outFile - the result file
    */
  private def writeCompressedFile(outFile: FileLike[_]) = {
    if(outFile == null)
      throw new IllegalArgumentException("No output file provided!")

    //create temp files
    val tempMap = (0 until PromisedWork.defaultThreads).map(i =>
      i -> new RichFile(new File(tempFolder, outFile.name.replace(config.inputSuffix, "") + "-temp" + i + config.inputSuffix))).toMap
    //create writer destinations
    val workerMap = tempMap.map(x => x._1 -> DestinationUtils.getWriterDestination(x._2, config.getFormatter.get))

    //mute all headers and footers except for the outer most
    for(i <- 0 until PromisedWork.defaultThreads){
      workerMap(i).setHeader("")
      workerMap(i).setFooter("")
    }

    //open writers
    workerMap.values.foreach(_.open())

    System.err.println("Writing " + workerMap.size + " sorted quad bins as temporary files")
    //create a worker for each writer and feet it with the sorted bucket
    val buckets = buckedMap.values.toList.map(x => x.asScala.toIterator)
    val writerPromise= for(sink <- workerMap) yield {
      val s = generalMergeWorker((q: Traversable[Quad]) => sink._2.write(q))
      s.work(buckets(sink._1))
    }

    //wait and close writers
    PromisedWork.waitAll(writerPromise.map(x => x.future).toList)
    workerMap.values.foreach(_.close())

    System.err.println("Concatenating all temp files to produce a final sorted copy")
    //concatinate the temp files to the result file (use only non negative key - negative entries are fo split files)
    val exit = IOUtils.concatFile(tempMap.values.toList.sortBy(x => x.name), outFile)

    if(exit != 0)
      throw new RuntimeException("Concatenating temporary files failed: cat command returned: " + exit + " !")
    for(tempFile <- tempMap.values)
      Files.delete(tempFile.getFile.toPath)
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
        case Success(s) => s.quads
        case Failure(f) => Iterator.empty
      }
      case None => Iterator.empty
    }))
    PromisedWork.waitAll(Seq(writerPromise.future))

    writerPromise.future.map(i => return i.quads).recover{
      case t: Throwable =>
        System.err.println(t.getMessage)
    }*/
    Iterator.empty
  }
}

object QuadSorter{
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
    else if(bins.size <= PromisedWork.defaultThreads){
      val zw = new ListBuffer[(Int, Int)]()
      for(bin <- bins){
        val parts = Math.ceil(bin._2.size.toDouble/bestSize.toDouble).toInt
        val uto = parts * bestSize
        val upper = uto - bin._2.size
        //val upper = Math.ceil(bin._2.size.toFloat/parts.toFloat)
        val fac = Math.min(bin._2.size, bestSize.toInt)
        val lower = Math.max(parts-upper.toInt, 1)
        zw.append((lower, fac))
        if(lower*fac == bin._2.size)
          zw.append((0 , fac - 1))
        else
          zw.append((upper.toInt , fac - 1))
      }
      System.err.println(zw.toString)
      BinDistribution(zw.toList)
    }
    else
      throw new IllegalArgumentException("At this stage this should not happen :)")
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
      (1, (length.toDouble*fileCompressionFactor).toLong)
  }

  def main(args: Array[String]): Unit ={
    assert(args.length > 0, "Please provide a properties file.")
    val config = new Config(args(0))

    val target = FilterTarget.subject
    val inputFile = config.inputDatasets.map(in => new RichFile(new File(config.dumpDir.getFile, in + config.inputSuffix)))

    val sorter = new QuadSorter(target, config)
    sorter.sortFile(inputFile: _*)
    System.exit(0)
  }

  case class MergeResult(quads: List[Quad], longestPrefix: String )
  case class BinDistribution(partitioning: List[(Int, Int)])
}