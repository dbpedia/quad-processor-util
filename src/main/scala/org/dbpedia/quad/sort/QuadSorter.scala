package org.dbpedia.quad.sort

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap

import org.dbpedia.quad.Quad
import org.dbpedia.quad.config.Config
import org.dbpedia.quad.destination.{Destination, FileDestination}
import org.dbpedia.quad.file.{FileLike, IOUtils, RichFile}
import org.dbpedia.quad.processing.{PromisedWork, QuadReader}
import org.dbpedia.quad.sort.QuadSorter.MergeResult
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
  private val buffer: ListBuffer[Promise[List[MergeResult]]] = new ListBuffer[Promise[List[MergeResult]]]()
  private var sourceReader: QuadReader = _
  private val segmentMap = new mutable.HashMap[Int, RichFile]()
  private val charDistributionComparisonLength = 2
  private val charDistribution: concurrent.Map[String, concurrent.Map[String, Int]] = new ConcurrentHashMap[String, concurrent.Map[String, Int]]().asScala
  private var recordCharDistribution = false
  private var numberOfQuads = 0
  private var numberOfSegments = 0
  private var tempFolder: File = _
  private val buckedMap: concurrent.Map[Int, ListBuffer[Quad]] = new ConcurrentHashMap[Int, ListBuffer[Quad]]().asScala
  private var prefixMap = Map[String, Int]()

  private def addPrefix(prefix: String): Unit ={
    if(!prefixMap.contains(prefix))
        prefixMap += prefix -> prefixMap.size
  }

  private def getPrefix(index: Int): String={
    prefixMap.find(x => x._2 == index) match{
      case Some(p) => p._1
      case None => ""
    }
  }

  private def getPrefixIndex(prefix: String): Int = prefixMap.get(prefix) match{
    case Some(x) => x+1
    case None => 0
  }

  private def getPrefixOrder(prefix: String): Int = {
    prefixMap.keys.toList.sorted.indexWhere(x => prefix ==x)
  }

  /**
    * This worker does the initial compare of the most atomic segments (~100 Quads) using a simple sortWith
    */
  private val initialSorter = PromisedWork[List[Quad], List[MergeResult]](1.5, 1.5){ quads: List[Quad] =>

    var ret = new ListBuffer[MergeResult]()
    if(quads.nonEmpty) {
      for(m <- evaluatePrefixes(quads)) {
        ret += new MergeResult(m._2, m._1)
      }
      ret.toList
    }
    else
      List(new MergeResult(new ListBuffer[Quad](), null))
  }

  private def evaluatePrefixes(quads: List[Quad]): Map[String, ListBuffer[Quad]] ={
    var map = Map[String, ListBuffer[Quad]]()
    val comp = new QuadComparator(target)
    val sorted = quads.sortWith((quad1, quad2) => {
      comp.compare(quad1, quad2) < 0
    })

    for(i <- 1 until sorted.size){
      val resourcea = FilterTarget.resolveQuadResource(sorted(i-1), target)
      val resourceb = FilterTarget.resolveQuadResource(sorted(i), target)
      var prefix = StringUtils.getLongestPrefix(resourcea, resourceb)
      val pathLength = Math.max(resourcea.lastIndexOf("/"), resourcea.lastIndexOf("#"))+1
      if(pathLength > 0 && prefix.length > pathLength)
        prefix = prefix.substring(0, pathLength)
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

    val prefixes = map.keySet.toList.sorted
    prefixes.reduceLeft((f,s) => {
      if(StringUtils.getLongestPrefix(f, s) == f) {
        map.get(f) match {
          case Some(m) => m.foreach(q => {
            val resource = FilterTarget.resolveQuadResource(q, target)
            val maxIndex = prefixes.map(y => StringUtils.getLongestPrefix(resource, y).length).zipWithIndex.maxBy(_._1)
            if (maxIndex._1 <= f.length)
              throw new RuntimeException("nope should not happen")
            map(prefixes(maxIndex._2)).append(q)
          })
          case None => throw new RuntimeException("nope should not happen")
        }
        map(f).clear()
      }
      s
    })
    for(m <- map.filter(x => x._2.nonEmpty))
      addPrefix(m._1)
    map
  }

  /**
    * a simple map add for the charDistributionMap
    * @param str the chars to add
    */
  private def addCharDistributionRecord(prefix: String, str: String): Unit = synchronized {
    if(str.length < charDistributionComparisonLength)
      return
    val key = str.substring(0, charDistributionComparisonLength)
    this.charDistribution.get(prefix) match {
      case Some(t) => t.get(key) match {
        case Some(m) => t.put(key, m + 1)
        case None => t.put(key, 1)
      }
      case None =>
        val zw = new ConcurrentHashMap[String, Int]()
        zw.put(key, 1)
        this.charDistribution.put(prefix, zw.asScala)
    }
  }

  /**
    * The merge method of two or more already sorted segments (using a quad-sink to digest the results)
    * @param mergees - a list of MergeResults - the already sorted segments to merge
    * @param sink - the Quad sink to which to forward the quads
    * @return - the longest common prefix of all instances detected (if ignorePrefix != null it is equal to ignorePrefix)
    */
  def mergeQuads (mergees: List[MergeResult], sink: (Quad) => Unit) : String = {
    val comp = new QuadComparator(target, mergees.head.longestPrefix)
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

      if(recordCharDistribution && mergees.head.longestPrefix != null) {
        //only positions after the prefix are of interest
        addCharDistributionRecord(mergees.head.longestPrefix, FilterTarget.resolveQuadResource(head._1, target).substring(mergees.head.longestPrefix.length))
      }
      sink(head._1)
    }
    //addPrefix(comp.getCommonPrefix)
    comp.getCommonPrefix
  }

  /**
    * This is a simple worker calling the merge method and storing the results in a list
    */
  private def mergeWorker() =
    PromisedWork[List[MergeResult], List[MergeResult]](1.5, 1.5) { mergees: List[MergeResult] =>
      val test = if(mergees.isEmpty) null
        else mergees.reduceLeft[MergeResult]((x,y) => if(x != null && x.longestPrefix == y.longestPrefix) y else null)
      if(test == null)
        throw new IllegalArgumentException("Attempt to merge bins with different prefixes.")
      //execute merger and collect the longest prefix
      if(mergees.isEmpty)
        List(new MergeResult(new ListBuffer[Quad](), null))
      else if(mergees.size == 1)
        List(mergees.head)
      else{
        var ret = new ListBuffer[Quad]()
        mergeQuads(mergees, (q: Quad) => ret.append(q))
        val r = List(new MergeResult(ret, mergees.head.longestPrefix))
        ret = null
        r
      }
    }

  private def redirectMergeWorker(): PromisedWork[List[MergeResult], List[MergeResult]] = {

    def redirectQuadToBucked(q: Quad, longestPrefix: String): Unit = synchronized {

      val key = try {
        //get the first to chars after the longest common prefix
        val targetResource = FilterTarget.resolveQuadResource(q, target)
        if(targetResource.length >= longestPrefix.length + charDistributionComparisonLength)
          targetResource.substring(longestPrefix.length, longestPrefix.length + charDistributionComparisonLength)
        else
          ""
      } catch {
        case t: Throwable => ""
      }
      charDistribution.get(longestPrefix) match {
        case Some(b) => b.get(key) match{
          case Some(c) => buckedMap(c).append(q)
          case None => throw new RuntimeException("Quad could not be placed in a bucket: " + q.toString())
        }
        case None => throw new RuntimeException("Quad could not be placed in a bucket: " + q.toString())
      }
    }

    PromisedWork[List[MergeResult], List[MergeResult]](1.5, 1.5) { mergees: List[MergeResult] =>

      val test = mergees.reduceLeft[MergeResult]((x,y) => if(x != null && x.longestPrefix == y.longestPrefix) y else null)
      if(test == null)
        throw new IllegalArgumentException("Attempt to merge bins with different prefixes.")
      //execute merger and collect the longest prefix
      val lp = mergees.head.longestPrefix
      mergeQuads(mergees, (q: Quad) => redirectQuadToBucked(q, lp))
      mergees.foreach(x => x.finalize())
      List(new MergeResult(new ListBuffer[Quad](), lp))
    }
  }

  /**
    * The writerworker offers a file-sink for the sorted quads if available (this.writerDestination != null)
    * @return else it returns the mergeWorker
    */
  private def writeWorker(dest: Destination) =
    PromisedWork[List[ListBuffer[Quad]], Unit](1.5, 1.5) { quads: List[ListBuffer[Quad]] =>
      mergeQuads(quads.map(quad => new MergeResult(quad, null)), (q: Quad) => dest.write(Seq(q)))
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
    tempFolder = Paths.get(config.dumpDir.getFile.getAbsolutePath, "tempsort").toFile
    tempFolder.mkdir()


    for(inputFile <- inputFiles) {
      segmentMap.clear()
      var fileQuadSize = 0
      val outFile =new File(config.dumpDir.getFile, inputFile.name.replace(config.inputSuffix, "") + "-sorted" + config.inputSuffix)
      val targetSize = QuadSorter.calculateFileSegmentation(inputFile)  //TODO
      this.sourceReader = new QuadReader(null, 100000)

      var fileFinished = false

      while(!fileFinished) {
        numberOfQuads = 0
        buffer.clear()
        charDistribution.clear()
        buckedMap.clear()
        (1 to PromisedWork.defaultThreads).foreach(i => buckedMap.put(i, new ListBuffer[Quad]))

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
        PromisedWork.waitAll(buffer)

        sortBuffer()
        fileQuadSize += numberOfQuads

        writeCompressedFile(inputFile.name)
      }

      if(numberOfSegments > 1){
        System.err.println("Merge " + segmentMap.size + " part-files to produce final, sorted file")
        mergeTemporaryFiles(outFile, fileQuadSize)
      }

      System.err.println("Sorting of " + fileQuadSize + " quads completed: " + outFile.getName)

      for(tempFile <- segmentMap.values)
        Files.delete(tempFile.getFile.toPath)
    }
    Files.delete(tempFolder.toPath)
  }

  private def mergeTemporaryFiles(outFile: File, finalSize: Int) = {
    val prefixGroups = segmentMap.values.map(x => {
      val prefix = getPrefix(Integer.valueOf("prefix\\d+".r.findFirstIn(x.name).get.substring(6)))
      val order = getPrefixOrder(prefix)
      (order, prefix, x)
    }).groupBy(x => x._1)

    val finalMergeSinkWorker = PromisedWork[Iterable[(Int, String, RichFile)], FileDestination](1.5, 1.5) { input: Iterable[(Int, String, RichFile)] =>
      val formatter = config.getFormatter.get
      formatter.setHeader("RDF properties: sorted by " + this.target + ", " + finalSize + " quads, " + formatter.serialization + " serialization")
      val destination = if(prefixGroups.size == 1)
        new FileDestination(outFile, formatter)
      else
        getPureDestination(outFile.getName.replace(config.inputSuffix, "") + "-final-temp" + input.head._1, input.head._1)
      destination.open()
      new QuadReader(null, 10000).readSortedQuads("Merging part-files for prefix: " + input.head._2, input.map(x => x._3).toSeq, target) { quads =>
        destination.write(quads)
      }
      destination.close()
      destination
    }

    val finalPromise = finalMergeSinkWorker.work(prefixGroups.values.toList)
    val futureList = PromisedWork.waitAll(finalPromise)

    futureList.onComplete {
      case Success(fileList) =>
        if (numberOfSegments > 1)
          if (!IOUtils.concatFile(fileList.map(x => x.richFile).toSeq, new RichFile(outFile)))
            throw new RuntimeException("Concatenating temporary files failed!")
      case Failure(f) => throw new RuntimeException("Writing the output file failed: " + f.getMessage)
    }
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
        case Failure(f) => List(new MergeResult(new ListBuffer[Quad](), null))
      }
      case None => List(new MergeResult(new ListBuffer[Quad](), null))
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
    System.err.println("Sorting " + count + " quads.")
    assert(numberOfQuads == bins.values.map(x => x.map(y => y.quads.size).sum).sum, "Amount of triples in sorted buckets did not match the input size!")

    //calculate the partitioning of the first merge
    //here we calculate the best distribution of prepared bins to so we will have X² bins after the first merge run
    // X = number of available cores (e.g. for a machine with 8 cores at least -> 8²: 64 bins or 8³: 512 etc)
    val partitioning = QuadSorter.calculateBestPartitioning(bins)

    System.err.println("Merging " + bins.values.flatten.size + " sorted quad bins to " + partitioning.partitioning.map(x => x._1).sum)
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
    recordCharDistribution = false

    //while buffer size > X² -> merge until X²
    while(buffer.size > Math.pow(PromisedWork.defaultThreads, 2)){
      val groupedBins = copyAndClearBuffer().values.flatMap(x => x.grouped(PromisedWork.defaultThreads))

      assert(count == groupedBins.flatten.map(x => x.quads.size).sum, "Amount of triples in sorted buckets did not match the input size!")

      System.err.println("Merging " + groupedBins.flatten.size + " sorted quad bins to " + groupedBins.size)
      sqrMerge(groupedBins.toList, mergeWorker())
    }

    // now we have X² bins -> we can calculate the best distribution of the X¹ Char agnostic buckets (each of these buckets have all entries of the files for their char range)
    calculateBucketDistribution()
    val groupedBins = copyAndClearBuffer().values.flatMap(x => x.grouped(PromisedWork.defaultThreads))
    System.err.println("Merging " + groupedBins.flatten.size + " sorted quad bins to " + groupedBins.size)

    assert(count == groupedBins.flatten.map(x => x.quads.size).sum, "Amount of triples in sorted buckets did not match the input size!")

    //feed the merger with the redirect worker which forwards the resulting quads into their respective Char agnostic bin
    sqrMerge(groupedBins.toList, redirectMergeWorker())
    // now we have X bins
    assert(count == buckedMap.values.map(x => x.size).sum, "Amount of triples in sorted buckets did not match the input size!")
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
    * After initially filling the char distribution table, we use this to allocate a target bin to each combination of initial characters
    * So that each resulting bin has a similar amount of entries
    * This is then used to redirect each quad to the allotted bin in the redirect worker
    */
  private def calculateBucketDistribution(): Unit ={
    val buckets = PromisedWork.defaultThreads
    val bestCount = (numberOfQuads.toDouble /buckets.toFloat).toInt
    var bucket = 0

    val list = this.charDistribution.toList

    for(cd <- list.indices) {
      var count = 0
      val bucketsAtStart = bucket
      val charDist = list(cd)._2
      var prefixPartitionBuckets = Math.max(1d, Math.round(charDist.values.sum.toDouble / bestCount.toDouble)).toInt
      //double check if this does not exceed the max buckets size (max buckets size - buckets already in use - buckets still needed)
      prefixPartitionBuckets = Math.min(buckets - bucket - (list.size - cd -1), prefixPartitionBuckets)
      val bestSizeForPrefixPartition = (charDist.values.sum.toDouble / prefixPartitionBuckets.toDouble).toInt
      bucket += 1 //we start with 1, each new prefix starts with a new bucket!
      val minFirst = charDist.keys.foldLeft(Char.MaxValue)((int, str) => if (int > str.charAt(0)) str.charAt(0) else int)
      val maxFirst = charDist.keys.foldLeft(Char.MinValue)((int, str) => if (int < str.charAt(0)) str.charAt(0) else int)
      val minSecond = charDist.keys.foldLeft(Char.MaxValue)((int, str) => if (int > str.charAt(1)) str.charAt(1) else int)
      val maxSecond = charDist.keys.foldLeft(Char.MinValue)((int, str) => if (int < str.charAt(1)) str.charAt(1) else int)

      for (i <- minFirst to maxFirst)
        for (j <- minSecond to maxSecond)
          charDist.get(Array(i, j).mkString) match {
            case Some(n) =>
              if (count + (n / 2) > ((bucket-bucketsAtStart) * bestSizeForPrefixPartition) && bucket < buckets)
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
    * @param template - file name template for input files
    */
  private def writeCompressedFile(template: String) = {
    val fnt = if(template == null || template.isEmpty)
      throw new IllegalArgumentException("No file name template provided!")
    else
      template.replace(config.inputSuffix, "")

    assert(buckedMap.size == PromisedWork.defaultThreads, "Amount of sorted buckets is not " + PromisedWork.defaultThreads)
    System.err.println("Started writing result to " + PromisedWork.defaultThreads + " temporary files.")

    //create a worker for each writer and feet it with the sorted bucket
    var writerCount = 0
    val destinations = new ListBuffer[FileDestination]()
    val writerPromise= for(dest <- 1 to PromisedWork.defaultThreads) yield {
      val fileName = fnt + "-%s-num" + writerCount + config.inputSuffix
      val prefix = buckedMap(dest).headOption match{
        case Some(q) => prefixMap.keys.find(x => FilterTarget.resolveQuadResource(q, this.target).contains(x) && prefixMap.get(x).nonEmpty) match{
          case Some(s) => getPrefixIndex(s)
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

    System.err.println("Concatenating all temp files to produce a final sorted copy")

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
      throw new RuntimeException("Something went wrong while calculating the best bucket distribution! Please create an issue on GitHub, this should not happen.")
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

  class MergeResult(val quads: ListBuffer[Quad], val longestPrefix: String ){
    override def finalize(){
      quads.clear()
    }
  }
  case class BinDistribution(partitioning: List[(Int, Int)])
}