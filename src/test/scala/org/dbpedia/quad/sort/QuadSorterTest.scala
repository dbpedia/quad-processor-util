package org.dbpedia.quad.sort

import org.dbpedia.quad.Quad
import org.dbpedia.quad.file.{RichFile, RichPath}
import org.dbpedia.quad.processing.QuadReader
import org.dbpedia.quad.sort.QuadSorter.MergeResult
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by chile on 14.06.17.
  */
class QuadSorterTest extends FunSuite {

  test("partitioning test"){
    var map = new mutable.HashMap[String, List[MergeResult]]()
/*
    //for(i <- 0 until 2) {
      val pf = "http://example.org/path/to/glory/"//+ ((i+1)*15)
      var zw = new ListBuffer[MergeResult]()
      for (j <- 0 until 3237)
        zw += MergeResult(new ListBuffer[Quad](), pf+"1")
      map +=  pf+"1" -> zw.toList
    zw = new ListBuffer[MergeResult]()
      for (j <- 0 until 123)
        zw += MergeResult(new ListBuffer[Quad](), pf+"2")
      map +=  pf+"2" -> zw.toList
    //}

    val res = QuadSorter.calculateBestPartitioning(map.toMap)*/
    //info(res.partitioning.toString)
  }

  test("test result file") {
    val comp = new CodePointComparator()

    val path = List("/home/chile/unifiedviews/solr-input/disambiguations_en-sorted.ttl.bz2")

    for (f <- path) {
      var lastQuad: Quad = null
      new QuadReader().readQuads("", new RichFile(f)) {
        quad => {
          if (lastQuad != null) {
            if (comp.compare(lastQuad.subject, quad.subject) > 0)
              throw new Exception("Not sorted: " + f)
          }
          lastQuad = quad
        }

      }
    }
  }
}
