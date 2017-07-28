package org.dbpedia.quad.sort

/**
  * Created by chile on 13.06.17.
  */
class CodePointComparatorTest extends org.scalatest.FunSuite {

  val codepointcomp = new CodePointComparator
  test("compare") {
    var testStr1 = "<http://dbpedia.org/resource/Boston"
    var testStr2 = "<http://dbpedia.org/resource/BostonNOW"
    info("Result: " + codepointcomp.compare(testStr1, testStr2) + " -> for comparing " + testStr1 + " vs. " + testStr2)

    testStr1 = "<http://dbpedia.org/resource/Boston_&_Roxbury_Mill_Dam"
    testStr2 = "<http://dbpedia.org/resource/Boston"
    info("Result: " + codepointcomp.compare(testStr1, testStr2) + " -> for comparing " + testStr1 + " vs. " + testStr2)

    testStr1 = "<http://dbpedia.org/resource/Boston.com"
    testStr2 = "<http://dbpedia.org/resource/Boston"
    info("Result: " + codepointcomp.compare(testStr1, testStr2) + " -> for comparing " + testStr1 + " vs. " + testStr2)


    testStr1 = "http://dbpedia.org/resource/Boston"
    testStr2 = "http://dbpedia.org/resource/BostonNOW"
    info("Result: " + codepointcomp.compare(testStr1, testStr2) + " -> for comparing " + testStr1 + " vs. " + testStr2)

    testStr1 = "http://dbpedia.org/resource/Boston_&_Roxbury_Mill_Dam"
    testStr2 = "http://dbpedia.org/resource/Boston"
    info("Result: " + codepointcomp.compare(testStr1, testStr2) + " -> for comparing " + testStr1 + " vs. " + testStr2)

    testStr1 = "http://dbpedia.org/resource/Boston.com"
    testStr2 = "http://dbpedia.org/resource/Boston"
    info("Result: " + codepointcomp.compare(testStr1, testStr2) + " -> for comparing " + testStr1 + " vs. " + testStr2)
  }

  test("string intersect test"){
    info("http://dbpedia.org/resource/Boston_&_Roxbury_Mill_Dam".intersect("http://dbpedia.org/resource/Boston"))
    info("http://dbpedia.org/resource/Boston".intersect("http://dbpedia.org/resource/Boston_&_Roxbury_Mill_Dam"))
    info(List("http://dbpedia.org/resource/Boston","http://dbpedia.org/resource/Boston_&_Roxbury_Mill_Dam", "http://dbpedia.org/resource/").foldRight("http://dbpediaorg/resource/Boston")((x,y)=> x.zip(y).takeWhile(z => z._1 == z._2).unzip._1.mkString))
  }
}
