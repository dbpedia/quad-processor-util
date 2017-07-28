package org.dbpedia.quad.sort

import java.util.Comparator

/**
  * Created by chile on 13.06.17.
  *
  * Compares by codepoint value of chars
  * records the smallest position of differing chars -> is used by the QuadComparator for prefix length calculation
  */
class CodePointComparator extends Comparator[String] {
  private var shortestCharDiffPosition = Int.MaxValue

  private def submitNewCharDiff(i: Int): Unit={
    if(i < shortestCharDiffPosition)
      shortestCharDiffPosition = i
  }

  override def compare(t: String, t1: String): Int = {
    val until = Math.min(t.length, t1.length)
    for(i <- 0 until until){
      val char1 = Character.codePointAt(t, i)
      val char2 = Character.codePointAt(t1, i)
      if(char1 < char2) {
        submitNewCharDiff(i)
        return -1
      }
      if(char1 > char2){
        submitNewCharDiff(i)
        return 1
      }
    }
    submitNewCharDiff(until)
    if(t1.length > t.length)
      return -1
    if(t.length > t1.length)
      return 1
    0
  }

  def getShortestCharDiffPosition: Int = shortestCharDiffPosition
}