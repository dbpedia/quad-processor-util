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

  override def compare(s1: String, s2: String): Int = {
    if (s1 == null && s2 == null) return 0
    else if (s1 == null) return 1
    else if (s2 == null) return -1

    val until = Math.min(s1.length, s2.length)
    for(i <- 0 until until){
      val char1 = Character.codePointAt(s1, i)
      val char2 = Character.codePointAt(s2, i)
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
    if(s2.length > s1.length)
      return -1
    if(s1.length > s2.length)
      return 1
    0
  }

  def getShortestCharDiffPosition: Int = shortestCharDiffPosition
}