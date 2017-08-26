package org.dbpedia.quad.sort

import java.util.Comparator

import org.dbpedia.quad.Quad
import org.dbpedia.quad.utils.{FilterTarget, StringUtils}

/**
  * Created by chile on 14.06.17.
  */
class QuadComparator(val target: FilterTarget.Value, prefix: String = null) extends Comparator[Quad]{
  private val stringComp = new CodePointComparator()
  private var commonPrefix: String = if(prefix != null) prefix else ""

  def compare(quad1: Quad, quad2: Quad): Int = {
    if(quad1 == null || quad2 == null)
      throw new IllegalArgumentException("Comparing Quad with null: ")
    if(prefix == null){                   //use the complete string to compare - record the longest common prefix
      val zw1 = FilterTarget.resolveQuadResource(quad1, target)
      val zw2 = FilterTarget.resolveQuadResource(quad2, target)
      val res = stringComp.compare(zw1, zw2)
      if(prefix == null){
        val pre = zw1.substring(0, stringComp.getShortestCharDiffPosition)
        commonPrefix = StringUtils.getLongestPrefix(commonPrefix, pre)
      }
      res
    }
    else{     //subtract the prefix and compare TODO make sure the prefix fits ???
      val zw1 = FilterTarget.resolveQuadResource(quad1, target).substring(prefix.length)
      val zw2 = FilterTarget.resolveQuadResource(quad2, target).substring(prefix.length)
      stringComp.compare(zw1, zw2)
    }
  }

  def getCommonPrefix: String = {
      commonPrefix
  }
}
