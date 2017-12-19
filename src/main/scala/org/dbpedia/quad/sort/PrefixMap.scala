package org.dbpedia.quad.sort

import java.util.Comparator

import org.apache.commons.collections4.trie.PatriciaTrie
import org.dbpedia.quad.sort.QuadSorter.PrefixRecord
import org.dbpedia.quad.utils.StringUtils

import scala.collection.mutable


import scala.collection.convert.decorateAsScala._

/**
  * Created by chile on 29.08.17.
  */
class PrefixMap extends PatriciaTrie[PrefixRecord] {

  private val comp =new CodePointComparator()
  override def comparator(): Comparator[_ >: String] = comp

  def addPrefix(prefix: String, charMap: mutable.Map[Char, Int], redirect: Option[String] = None, split: Boolean = false): Unit = synchronized {
    if(!this.keySet.contains(prefix))
      this.put(prefix, new PrefixRecord(prefix, this.size, charMap))
    else{
      val old = this.get(prefix)
      for(oc <- old.charMap){
        charMap.get(oc._1) match{
          case Some(i) => charMap.put(oc._1, oc._2 + i)
          case None => charMap.put(oc._1, oc._2)
        }
      }
      this.put(prefix, new PrefixRecord(prefix, old.index, charMap, redirect, split))
    }
  }

  def getPrefix(index: Int): String={
    this.asScala.toList.find(x => x._2.index == index-1) match{
      case Some(p) => p._1
      case None => ""
    }
  }


  def getPrefixIndex(prefix: String): Int = Option(this.get(prefix)) match{
    case Some(x) => x.index+1
    case None => 0
  }

  def getLongestPrefix(uri: String): String ={
    this.selectKey(uri)
  }

  def isContainedIn(prefix: String): List[PrefixRecord]={
    this.asScala.filter(x => x._2.redirect.isEmpty && !x._2.split && StringUtils.getLongestPrefix(x._1, prefix) == prefix).values.toList
  }

  /**
    * redirects prefixes if necessary
    * @param prefix
    */
  def resolvePrefix(prefix: String, resource: String): PrefixRecord ={
    Option(this.get(prefix)) match{
      case Some(p) => p.redirect match{
        case Some(r) => resolvePrefix(r, resource)
        case None => if(p.split) resolvePrefix(prefix + resource.trim.substring(prefix.length, prefix.length+1), resource) else p
      }
      case None => throw new IllegalStateException("Prefix was not found: " + prefix)  //should not happen
    }
  }

  def clearSplitPrefixes(): Unit ={
    this.asScala.filter(x => x._2.split).keys.foreach(p => this.remove(p))
  }

  def apply(key: String): PrefixRecord = this.get(key)
}
