package org.dbpedia.quad.sort

import java.util.Comparator

import org.apache.commons.collections4.trie.PatriciaTrie
import org.dbpedia.quad.sort.QuadSorter.PrefixRecord

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
      this.put(prefix, new PrefixRecord(prefix, this.size, charMap, redirect, split))
    else{
      val old = this.get(prefix)
      for((key, value) <- old.charMap){
        charMap.get(key) match{
          case Some(i) => charMap.put(key, value + i)
          case None => charMap.put(key, value)
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

  def getLongestPrefix(uri: String, skip: String = null): PrefixRecord ={
    var currentPrefix = if(skip != null) skip else ""
    if(skip == null){
      while(this.get(currentPrefix) == null && currentPrefix.length < uri.length)
        currentPrefix = uri.substring(0, currentPrefix.length+1)
    }
    var record: PrefixRecord = this.get(currentPrefix)
    while(record == null || record.split){
      currentPrefix = uri.substring(0, currentPrefix.length+1)
      record = this.get(currentPrefix)
    }
    record
  }

  def isContainedIn(prefix: String): List[PrefixRecord]={
    this.headMap(prefix).asScala.filter(x => x._2.redirect.isEmpty && !x._2.split).values.toList
  }

  /**
    * redirects prefixes if necessary
    * @param prefix
    */
  def resolvePrefix(prefix: String): (String) => PrefixRecord ={
    Option(this.get(prefix)) match{
      case Some(p) => p.redirect match{
        case Some(r) => resolvePrefix(r)
        case None => if(p.split)
          (resource: String) => {
            if(!resource.startsWith(p.prefix))
              throw new IllegalArgumentException("Incompatible resource provided: " + resource + " for prefix " + p.prefix)
            val newPrefix = p.prefix + resource.substring(p.prefix.length, p.prefix.length+1)
            this.get(newPrefix)
          }
          else
            (_: String) => p
      }
      case None => throw new IllegalStateException("Prefix was not found: " + prefix)  //should not happen
    }
  }

  def clearSplitPrefixes(): Unit ={
    this.asScala.filter(x => x._2.split).keys.foreach(p => this.remove(p))
  }

  def apply(key: String): PrefixRecord = this.get(key)
}
