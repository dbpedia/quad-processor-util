package org.dbpedia.quad.sort

import org.dbpedia.quad.sort.QuadSorter.PrefixRecord

import scala.collection.mutable

/**
  * Created by chile on 29.08.17.
  */
class PrefixMap extends mutable.Map[String, PrefixRecord]{
  private val prefixMap: mutable.Map[String, PrefixRecord] = mutable.Map[String, PrefixRecord]()


  override def get(key: String): Option[PrefixRecord] = prefixMap.get(key)

  override def iterator: Iterator[(String, PrefixRecord)] = prefixMap.iterator



  def addPrefix(prefix: String, charMap: mutable.Map[Char, Int], redirect: Option[String] = None, split: Boolean = false): Unit = synchronized {
    if(!prefixMap.keySet.contains(prefix))
      prefixMap.put(prefix, new PrefixRecord(prefix, prefixMap.size, charMap))
    else{
      val old = prefixMap(prefix)
      for(oc <- old.charMap){
        charMap.get(oc._1) match{
          case Some(i) => charMap.put(oc._1, oc._2 + i)
          case None => charMap.put(oc._1, oc._2)
        }
      }
      prefixMap.put(prefix, new PrefixRecord(prefix, old.index, charMap, redirect, split))
    }
  }

  def getPrefix(index: Int): String={
    prefixMap.find(x => x._2.index == index-1) match{
      case Some(p) => p._1
      case None => ""
    }
  }

  def getPrefixIndex(prefix: String): Int = prefixMap.get(prefix) match{
    case Some(x) => x.index+1
    case None => 0
  }

  def getPrefixOrder(prefix: String): Int = {
    val comp =new CodePointComparator()
    prefixMap.keys.toList.sortWith((x,y) => comp.compare(x,y) < 0).indexWhere(x => prefix ==x)+1
  }

  def getLongestPrefix(uri: String): String ={
    var prefix = ""
    for(p <- prefixMap.filter(x => x._2.count > 0).keys)
      if(prefix.length < p.length && uri.contains(p))
        prefix = p
    prefix
  }

  /**
    * redirects prefixes if necessary
    * @param prefix
    */
  def resolvePrefix(prefix: String, resource: String): PrefixRecord ={
    prefixMap.get(prefix) match{
      case Some(p) => p.redirect match{
        case Some(r) => resolvePrefix(r, resource)
        case None => if(p.split) resolvePrefix(prefix + resource.trim.substring(prefix.length, prefix.length+1).toUpperCase(), resource) else p
      }
      case None => null  //should not happen
    }
  }

  override def +=(kv: (String, PrefixRecord)): PrefixMap.this.type = {
    prefixMap += kv
    this
  }

  override def -=(key: String): PrefixMap.this.type = {
    prefixMap -= key
    this
  }
}
