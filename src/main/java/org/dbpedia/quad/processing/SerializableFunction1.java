package org.dbpedia.quad.processing;

import scala.Serializable;
import scala.runtime.AbstractFunction1;

/**
 * Created by chile on 21.10.17.
 */
public abstract class SerializableFunction1<T1,R> extends AbstractFunction1<T1, R> implements Serializable {
}