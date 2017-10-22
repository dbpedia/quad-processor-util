package org.dbpedia.quad.processing;

import scala.Function1;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

/**
 * Created by chile on 21.10.17.
 */
public abstract class SerializableFunction1<T1,R> extends AbstractFunction1<T1, R> implements Serializable {
    @Override
    public <A> Function1<A, R> compose(Function1<A, T1> g) {
        return super.compose(g);
    }

    @Override
    public <A> Function1<T1, A> andThen(Function1<R, A> g) {
        return super.andThen(g);
    }
}