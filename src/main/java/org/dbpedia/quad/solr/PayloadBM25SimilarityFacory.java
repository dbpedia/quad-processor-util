package org.dbpedia.quad.solr;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.similarities.BM25SimilarityFactory;

/**
 * Created by chile on 15.08.17.
 */
public class PayloadBM25SimilarityFacory extends BM25SimilarityFactory {
    private float k1;
    private float b;
    @Override
    public void init(SolrParams params) {
        super.init(params);
        this.k1 = params.getFloat("k1", 1.2F);
        this.b = params.getFloat("b", 0.75F);
    }

    @Override
    public Similarity getSimilarity() {
        return new PayloadBM25Similarity(this.k1, this.b);
    }
}

class PayloadBM25Similarity extends BM25Similarity {

    public PayloadBM25Similarity(float k1, float b) {
        super(k1, b);
    }

    //Here's where we actually decode the payload and return it.
    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
        if (payload == null) return 1.0F;
        return PayloadHelper.decodeFloat(payload.bytes, payload.offset);
    }

    @Override
    protected byte encodeNormValue(float boost, int fieldLength) {
        return super.encodeNormValue(boost, fieldLength);
    }
}