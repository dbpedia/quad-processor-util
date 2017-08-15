package org.dbpedia.quad.solr;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Created by chile on 15.08.17.
 */
public class PayloadAnalyzer extends Analyzer {
    private PayloadEncoder encoder = new FloatEncoder();

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new StandardTokenizer();
        TokenStream stream = new LowerCaseFilter(tokenizer);
        //stream = new PorterStemFilter(stream);
        return new TokenStreamComponents(tokenizer, stream);
    }
}
