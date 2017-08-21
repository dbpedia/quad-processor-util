package org.dbpedia.quad.solr;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedDismaxQParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;

/**
 * Created by chile on 15.08.17.
 */
public class PayloadParserPlugin extends QParserPlugin {

    @Override
    public QParser createParser(String s, SolrParams solrParams, SolrParams solrParams1, SolrQueryRequest solrQueryRequest) {
        return new PayloadQParser(s, solrParams, solrParams1, solrQueryRequest);
    }

    @Override
    public void init(NamedList namedList) {

    }
}

class PayloadQParser extends ExtendedDismaxQParser {
    PayloadQueryParser pqParser;

    public PayloadQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        super(qstr, localParams, params, req);
    }

    // This is kind of tricky. The deal here is that you do NOT
    // want to get into all the process of parsing parentheses,
    // operators like AND/OR/NOT/+/- etc, it's difficult. So we'll
    // let the default parsing do all this for us.
    // Eventually the complex logic will resolve to asking for
    // fielded query, which we define in the PayloadQueryParser
    // below.
    @Override
    public Query parse() throws SyntaxError {
        return super.parse();
    }

    @Override
    public String[] getDefaultHighlightFields() {
        return pqParser == null ? new String[]{} :
                new String[] {pqParser.getDefaultField()};
    }

    @Override
    protected ExtendedSolrQueryParser createEdismaxQueryParser(QParser qParser, String field) {
        return new PayloadQueryParser(qParser, field);
    }
}


// Here's the tricky bit. You let the methods defined in the
// superclass do the heavy lifting, parsing all the
// parentheses/AND/OR/NOT/+/- whatever. Then, eventually, when
// all that's resolved down to a field and a term, and
// BOOM, you're here at the simple "getFieldQuery" call.
// NOTE: this is not suitable for phrase queries, the limitation
// here is that we're only evaluating payloads for
// queries that can resolve to combinations of single word
// fielded queries.
class PayloadQueryParser extends ExtendedDismaxQParser.ExtendedSolrQueryParser {
    PayloadQueryParser(QParser parser, String defaultField) {
        super(parser, defaultField);
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, boolean quoted) throws SyntaxError {
        //SchemaField sf = this.schema.getFieldOrNull(field);
        // Note that this will work for any field defined with the
        // <fieldType> of "payloads", not just the field "payloads".
        // One could easily parameterize this in the config files to
        // avoid hard-coding the values.

        //if(sf != null)
        //    throw new RuntimeException(sf.getType().getTypeName());
        throw new RuntimeException(field);
        //if (field.equalsIgnoreCase("redirectsText_phrase")) {
            //return new PayloadTermQuery(new Term(field, queryText), new AveragePayloadFunction(), false);
        //}
        //return super.getFieldQuery(field, queryText, quoted);
    }
}