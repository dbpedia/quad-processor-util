package org.dbpedia.quad.solr;

import org.apache.solr.common.SolrInputDocument;

/**
 * This is a general information for solr documents
 * 
 * @author kay
 *
 */
public interface KgSorlInputDocument {
	
	/**
	 * 
	 * @return unique ID of this SOLR document
	 */
	public String getId();
	
	/**
	 * This method can be used to obtain the generated SOLR input document
	 * 
	 * @return solr input document which contains the provided information
	 */
	public SolrInputDocument getSolrInputDocument();

}
