package org.dbpedia.quad.solr;

/**
 * General KG SOLR Exception
 * 
 * @author kay
 *
 */
public class KgSolrException extends Exception {

	/**
	 * 
	 */
	
	public KgSolrException(final String message) {
		super(message);
	}
	
	public KgSolrException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
