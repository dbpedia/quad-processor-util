package org.dbpedia.quad.solr;


import org.apache.solr.common.SolrInputDocument;

import java.util.Collection;

/**
 * This class can be used to create a SOLR document
 * which stores information about URIs to label + type information
 * 
 * @author kay
 *
 */
public class SolrUriInputDocument implements KgSorlInputDocument {
	
	/** this is the URI to which this document is mapped to */
	final protected String uri;
	
	/** actual document which will be send to SOLR */
	protected SolrInputDocument solrDocument = new SolrInputDocument();
	
	public SolrUriInputDocument(final String uri) {
		this.uri = uri;
	}
	
	@Override
	public String getId() {
		return this.uri;
	}
	
	/**
	 * Add field data to the SOLR document
	 * 
	 * @param fieldName
	 * @param fieldData
	 */
	public void addFieldData(final String fieldName, final Object fieldData) {
		if (null == fieldName || null == fieldData) {
			return;
		}
		
		this.solrDocument.addField(fieldName, fieldData);
	}
	
	/**
	 * Add field data to multi-value field
	 * 
	 * @param fieldName
	 * @param fieldDataCollection
	 */
	public void addFieldData(final String fieldName, final Collection<Object> fieldDataCollection) {
		if (null == fieldName || null == fieldDataCollection || fieldDataCollection.isEmpty()) {
			return;
		}
		
		for (Object fieldData : fieldDataCollection) {
			this.solrDocument.addField(fieldName, fieldData);
		}
	}
	
	
	/**
	 * This method can be used to add child documents
	 * 
	 * @param doc - child doc
	 */
	public void addChildDocument(final KgSorlInputDocument doc) {
		SolrInputDocument childDoc = doc.getSolrInputDocument();
		this.solrDocument.addChildDocument(childDoc);
	}
	
	
	public boolean hasChildDocuments() {
		return this.solrDocument.hasChildDocuments();
	}
	
	@Override
	public SolrInputDocument getSolrInputDocument() {
		return this.solrDocument;
	}	
}

