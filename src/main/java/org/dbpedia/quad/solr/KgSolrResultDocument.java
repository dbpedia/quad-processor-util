package org.dbpedia.quad.solr;

import org.apache.solr.common.SolrDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KgSolrResultDocument {
	
	/** actual SOLR document which was returned by SOLR  */
	final Map<String, Object> solrDocumentValues = new HashMap<>();
	
	public KgSolrResultDocument(final SolrDocument solrDocument) {
		if (null == solrDocument) {
			return;
		}
		
		for (String fieldName : solrDocument.getFieldNames()) {
			Object value = solrDocument.get(fieldName);
			this.solrDocumentValues.put(fieldName, value);
		}		
	}
	
	/**
	 * This method can be used to return a value
	 * as string
	 * 
	 * @param fieldName
	 * @return
	 */
	public String getFieldValueAsString(final String fieldName) {
		if (null == fieldName || fieldName.isEmpty()) {
			return null;
		}
		
		Object fieldValue = this.solrDocumentValues.get(fieldName);
		if (null == fieldValue) {
			return null;
		}
		
		return (String) fieldValue;		
	}
	
	/**
	 * This method can be used to return a value
	 * as a list of strings
	 * 
	 * @param fieldName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<String> getFieldValueAsStringList(final String fieldName) {
		if (null == fieldName || fieldName.isEmpty()) {
			return null;
		}
		
		Object fieldValuesObject = this.solrDocumentValues.get(fieldName);
		if (null != fieldValuesObject) {
			
			if (fieldValuesObject instanceof List<?>) {
				return (List<String>) fieldValuesObject;
			}
			
			String[] fieldValues = (String[]) fieldValuesObject;
			
			List<String> fieldValueList = new ArrayList<>();
			for (String fieldValue : fieldValues) {
				fieldValueList.add(fieldValue);
			}
			
			return fieldValueList;
		}
		
		return null;		
	}
	
	public Object getFieldValueAsObject(final String fieldName) {
		if (null == fieldName || fieldName.isEmpty()) {
			return null;
		}
		
		Object fieldValue = this.solrDocumentValues.get(fieldName);
		return fieldValue;		
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		
		for (String key : solrDocumentValues.keySet()) {
			buffer.append(key);
			buffer.append("' : '");
			buffer.append(solrDocumentValues.get(key));
			buffer.append("'\n");
		}
		
		return buffer.toString();
	}

}
