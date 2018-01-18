package org.dbpedia.quad.solr;


import org.apache.solr.common.SolrInputDocument;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class can be used to create a SOLR document
 * which stores information about URIs to label + type information
 * 
 * @author kay
 *
 */
public class SolrUriInputDocument implements KgSorlInputDocument {
	
	/** this is the URI to which this document is mapped to */
	private final String id;
	
	/** actual document which will be send to SOLR */
	private SolrInputDocument solrDocument = new SolrInputDocument();

	private SolrSchema schema;

	public SolrUriInputDocument(final String id) {
		this.id = id;
	}

    public SolrUriInputDocument(final String id, final SolrSchema schema) {
        this.id = id;
        this.schema = schema;
    }

	@Override
	public String getId() {
		return this.id;
	}
	
	/**
	 * Add field data to the SOLR document
	 * 
	 * @param fieldName
	 * @param fieldData
	 */
	public void addFieldData(final String fieldName, final Object fieldData) {
        addFieldData(fieldName, fieldData, 1f);
	}

	public void addFieldData(final String fieldName, final Object fieldData, final float boost) {
		if (null == fieldName || null == fieldData) {
			return;
		}
		Field f = null;
		if(schema != null) {
            f = schema.getField(fieldName);
            if (f == null)
                throw new IllegalArgumentException("The schema does not have a field called " + fieldName);

            if (f.isMultiValued()) {
                addFieldData(fieldName, Collections.singletonList(fieldData), boost);
                return;
            }

            if (this.solrDocument.getFieldValue(fieldName) != null)
                throw new IllegalArgumentException("This field already has a value:" + fieldName);
        }
		if(f != null && f.getType().equals("string") && testFieldSize(f, fieldData))
			return;

		this.solrDocument.addField(fieldName, fieldData, boost);
	}
	
	/**
	 * Add field data to multi-value field
	 * 
	 * @param fieldName
	 * @param fieldDataCollection
	 */
    public void addFieldData(final String fieldName, final Collection<Object> fieldDataCollection) {
        addFieldData(fieldName, fieldDataCollection, 1f);
    }

	public void addFieldData(final String fieldName, final Collection<Object> fieldDataCollection, final float boost) {
		if (null == fieldName || null == fieldDataCollection || fieldDataCollection.isEmpty()) {
			return;
		}

		Field f = null;
		if(schema != null) {
            f = schema.getField(fieldName);
            if (f == null)
                throw new IllegalArgumentException("The schema does not have a field called " + fieldName);

            if (!f.isMultiValued())
                throw new IllegalArgumentException("This field is not defined as 'multivalued': " + fieldName);
        }

        if(testFieldSize(f, fieldDataCollection))
        	return;

		ArrayList<Object> zw = new ArrayList<>(fieldDataCollection);
		int size = zw.size();
		while (testFieldSize(f, zw)) {
			zw = new ArrayList<>(zw.subList(0, size));
			size--;
		}
		this.solrDocument.addField(fieldName, zw, boost);
	}

	private boolean testFieldSize(Field f, Object fieldData) {
		if(f != null && fieldData != null && f.getType().equals("string")){
            int currentLength = 0;
            if(fieldData instanceof Collection<?>)
				currentLength = ((Collection<String>) fieldData).stream()
                    .filter(Objects::nonNull)
                    .map(x -> x.getBytes().length)
                    .reduce((x,y) -> x+y)
                    .orElse(0);
            else
            	currentLength = fieldData.toString().getBytes().length;
            //fields of type string cant have more bytes!
            if(currentLength > 32766)
				return true;
        }
		return false;
	}

	public Object getFieldData(String fieldName){
        if(schema!= null && schema.getField(fieldName).isMultiValued())
            return this.solrDocument.getFieldValues(fieldName);
        else
            return this.solrDocument.getFieldValue(fieldName);
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

		Object fieldValue = this.solrDocument.getFieldValue(fieldName);
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
			return Collections.emptyList();
		}

		Object fieldValuesObject = this.solrDocument.getFieldValue(fieldName);
		if (null != fieldValuesObject) {

			if (fieldValuesObject instanceof List<?>) {
				return (List<String>) fieldValuesObject;
			}

			return Arrays.asList(fieldValuesObject).stream().map(Object::toString).collect(Collectors.toList());
		}

		return Collections.emptyList();
	}
	
	
	public boolean hasChildDocuments() {
		return this.solrDocument.hasChildDocuments();
	}
	
	@Override
	public SolrInputDocument getSolrInputDocument() {
		return this.solrDocument;
	}	
}

