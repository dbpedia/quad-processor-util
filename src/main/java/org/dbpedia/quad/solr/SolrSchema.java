package org.dbpedia.quad.solr;

import com.sun.tools.javac.util.Assert;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by chile on 11.10.17.
 */
public class SolrSchema {

    private XPathFactory xPathfactory = XPathFactory.newInstance();
    private DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    private XPathExpression fieldPath = xPathfactory.newXPath().compile("/schema/field");

    private Document schema;
    private HashMap<String, Field> fields = new HashMap<>();

    public SolrSchema(File schemaFile) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {
        DocumentBuilder builder = factory.newDocumentBuilder();
        schema = builder.parse(schemaFile);
        loadFields();
    }

    public Set<String> getFieldNames(){
        return fields.keySet();
    }

    public Field getField(String name){
        return fields.get(name);
    }

    private void loadFields() throws XPathExpressionException {
        NodeList nodes = (NodeList)fieldPath.evaluate(schema, XPathConstants.NODESET);
        for(int i =0; i < nodes.getLength(); i++){
            Node n = nodes.item(i);
            NamedNodeMap attributes = n.getAttributes();
            Node name = attributes.getNamedItem("name");
            Node type = attributes.getNamedItem("type");
            Assert.checkNonNull(name, "A field in the schema file has no name attribute!");
            Assert.checkNonNull(type, "A field in the schema file has no type attribute!");

            Field field = new Field(name.getNodeValue(), type.getNodeValue());
            if(attributes.getNamedItem("property") != null)
                field.setProperty(attributes.getNamedItem("property").getNodeValue());
            if(attributes.getNamedItem("indexed") != null)
                field.setIndexed(Boolean.parseBoolean(attributes.getNamedItem("indexed").getNodeValue()));
            if(attributes.getNamedItem("stored") != null)
                field.setStored(Boolean.parseBoolean(attributes.getNamedItem("stored").getNodeValue()));
            if(attributes.getNamedItem("required") != null)
                field.setRequired(Boolean.parseBoolean(attributes.getNamedItem("required").getNodeValue()));
            if(attributes.getNamedItem("multiValued") != null)
                field.setMultiValued(Boolean.parseBoolean(attributes.getNamedItem("multiValued").getNodeValue()));
            if(attributes.getNamedItem("termVectors") != null)
                field.setTermVectors(Boolean.parseBoolean(attributes.getNamedItem("termVectors").getNodeValue()));
            if(attributes.getNamedItem("termPositions") != null)
                field.setTermPositions(Boolean.parseBoolean(attributes.getNamedItem("termPositions").getNodeValue()));
            if(attributes.getNamedItem("termOffsets") != null)
                field.setTermOffsets(Boolean.parseBoolean(attributes.getNamedItem("termOffsets").getNodeValue()));

            if(n.getLocalName().equals("dynamicField")){
                field.setDynamic(true);
            }

            fields.put(field.getName(), field);
        }
    }

    class Field{
        private String name;
        private String type;
        private String property = null;
        private Boolean indexed = true;
        private Boolean stored= true;
        private Boolean dynamic = false;
        private Boolean required= false;
        private Boolean multiValued= false;
        private Boolean termVectors= false;
        private Boolean termPositions= false;
        private Boolean termOffsets= false;

        public Field(String name, String type){
            this.name = name;
            this.type = type;
        }

        public Boolean isDynamic() {
            return dynamic;
        }

        private void setDynamic(Boolean dynamic) {
            this.dynamic = dynamic;
        }

        public String getName() {
            return name;
        }

        private void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        private void setType(String type) {
            this.type = type;
        }

        public String getProperty() {
            return property;
        }

        private void setProperty(String property) {
            this.property = property;
        }

        public Boolean isIndexed() {
            return indexed;
        }

        private void setIndexed(Boolean indexed) {
            this.indexed = indexed;
        }

        public Boolean isStored() {
            return stored;
        }

        private void setStored(Boolean stored) {
            this.stored = stored;
        }

        public Boolean isRequired() {
            return required;
        }

        private void setRequired(Boolean required) {
            this.required = required;
        }

        public Boolean isMultiValued() {
            return multiValued;
        }

        private void setMultiValued(Boolean multiValued) {
            this.multiValued = multiValued;
        }

        public Boolean isTermVectors() {
            return termVectors;
        }

        private void setTermVectors(Boolean termVectors) {
            this.termVectors = termVectors;
        }

        public Boolean isTermPositions() {
            return termPositions;
        }

        private void setTermPositions(Boolean termPositions) {
            this.termPositions = termPositions;
        }

        public Boolean isTermOffsets() {
            return termOffsets;
        }

        private void setTermOffsets(Boolean termOffsets) {
            this.termOffsets = termOffsets;
        }
    }
}
