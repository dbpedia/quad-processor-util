package org.dbpedia.quad.solr;

public class Field{
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

    void setDynamic(Boolean dynamic) {
        this.dynamic = dynamic;
    }

    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    void setType(String type) {
        this.type = type;
    }

    public String getProperty() {
        return property;
    }

    void setProperty(String property) {
        this.property = property;
    }

    public Boolean isIndexed() {
        return indexed;
    }

    void setIndexed(Boolean indexed) {
        this.indexed = indexed;
    }

    public Boolean isStored() {
        return stored;
    }

    void setStored(Boolean stored) {
        this.stored = stored;
    }

    public Boolean isRequired() {
        return required;
    }

    void setRequired(Boolean required) {
        this.required = required;
    }

    public Boolean isMultiValued() {
        return multiValued;
    }

    void setMultiValued(Boolean multiValued) {
        this.multiValued = multiValued;
    }

    public Boolean isTermVectors() {
        return termVectors;
    }

    void setTermVectors(Boolean termVectors) {
        this.termVectors = termVectors;
    }

    public Boolean isTermPositions() {
        return termPositions;
    }

    void setTermPositions(Boolean termPositions) {
        this.termPositions = termPositions;
    }

    public Boolean isTermOffsets() {
        return termOffsets;
    }

    void setTermOffsets(Boolean termOffsets) {
        this.termOffsets = termOffsets;
    }
}