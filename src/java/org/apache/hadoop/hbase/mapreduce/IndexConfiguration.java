/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * Configuration parameters for building a Lucene index.
 */
public class IndexConfiguration extends Configuration {
  
  private static final Log LOG = LogFactory.getLog(IndexConfiguration.class);

  static final String HBASE_COLUMN_NAME = "hbase.column.name";
  static final String HBASE_COLUMN_STORE = "hbase.column.store";
  static final String HBASE_COLUMN_INDEX = "hbase.column.index";
  static final String HBASE_COLUMN_TOKENIZE = "hbase.column.tokenize";
  static final String HBASE_COLUMN_BOOST = "hbase.column.boost";
  static final String HBASE_COLUMN_OMIT_NORMS = "hbase.column.omit.norms";
  static final String HBASE_INDEX_ROWKEY_NAME = "hbase.index.rowkey.name";
  static final String HBASE_INDEX_ANALYZER_NAME = "hbase.index.analyzer.name";
  static final String HBASE_INDEX_MAX_BUFFERED_DOCS =
    "hbase.index.max.buffered.docs";
  static final String HBASE_INDEX_MAX_BUFFERED_DELS =
    "hbase.index.max.buffered.dels";
  static final String HBASE_INDEX_MAX_FIELD_LENGTH =
    "hbase.index.max.field.length";
  static final String HBASE_INDEX_MAX_MERGE_DOCS =
    "hbase.index.max.merge.docs";
  static final String HBASE_INDEX_MERGE_FACTOR = "hbase.index.merge.factor";
  // double ramBufferSizeMB;
  static final String HBASE_INDEX_SIMILARITY_NAME =
    "hbase.index.similarity.name";
  static final String HBASE_INDEX_USE_COMPOUND_FILE =
    "hbase.index.use.compound.file";
  static final String HBASE_INDEX_OPTIMIZE = "hbase.index.optimize";

  public static class ColumnConf extends Properties {

    private static final long serialVersionUID = 7419012290580607821L;

    boolean getBoolean(String name, boolean defaultValue) {
      String valueString = getProperty(name);
      if ("true".equals(valueString))
        return true;
      else if ("false".equals(valueString))
        return false;
      else
        return defaultValue;
    }

    void setBoolean(String name, boolean value) {
      setProperty(name, Boolean.toString(value));
    }

    float getFloat(String name, float defaultValue) {
      String valueString = getProperty(name);
      if (valueString == null)
        return defaultValue;
      try {
        return Float.parseFloat(valueString);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }

    void setFloat(String name, float value) {
      setProperty(name, Float.toString(value));
    }
  }

  private Map<String, ColumnConf> columnMap =
    new ConcurrentHashMap<String, ColumnConf>();

  public Iterator<String> columnNameIterator() {
    return columnMap.keySet().iterator();
  }

  public boolean isIndex(String columnName) {
    return getColumn(columnName).getBoolean(HBASE_COLUMN_INDEX, true);
  }

  public void setIndex(String columnName, boolean index) {
    getColumn(columnName).setBoolean(HBASE_COLUMN_INDEX, index);
  }

  public boolean isStore(String columnName) {
    return getColumn(columnName).getBoolean(HBASE_COLUMN_STORE, false);
  }

  public void setStore(String columnName, boolean store) {
    getColumn(columnName).setBoolean(HBASE_COLUMN_STORE, store);
  }

  public boolean isTokenize(String columnName) {
    return getColumn(columnName).getBoolean(HBASE_COLUMN_TOKENIZE, true);
  }

  public void setTokenize(String columnName, boolean tokenize) {
    getColumn(columnName).setBoolean(HBASE_COLUMN_TOKENIZE, tokenize);
  }

  public float getBoost(String columnName) {
    return getColumn(columnName).getFloat(HBASE_COLUMN_BOOST, 1.0f);
  }

  public void setBoost(String columnName, float boost) {
    getColumn(columnName).setFloat(HBASE_COLUMN_BOOST, boost);
  }

  public boolean isOmitNorms(String columnName) {
    return getColumn(columnName).getBoolean(HBASE_COLUMN_OMIT_NORMS, true);
  }

  public void setOmitNorms(String columnName, boolean omitNorms) {
    getColumn(columnName).setBoolean(HBASE_COLUMN_OMIT_NORMS, omitNorms);
  }

  private ColumnConf getColumn(String columnName) {
    ColumnConf column = columnMap.get(columnName);
    if (column == null) {
      column = new ColumnConf();
      columnMap.put(columnName, column);
    }
    return column;
  }

  public String getAnalyzerName() {
    return get(HBASE_INDEX_ANALYZER_NAME,
        "org.apache.lucene.analysis.standard.StandardAnalyzer");
  }

  public void setAnalyzerName(String analyzerName) {
    set(HBASE_INDEX_ANALYZER_NAME, analyzerName);
  }

  public int getMaxBufferedDeleteTerms() {
    return getInt(HBASE_INDEX_MAX_BUFFERED_DELS, 1000);
  }

  public void setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    setInt(HBASE_INDEX_MAX_BUFFERED_DELS, maxBufferedDeleteTerms);
  }

  public int getMaxBufferedDocs() {
    return getInt(HBASE_INDEX_MAX_BUFFERED_DOCS, 10);
  }

  public void setMaxBufferedDocs(int maxBufferedDocs) {
    setInt(HBASE_INDEX_MAX_BUFFERED_DOCS, maxBufferedDocs);
  }

  public int getMaxFieldLength() {
    return getInt(HBASE_INDEX_MAX_FIELD_LENGTH, Integer.MAX_VALUE);
  }

  public void setMaxFieldLength(int maxFieldLength) {
    setInt(HBASE_INDEX_MAX_FIELD_LENGTH, maxFieldLength);
  }

  public int getMaxMergeDocs() {
    return getInt(HBASE_INDEX_MAX_MERGE_DOCS, Integer.MAX_VALUE);
  }

  public void setMaxMergeDocs(int maxMergeDocs) {
    setInt(HBASE_INDEX_MAX_MERGE_DOCS, maxMergeDocs);
  }

  public int getMergeFactor() {
    return getInt(HBASE_INDEX_MERGE_FACTOR, 10);
  }

  public void setMergeFactor(int mergeFactor) {
    setInt(HBASE_INDEX_MERGE_FACTOR, mergeFactor);
  }

  public String getRowkeyName() {
    return get(HBASE_INDEX_ROWKEY_NAME, "ROWKEY");
  }

  public void setRowkeyName(String rowkeyName) {
    set(HBASE_INDEX_ROWKEY_NAME, rowkeyName);
  }

  public String getSimilarityName() {
    return get(HBASE_INDEX_SIMILARITY_NAME, null);
  }

  public void setSimilarityName(String similarityName) {
    set(HBASE_INDEX_SIMILARITY_NAME, similarityName);
  }

  public boolean isUseCompoundFile() {
    return getBoolean(HBASE_INDEX_USE_COMPOUND_FILE, false);
  }

  public void setUseCompoundFile(boolean useCompoundFile) {
    setBoolean(HBASE_INDEX_USE_COMPOUND_FILE, useCompoundFile);
  }

  public boolean doOptimize() {
    return getBoolean(HBASE_INDEX_OPTIMIZE, true);
  }

  public void setDoOptimize(boolean doOptimize) {
    setBoolean(HBASE_INDEX_OPTIMIZE, doOptimize);
  }

  public void addFromXML(String content) {
    try {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance()
          .newDocumentBuilder();

      Document doc = builder
          .parse(new ByteArrayInputStream(content.getBytes()));

      Element root = doc.getDocumentElement();
      if (!"configuration".equals(root.getTagName())) {
        LOG.fatal("bad conf file: top-level element not <configuration>");
      }

      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) {
          continue;
        }

        Element prop = (Element) propNode;
        if ("property".equals(prop.getTagName())) {
          propertyFromXML(prop, null);
        } else if ("column".equals(prop.getTagName())) {
          columnConfFromXML(prop);
        } else {
          LOG.warn("bad conf content: element neither <property> nor <column>");
        }
      }
    } catch (Exception e) {
      LOG.fatal("error parsing conf content: " + e);
      throw new RuntimeException(e);
    }
  }

  private void propertyFromXML(Element prop, Properties properties) {
    NodeList fields = prop.getChildNodes();
    String attr = null;
    String value = null;

    for (int j = 0; j < fields.getLength(); j++) {
      Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }

      Element field = (Element) fieldNode;
      if ("name".equals(field.getTagName())) {
        attr = ((Text) field.getFirstChild()).getData();
      }
      if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
        value = ((Text) field.getFirstChild()).getData();
      }
    }

    if (attr != null && value != null) {
      if (properties == null) {
        set(attr, value);
      } else {
        properties.setProperty(attr, value);
      }
    }
  }

  private void columnConfFromXML(Element column) {
    ColumnConf columnConf = new ColumnConf();
    NodeList props = column.getChildNodes();
    for (int i = 0; i < props.getLength(); i++) {
      Node propNode = props.item(i);
      if (!(propNode instanceof Element)) {
        continue;
      }

      Element prop = (Element) propNode;
      if ("property".equals(prop.getTagName())) {
        propertyFromXML(prop, columnConf);
      } else {
        LOG.warn("bad conf content: element not <property>");
      }
    }

    if (columnConf.getProperty(HBASE_COLUMN_NAME) != null) {
      columnMap.put(columnConf.getProperty(HBASE_COLUMN_NAME), columnConf);
    } else {
      LOG.warn("bad column conf: name not specified");
    }
  }

  public void write(OutputStream out) {
    try {
      Document doc = writeDocument();
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Document writeDocument() {
    Iterator<Map.Entry<String, String>> iter = iterator();
    try {
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .newDocument();
      Element conf = doc.createElement("configuration");
      doc.appendChild(conf);
      conf.appendChild(doc.createTextNode("\n"));

      Map.Entry<String, String> entry;
      while (iter.hasNext()) {
        entry = iter.next();
        String name = entry.getKey();
        String value = entry.getValue();
        writeProperty(doc, conf, name, value);
      }

      Iterator<String> columnIter = columnNameIterator();
      while (columnIter.hasNext()) {
        writeColumn(doc, conf, columnIter.next());
      }

      return doc;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void writeProperty(Document doc, Element parent, String name,
      String value) {
    Element propNode = doc.createElement("property");
    parent.appendChild(propNode);

    Element nameNode = doc.createElement("name");
    nameNode.appendChild(doc.createTextNode(name));
    propNode.appendChild(nameNode);

    Element valueNode = doc.createElement("value");
    valueNode.appendChild(doc.createTextNode(value));
    propNode.appendChild(valueNode);

    parent.appendChild(doc.createTextNode("\n"));
  }

  private void writeColumn(Document doc, Element parent, String columnName) {
    Element column = doc.createElement("column");
    parent.appendChild(column);
    column.appendChild(doc.createTextNode("\n"));

    ColumnConf columnConf = getColumn(columnName);
    for (Map.Entry<Object, Object> entry : columnConf.entrySet()) {
      if (entry.getKey() instanceof String
          && entry.getValue() instanceof String) {
        writeProperty(doc, column, (String) entry.getKey(), (String) entry
            .getValue());
      }
    }
  }

  @Override
  public String toString() {
    StringWriter writer = new StringWriter();
    try {
      Document doc = writeDocument();
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(writer);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return writer.toString();
  }
}