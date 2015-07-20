/**
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
package org.apache.hadoop.io.erasurecode;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * A EC schema loading utility that loads predefined EC schemas from XML file
 */
public class SchemaLoader {
  private static final Logger LOG = LoggerFactory.getLogger(
      SchemaLoader.class.getName());

  /**
   * Load predefined ec schemas from configuration file. This file is
   * expected to be in the XML format.
   */
  public List<ECSchema> loadSchema(String schemaFilePath) {
    File confFile = getSchemaFile(schemaFilePath);
    if (confFile == null) {
      LOG.warn("Not found any predefined EC schema file");
      return Collections.emptyList();
    }

    try {
      return loadSchema(confFile);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Failed to load schema file: " + confFile);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load schema file: " + confFile);
    } catch (SAXException e) {
      throw new RuntimeException("Failed to load schema file: " + confFile);
    }
  }

  private List<ECSchema> loadSchema(File schemaFile)
      throws ParserConfigurationException, IOException, SAXException {

    LOG.info("Loading predefined EC schema file {}", schemaFile);

    // Read and parse the schema file.
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setIgnoringComments(true);
    DocumentBuilder builder = dbf.newDocumentBuilder();
    Document doc = builder.parse(schemaFile);
    Element root = doc.getDocumentElement();

    if (!"schemas".equals(root.getTagName())) {
      throw new RuntimeException("Bad EC schema config file: " +
          "top-level element not <schemas>");
    }

    NodeList elements = root.getChildNodes();
    List<ECSchema> schemas = new ArrayList<ECSchema>();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        if ("schema".equals(element.getTagName())) {
          ECSchema schema = loadSchema(element);
            schemas.add(schema);
        } else {
          LOG.warn("Bad element in EC schema configuration file: {}",
              element.getTagName());
        }
      }
    }

    return schemas;
  }

  /**
   * Path to the XML file containing predefined ec schemas. If the path is
   * relative, it is searched for in the classpath.
   */
  private File getSchemaFile(String schemaFilePath) {
    File schemaFile = new File(schemaFilePath);
    if (! schemaFile.isAbsolute()) {
      URL url = Thread.currentThread().getContextClassLoader()
          .getResource(schemaFilePath);
      if (url == null) {
        LOG.warn("{} not found on the classpath.", schemaFilePath);
        schemaFile = null;
      } else if (! url.getProtocol().equalsIgnoreCase("file")) {
        throw new RuntimeException(
            "EC predefined schema file " + url +
                " found on the classpath is not on the local filesystem.");
      } else {
        schemaFile = new File(url.getPath());
      }
    }

    return schemaFile;
  }

  /**
   * Loads a schema from a schema element in the configuration file
   */
  private ECSchema loadSchema(Element element) {
    String schemaName = element.getAttribute("name");
    Map<String, String> ecOptions = new HashMap<String, String>();
    NodeList fields = element.getChildNodes();

    for (int i = 0; i < fields.getLength(); i++) {
      Node fieldNode = fields.item(i);
      if (fieldNode instanceof Element) {
        Element field = (Element) fieldNode;
        String tagName = field.getTagName();
        String value = ((Text) field.getFirstChild()).getData().trim();
        ecOptions.put(tagName, value);
      }
    }

    ECSchema schema = new ECSchema(schemaName, ecOptions);
    return schema;
  }
}
