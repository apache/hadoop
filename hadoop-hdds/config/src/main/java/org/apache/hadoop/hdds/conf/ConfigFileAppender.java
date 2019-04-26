/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.conf;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Simple DOM based config file writer.
 * <p>
 * This class can init/load existing ozone-default-generated.xml fragments
 * and append new entries and write to the file system.
 */
public class ConfigFileAppender {

  private Document document;

  private final DocumentBuilder builder;

  public ConfigFileAppender() {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      builder = factory.newDocumentBuilder();
    } catch (Exception ex) {
      throw new ConfigurationException("Can initialize new configuration", ex);
    }
  }

  /**
   * Initialize a new ozone-site.xml structure with empty content.
   */
  public void init() {
    try {
      document = builder.newDocument();
      document.appendChild(document.createElement("configuration"));
    } catch (Exception ex) {
      throw new ConfigurationException("Can initialize new configuration", ex);
    }
  }

  /**
   * Load existing ozone-site.xml content and parse the DOM tree.
   */
  public void load(InputStream stream) {
    try {
      document = builder.parse(stream);
    } catch (Exception ex) {
      throw new ConfigurationException("Can't load existing configuration", ex);
    }
  }

  /**
   * Add configuration fragment.
   */
  public void addConfig(String key, String defaultValue, String description,
      ConfigTag[] tags) {
    Element root = document.getDocumentElement();
    Element propertyElement = document.createElement("property");

    addXmlElement(propertyElement, "name", key);

    addXmlElement(propertyElement, "value", defaultValue);

    addXmlElement(propertyElement, "description", description);

    String tagsAsString = Arrays.stream(tags).map(tag -> tag.name())
        .collect(Collectors.joining(", "));

    addXmlElement(propertyElement, "tag", tagsAsString);

    root.appendChild(propertyElement);
  }

  private void addXmlElement(Element parentElement, String tagValue,
      String textValue) {
    Element element = document.createElement(tagValue);
    element.appendChild(document.createTextNode(textValue));
    parentElement.appendChild(element);
  }

  /**
   * Write out the XML content to a writer.
   */
  public void write(Writer writer) {
    try {
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transf = transformerFactory.newTransformer();

      transf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
      transf.setOutputProperty(OutputKeys.INDENT, "yes");
      transf
          .setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

      transf.transform(new DOMSource(document), new StreamResult(writer));
    } catch (TransformerException e) {
      throw new ConfigurationException("Can't write the configuration xml", e);
    }
  }
}
