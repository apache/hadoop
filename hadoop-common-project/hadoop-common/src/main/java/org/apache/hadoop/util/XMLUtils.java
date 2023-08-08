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

package org.apache.hadoop.util;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.*;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * General xml utilities.
 *   
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XMLUtils {

  private static final Logger LOG =
          LoggerFactory.getLogger(XMLUtils.class);

  public static final String DISALLOW_DOCTYPE_DECL =
      "http://apache.org/xml/features/disallow-doctype-decl";
  public static final String LOAD_EXTERNAL_DECL =
      "http://apache.org/xml/features/nonvalidating/load-external-dtd";
  public static final String EXTERNAL_GENERAL_ENTITIES =
      "http://xml.org/sax/features/external-general-entities";
  public static final String EXTERNAL_PARAMETER_ENTITIES =
      "http://xml.org/sax/features/external-parameter-entities";
  public static final String CREATE_ENTITY_REF_NODES =
      "http://apache.org/xml/features/dom/create-entity-ref-nodes";
  public static final String VALIDATION =
      "http://xml.org/sax/features/validation";

  private static final AtomicBoolean CAN_SET_TRANSFORMER_ACCESS_EXTERNAL_DTD =
          new AtomicBoolean(true);
  private static final AtomicBoolean CAN_SET_TRANSFORMER_ACCESS_EXTERNAL_STYLESHEET =
          new AtomicBoolean(true);

  /**
   * Transform input xml given a stylesheet.
   * 
   * @param styleSheet the style-sheet
   * @param xml input xml data
   * @param out output
   * @throws TransformerConfigurationException synopsis signals a problem
   *         creating a transformer object.
   * @throws TransformerException this is used for throwing processor
   *          exceptions before the processing has started.
   */
  public static void transform(
                               InputStream styleSheet, InputStream xml, Writer out
                               ) 
    throws TransformerConfigurationException, TransformerException {
    // Instantiate a TransformerFactory
    TransformerFactory tFactory = newSecureTransformerFactory();

    // Use the TransformerFactory to process the  
    // stylesheet and generate a Transformer
    Transformer transformer = tFactory.newTransformer(
                                                      new StreamSource(styleSheet)
                                                      );

    // Use the Transformer to transform an XML Source 
    // and send the output to a Result object.
    transformer.transform(new StreamSource(xml), new StreamResult(out));
  }

  /**
   * This method should be used if you need a {@link DocumentBuilderFactory}. Use this method
   * instead of {@link DocumentBuilderFactory#newInstance()}. The factory that is returned has
   * secure configuration enabled.
   *
   * @return a {@link DocumentBuilderFactory} with secure configuration enabled
   * @throws ParserConfigurationException if the {@code JAXP} parser does not support the
   * secure configuration
   */
  public static DocumentBuilderFactory newSecureDocumentBuilderFactory()
          throws ParserConfigurationException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    dbf.setFeature(DISALLOW_DOCTYPE_DECL, true);
    dbf.setFeature(LOAD_EXTERNAL_DECL, false);
    dbf.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    dbf.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    dbf.setFeature(CREATE_ENTITY_REF_NODES, false);
    return dbf;
  }

  /**
   * This method should be used if you need a {@link SAXParserFactory}. Use this method
   * instead of {@link SAXParserFactory#newInstance()}. The factory that is returned has
   * secure configuration enabled.
   *
   * @return a {@link SAXParserFactory} with secure configuration enabled
   * @throws ParserConfigurationException if the {@code JAXP} parser does not support the
   * secure configuration
   * @throws SAXException if there are another issues when creating the factory
   */
  public static SAXParserFactory newSecureSAXParserFactory()
          throws SAXException, ParserConfigurationException {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    spf.setFeature(DISALLOW_DOCTYPE_DECL, true);
    spf.setFeature(LOAD_EXTERNAL_DECL, false);
    spf.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    spf.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    return spf;
  }

  /**
   * This method should be used if you need a {@link TransformerFactory}. Use this method
   * instead of {@link TransformerFactory#newInstance()}. The factory that is returned has
   * secure configuration enabled.
   *
   * @return a {@link TransformerFactory} with secure configuration enabled
   * @throws TransformerConfigurationException if the {@code JAXP} transformer does not
   * support the secure configuration
   */
  public static TransformerFactory newSecureTransformerFactory()
          throws TransformerConfigurationException {
    TransformerFactory trfactory = TransformerFactory.newInstance();
    trfactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    setOptionalSecureTransformerAttributes(trfactory);
    return trfactory;
  }

  /**
   * This method should be used if you need a {@link SAXTransformerFactory}. Use this method
   * instead of {@link SAXTransformerFactory#newInstance()}. The factory that is returned has
   * secure configuration enabled.
   *
   * @return a {@link SAXTransformerFactory} with secure configuration enabled
   * @throws TransformerConfigurationException if the {@code JAXP} transformer does not
   * support the secure configuration
   */
  public static SAXTransformerFactory newSecureSAXTransformerFactory()
          throws TransformerConfigurationException {
    SAXTransformerFactory trfactory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
    trfactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    setOptionalSecureTransformerAttributes(trfactory);
    return trfactory;
  }

  /**
   * These attributes are recommended for maximum security but some JAXP transformers do
   * not support them. If at any stage, we fail to set these attributes, then we won't try again
   * for subsequent transformers.
   *
   * @param transformerFactory to update
   */
  private static void setOptionalSecureTransformerAttributes(
          TransformerFactory transformerFactory) {
    bestEffortSetAttribute(transformerFactory, CAN_SET_TRANSFORMER_ACCESS_EXTERNAL_DTD,
            XMLConstants.ACCESS_EXTERNAL_DTD, "");
    bestEffortSetAttribute(transformerFactory, CAN_SET_TRANSFORMER_ACCESS_EXTERNAL_STYLESHEET,
            XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
  }

  /**
   * Set an attribute value on a {@link TransformerFactory}. If the TransformerFactory
   * does not support the attribute, the method just returns <code>false</code> and
   * logs the issue at debug level.
   *
   * @param transformerFactory to update
   * @param flag that indicates whether to do the update and the flag can be set to
   *             <code>false</code> if an update fails
   * @param name of the attribute to set
   * @param value to set on the attribute
   */
  static void bestEffortSetAttribute(TransformerFactory transformerFactory, AtomicBoolean flag,
                                     String name, Object value) {
    if (flag.get()) {
      try {
        transformerFactory.setAttribute(name, value);
      } catch (Throwable t) {
        flag.set(false);
        LOG.debug("Issue setting TransformerFactory attribute {}: {}", name, t.toString());
      }
    }
  }
}
