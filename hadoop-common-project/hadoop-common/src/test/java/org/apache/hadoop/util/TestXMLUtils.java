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

import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.SAXParser;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TestXMLUtils extends AbstractHadoopTestBase {

  @Test
  public void testSecureDocumentBuilderFactory() throws Exception {
    DocumentBuilder db = XMLUtils.newSecureDocumentBuilderFactory().newDocumentBuilder();
    Document doc = db.parse(new InputSource(new StringReader("<root/>")));
    Assertions.assertThat(doc).describedAs("parsed document").isNotNull();
  }

  @Test(expected = SAXException.class)
  public void testExternalDtdWithSecureDocumentBuilderFactory() throws Exception {
    DocumentBuilder db = XMLUtils.newSecureDocumentBuilderFactory().newDocumentBuilder();
    try (InputStream stream = getResourceStream("/xml/external-dtd.xml")) {
      Document doc = db.parse(stream);
    }
  }

  @Test(expected = SAXException.class)
  public void testEntityDtdWithSecureDocumentBuilderFactory() throws Exception {
    DocumentBuilder db = XMLUtils.newSecureDocumentBuilderFactory().newDocumentBuilder();
    try (InputStream stream = getResourceStream("/xml/entity-dtd.xml")) {
      Document doc = db.parse(stream);
    }
  }

  @Test
  public void testSecureSAXParserFactory() throws Exception {
    SAXParser parser = XMLUtils.newSecureSAXParserFactory().newSAXParser();
    parser.parse(new InputSource(new StringReader("<root/>")), new DefaultHandler());
  }

  @Test(expected = SAXException.class)
  public void testExternalDtdWithSecureSAXParserFactory() throws Exception {
    SAXParser parser = XMLUtils.newSecureSAXParserFactory().newSAXParser();
    try (InputStream stream = getResourceStream("/xml/external-dtd.xml")) {
      parser.parse(stream, new DefaultHandler());
    }
  }

  @Test(expected = SAXException.class)
  public void testEntityDtdWithSecureSAXParserFactory() throws Exception {
    SAXParser parser = XMLUtils.newSecureSAXParserFactory().newSAXParser();
    try (InputStream stream = getResourceStream("/xml/entity-dtd.xml")) {
      parser.parse(stream, new DefaultHandler());
    }
  }

  @Test
  public void testSecureTransformerFactory() throws Exception {
    Transformer transformer = XMLUtils.newSecureTransformerFactory().newTransformer();
    DocumentBuilder db = XMLUtils.newSecureDocumentBuilderFactory().newDocumentBuilder();
    Document doc = db.parse(new InputSource(new StringReader("<root/>")));
    try (StringWriter stringWriter = new StringWriter()) {
      transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
      Assertions.assertThat(stringWriter.toString()).contains("<root");
    }
  }

  @Test(expected = TransformerException.class)
  public void testExternalDtdWithSecureTransformerFactory() throws Exception {
    Transformer transformer = XMLUtils.newSecureTransformerFactory().newTransformer();
    try (
        InputStream stream = getResourceStream("/xml/external-dtd.xml");
        StringWriter stringWriter = new StringWriter()
    ) {
      transformer.transform(new StreamSource(stream), new StreamResult(stringWriter));
    }
  }

  @Test
  public void testSecureSAXTransformerFactory() throws Exception {
    Transformer transformer = XMLUtils.newSecureSAXTransformerFactory().newTransformer();
    DocumentBuilder db = XMLUtils.newSecureDocumentBuilderFactory().newDocumentBuilder();
    Document doc = db.parse(new InputSource(new StringReader("<root/>")));
    try (StringWriter stringWriter = new StringWriter()) {
      transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
      Assertions.assertThat(stringWriter.toString()).contains("<root");
    }
  }

  @Test(expected = TransformerException.class)
  public void testExternalDtdWithSecureSAXTransformerFactory() throws Exception {
    Transformer transformer = XMLUtils.newSecureSAXTransformerFactory().newTransformer();
    try (
        InputStream stream = getResourceStream("/xml/external-dtd.xml");
        StringWriter stringWriter = new StringWriter()
    ) {
      transformer.transform(new StreamSource(stream), new StreamResult(stringWriter));
    }
  }

  @Test
  public void testBestEffortSetAttribute() throws Exception {
    TransformerFactory factory = TransformerFactory.newInstance();
    AtomicBoolean flag1 = new AtomicBoolean(true);
    XMLUtils.bestEffortSetAttribute(factory, flag1, "unsupportedAttribute false", "abc");
    Assert.assertFalse("unexpected attribute results in return of false?", flag1.get());
    AtomicBoolean flag2 = new AtomicBoolean(true);
    XMLUtils.bestEffortSetAttribute(factory, flag2, XMLConstants.ACCESS_EXTERNAL_DTD, "");
    Assert.assertTrue("expected attribute results in return of true?", flag2.get());
    AtomicBoolean flag3 = new AtomicBoolean(false);
    XMLUtils.bestEffortSetAttribute(factory, flag3, XMLConstants.ACCESS_EXTERNAL_DTD, "");
    Assert.assertFalse("expected attribute results in return of false if input flag is false?",
            flag3.get());
  }

  private static InputStream getResourceStream(final String filename) {
    return TestXMLUtils.class.getResourceAsStream(filename);
  }
}
