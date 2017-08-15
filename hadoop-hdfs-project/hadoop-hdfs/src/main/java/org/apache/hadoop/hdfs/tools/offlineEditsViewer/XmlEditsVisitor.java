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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * An XmlEditsVisitor walks over an EditLog structure and writes out
 * an equivalent XML document that contains the EditLog's components.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XmlEditsVisitor implements OfflineEditsVisitor {
  private final OutputStream out;
  private ContentHandler contentHandler;
  private final SAXTransformerFactory factory;
  private final static String XML_INDENTATION_PROP ="{http://xml.apache.org/" +
          "xslt}indent-amount";
  private final static String XML_INDENTATION_NUM ="2";

  /**
   * Create a processor that writes to the file named and may or may not
   * also output to the screen, as specified.
   *
   * @param out output stream to write
   * @throws IOException on any error
   */
  public XmlEditsVisitor(OutputStream out)
      throws IOException {
    this.out = out;
    factory =(SAXTransformerFactory)SAXTransformerFactory.newInstance();
    try {
      TransformerHandler handler = factory.newTransformerHandler();
      handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "xml");
      handler.getTransformer().setOutputProperty(OutputKeys.ENCODING, "UTF-8");
      handler.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
      handler.getTransformer().setOutputProperty(XML_INDENTATION_PROP,
              XML_INDENTATION_NUM);
      handler.getTransformer().setOutputProperty(OutputKeys.STANDALONE, "yes");
      handler.setResult(new StreamResult(out));
      contentHandler = handler;
      
      contentHandler.startDocument();
      contentHandler.startElement("", "", "EDITS", new AttributesImpl());
    } catch (TransformerConfigurationException e) {
      throw new IOException("SAXTransformer error: " + e.getMessage());
    } catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
  }

  /**
   * Start visitor (initialization)
   */
  @Override
  public void start(int version) throws IOException {
    try {
      contentHandler.startElement("", "", "EDITS_VERSION", new AttributesImpl());
      StringBuilder bld = new StringBuilder();
      bld.append(version);
      addString(bld.toString());
      contentHandler.endElement("", "", "EDITS_VERSION");
    }
    catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
  }

  public void addString(String str) throws SAXException {
    int slen = str.length();
    char arr[] = new char[slen];
    str.getChars(0, slen, arr, 0);
    contentHandler.characters(arr, 0, slen);
  }
  
  /**
   * Finish visitor
   */
  @Override
  public void close(Throwable error) throws IOException {
    try {
      contentHandler.endElement("", "", "EDITS");
      if (error != null) {
        String msg = error.getMessage();
        XMLUtils.addSaxString(contentHandler, "ERROR",
            (msg == null) ? "null" : msg);
      }
      contentHandler.endDocument();
    }
    catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
    out.close();
  }

  @Override
  public void visitOp(FSEditLogOp op) throws IOException {
    try {
      op.outputToXml(contentHandler);
    }
    catch (SAXException e) {
      throw new IOException("SAX error: " + e.getMessage());
    }
  }
}
