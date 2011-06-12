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
import java.io.FileNotFoundException;
import java.io.FileInputStream;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Tokenizer that reads tokens from XML file
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XmlTokenizer implements Tokenizer {

  FileInputStream is = null;
  XMLStreamReader in;

  /**
   * XmlTokenizer constructor
   *
   * @param filename input filename
   */
  public XmlTokenizer(String filename) throws IOException {
    XMLInputFactory f = XMLInputFactory.newInstance();
    // FileInputStream is = null;
    try {
      is = new FileInputStream(filename);
      in = f.createXMLStreamReader(is);
    } catch(XMLStreamException e) {
      // if(is != null) { is.close(); }
      throw new IOException("Cannot create XML stream", e);
    } catch(FileNotFoundException e) {
      //if(is != null) { is.close(); }
      throw new IOException("Cannot open input file " + filename, e);
    }
  }

  /**
   * Get next element's value, checks that the element's name
   * is wantedName.
   *
   * @param wantedName a name of node that we are looking for
   */
  private String getNextElementsValue(String wantedName) throws IOException {
    boolean gotSTART_ELEMENT = false;
    try {
      int eventType = in.getEventType();
      while(true) {
        switch(eventType) {
          case XMLStreamConstants.CHARACTERS: // 4
            if(gotSTART_ELEMENT) {
              // XML returns "\n" instead of empty (zero-length) string
              // for elements like <x></x>
              return in.getText().trim();
            }
            break;
          case XMLStreamConstants.END_DOCUMENT: // 8
            throw new IOException("End of XML while looking for element [" +
              wantedName + "]");
            // break;
          case XMLStreamConstants.START_ELEMENT : // 1
            if(gotSTART_ELEMENT) {
              throw new IOException("START_ELEMENT [" +
                in.getName() +
                " event when expecting CHARACTERS event for [" +
                wantedName + "]");
            } else if(in.getName().toString().equals(wantedName)) {
              gotSTART_ELEMENT = true;
            } else {
              throw new IOException("unexpected element name [" +
                in.getName() + "], was expecting [" +
                wantedName + "]");
            }
            break;
          case XMLStreamConstants.COMMENT:
          case XMLStreamConstants.END_ELEMENT: // 2
          case XMLStreamConstants.SPACE:
          case XMLStreamConstants.START_DOCUMENT: // 7
            // these are in XML but we don't need them
            break;
          // these should never appear in edits XML
          case XMLStreamConstants.ATTRIBUTE:
          case XMLStreamConstants.CDATA:
          case XMLStreamConstants.DTD:
          case XMLStreamConstants.ENTITY_DECLARATION:
          case XMLStreamConstants.ENTITY_REFERENCE:
          case XMLStreamConstants.NAMESPACE:
          case XMLStreamConstants.NOTATION_DECLARATION:
          case XMLStreamConstants.PROCESSING_INSTRUCTION:
          default:
            throw new IOException("Unsupported event type [" +
              eventType + "] (see XMLStreamConstants)");
        }
        if(!in.hasNext()) { break; }
        eventType = in.next();
      }
    } catch(XMLStreamException e) {
      throw new IOException("Error reading XML stream", e);
    }
    throw new IOException(
      "Error reading XML stream, should never reach this line, " +
      "most likely XML does not have elements we are loking for");
  }

  /**
   * @see org.apache.hadoop.hdfs.tools.offlineEditsViewer.Tokenizer#read
   *
   * @param t a token to read
   * @return token that was just read
   */
  public Token read(Token t) throws IOException {
    t.fromString(getNextElementsValue(t.getEditsElement().toString()));
    return t;
  }
}
