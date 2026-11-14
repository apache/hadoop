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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX XML parser for parsing Blob Layout XML responses into {@link BlobLayoutResponse} objects.
 * <p>
 * This parser handles XML elements such as <Range>, <Endpoint>, <NextMarker>, and <MaxResults>,
 * and populates a {@link BlobLayoutResponse} instance accordingly. It is designed to be used
 * with an XML parser that supports SAX event handling.
 * </p>
 *
 * <p>Usage example:</p>
 * <pre>
 *   BlobLayoutXmlParser handler = new BlobLayoutXmlParser();
 *   SAXParserFactory factory = SAXParserFactory.newInstance();
 *   SAXParser parser = factory.newSAXParser();
 *   parser.parse(xmlInputStream, handler);
 *   BlobLayoutResponse response = handler.getResponse();
 * </pre>
 */
public class BlobLayoutXmlParser extends DefaultHandler {

  /**
   * The BlobLayoutResponse object that will be populated by this parser.
   */
  private final BlobLayoutResponse response = new BlobLayoutResponse();

  /**
   * Buffer for accumulating character data between XML tags.
   */
  private final StringBuilder textBuffer = new StringBuilder();

  /**
   * Returns the parsed BlobLayoutResponse after XML parsing is complete.
   *
   * @return the populated BlobLayoutResponse
   */
  public BlobLayoutResponse getResponse() {
    return response;
  }

  /**
   * Handles the start of an XML element. Resets the text buffer and processes
   * <Range> and <Endpoint> elements by extracting their attributes and adding
   * them to the response.
   *
   * @param uri the Namespace URI
   * @param localName the local name (without prefix)
   * @param qName the qualified name (with prefix)
   * @param attributes the attributes attached to the element
   */
  @Override
  public void startElement(String uri,
      String localName,
      String qName,
      Attributes attributes) {

    textBuffer.setLength(0); // reset text buffer

    switch (qName) {
      case "Range" -> {
        BlobLayoutResponse.Range r = new BlobLayoutResponse.Range(
                Long.parseLong(attributes.getValue("Start")),
                Long.parseLong(attributes.getValue("End")),
                Integer.parseInt(attributes.getValue("EndpointIndex"))
        );
        response.addRange(r);
      }
      case "Endpoint" -> {
        BlobLayoutResponse.Endpoint e = new BlobLayoutResponse.Endpoint(
                Integer.parseInt(attributes.getValue("Index")),
                attributes.getValue("Value"));
        response.addEndpoint(e);
      }
      default -> {
        // No handling required for other elements.
      }
    }
  }

  /**
   * Handles character data between XML tags. Appends the characters to the text buffer.
   *
   * @param ch the characters
   * @param start the start position in the character array
   * @param length the number of characters to use from the character array
   */
  @Override
  public void characters(char[] ch, int start, int length) {
    textBuffer.append(ch, start, length);
  }

  /**
   * Handles the end of an XML element. For <NextMarker> and <MaxResults> elements,
   * sets the corresponding values in the response using the accumulated text buffer.
   *
   * @param uri the Namespace URI
   * @param localName the local name (without prefix)
   * @param qName the qualified name (with prefix)
   */
  @Override
  public void endElement(String uri, String localName, String qName) {

    String text = textBuffer.toString().trim();

    switch (qName) {
    case "NextMarker" -> response.setNextMarker(text);   // will be "" if empty
    case "MaxResults" -> response.setMaxResults(text);
    default -> { }
    }
  }
}
