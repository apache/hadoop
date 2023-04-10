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

package org.apache.hadoop.fs.azurebfs;

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.INVALID_XML;

/**
 * Parses the response inputSteam and populates an object of {@link BlobList}. Parsing
 * creates a list of {@link BlobProperty}.<br>
 * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs?tabs=azure-ad#response-body">
 * BlobList API XML response example</a>
 */
public class BlobListXmlParser extends DefaultHandler {
  /**
   * Object that contains the parsed response.
   */
  private final BlobList blobList;
  private final String url;
  /**
   * {@link BlobProperty} for which at a given moment, the parsing is going on.
   */
  private BlobProperty currentBlobProperty;
  /**
   * Maintains the value in a given XML-element.
   */
  private StringBuilder bld = new StringBuilder();
  /**
   * Maintains the stack of XML-elements in memory at a given moment.
   */
  private final Stack<String> elements = new Stack<>();

  /**
   * Set an object of {@link BlobList} to populate from the parsing.
   * Set the url for which GetBlobList API is called.
   */
  public BlobListXmlParser(final BlobList blobList, final String url) {
    this.blobList = blobList;
    this.url = url;
  }

  /**
   * <pre>Receive notification of the start of an element.</pre>
   * If the xml start tag is "Blob", it defines that a new BlobProperty information
   * is going to be parsed.
   */
  @Override
  public void startElement(final String uri,
      final String localName,
      final String qName,
      final Attributes attributes) throws SAXException {
    elements.push(localName);
    if (AbfsHttpConstants.BLOB.equals(localName)) {
      currentBlobProperty = new BlobProperty();
    }
  }

  /**
   * <pre>Receive notification of the end of an element.</pre>
   * Whenever an XML-tag is closed, the parent-tag and current-tag shall be
   * checked and correct property shall be set in the active {@link #currentBlobProperty}.
   * If the current-tag is "Blob", it means that there are no more properties to
   * be set in the  the active {@link #currentBlobProperty}, and it shall be the
   * {@link #blobList}.
   */
  @Override
  public void endElement(final String uri,
      final String localName,
      final String qName)
      throws SAXException {
    String currentNode = elements.pop();
    /*
     * Check if the ending tag is correct to the starting tag in the stack.
     */
    if (!currentNode.equals(localName)) {
      throw new SAXException(INVALID_XML);
    }
    String parentNode = "";
    if (elements.size() > 0) {
      parentNode = elements.peek();
    }

    String value = bld.toString();
    if (value.isEmpty()) {
      value = null;
    }

    /*
     * If the closing tag is Blob, there are no more properties to be set in
     * currentBlobProperty.
     */
    if (AbfsHttpConstants.BLOB.equals(currentNode)) {
      blobList.addBlobProperty(currentBlobProperty);
      currentBlobProperty = null;
    }

    if (AbfsHttpConstants.NEXT_MARKER.equals(currentNode)) {
      blobList.setNextMarker(value);
    }

    if (parentNode.equals(AbfsHttpConstants.BLOB_PREFIX)) {
      if (currentNode.equals(AbfsHttpConstants.NAME)) {
        currentBlobProperty.setBlobPrefix(value);
      }
    }
    /*
     * For case:
     * <Blob>
     * <Name>value</name>
     * ....</Blob>
     */
    if (parentNode.equals(AbfsHttpConstants.BLOB)) {
      if (currentNode.equals(AbfsHttpConstants.NAME)) {
        currentBlobProperty.setName(value);
        currentBlobProperty.setPath(new Path("/" + value));
        currentBlobProperty.setUrl(url + "/" + value);
      }
    }
    /*
     * For case:
     * <Blob>...<Metadata>
     * <key1>value</key1>...<keyN>value</keyN></Metadata>
     * ....</Blob>
     * ParentNode will be Metadata for all key1, key2, ... , keyN.
     */
    if (parentNode.equals(AbfsHttpConstants.METADATA)) {
      currentBlobProperty.addMetadata(currentNode, value);
      if (HDI_ISFOLDER.equals(currentNode)) {
        currentBlobProperty.setIsDirectory(Boolean.valueOf(value));
      }
    }
    /*
     * For case:
     * <Blob>...<Properties>
     * <Content-Length>value</Content-Length><ResourceType>value</ResourceType></Metadata>
     * ....</Blob>
     * ParentNode will be Properties for Content-Length, ResourceType.
     */
    if (parentNode.equals(AbfsHttpConstants.PROPERTIES)) {
      if (currentNode.equals(AbfsHttpConstants.CONTENT_LEN)) {
        currentBlobProperty.setContentLength(Long.valueOf(value));
      }
      if (currentNode.equals(AbfsHttpConstants.RESOURCE_TYPE)) {
        if (DIRECTORY.equals(value)) {
          currentBlobProperty.setIsDirectory(true);
        }
      }
    }
    /*
     * refresh bld for the next XML-tag value
     */
    bld = new StringBuilder();
  }

  /**
   * Receive notification of character data inside an element. No heuristics to
   * apply. Just append the {@link #bld}.
   */
  @Override
  public void characters(final char[] ch, final int start, final int length)
      throws SAXException {
    bld.append(ch, start, length);
  }
}