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

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * Parses the response inputSteam and populates an object of {@link BlobListResultSchema}. Parsing
 * creates a list of {@link BlobListResultEntrySchema}.<br>
 * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs?tabs=azure-ad#response-body">
 * BlobList API XML response example</a>
 * <pre>
 *   {@code
 *   <EnumerationResults ServiceEndpoint="http://myaccount.blob.core.windows.net/" ContainerName="mycontainer">
 *    <Prefix>string-value</Prefix>
 *    <Marker>string-value</Marker>
 *    <MaxResults>int-value</MaxResults>
 *    <Delimiter>string-value</Delimiter>
 *    <Blobs>
 *      <Blob> </Blob>
 *      <BlobPrefix> </BlobPrefix>
 *    </Blobs>
 *    <NextMarker />
 *   </EnumerationResults>
 *   }
 * </pre>
 */


public class BlobListXmlParser extends DefaultHandler {
  /**
   * Object that contains the parsed response.
   */
  private final BlobListResultSchema listResultSchema;
  private final String url;
  /**
   * {@link BlobListResultEntrySchema} for which at a given moment, the parsing is going on.
   *
   * Following XML elements will be parsed and added in currentBlobEntry.
   * 1. Blob: for explicit files and directories
   * <Blob>
   *   <Name>blob-name</name>
   *   <Properties> </Properties>
   *   <Metadata>
   *     <Name>value</Name>
   *   </Metadata>
   * </Blob>
   *
   * 2. BlobPrefix: for directories both explicit and implicit
   * <BlobPrefix>
   *   <Name>blob-prefix</Name>
   * </BlobPrefix>
   */
  private BlobListResultEntrySchema currentBlobEntry;
  /**
   * Maintains the value in a given XML-element.
   */
  private StringBuilder bld = new StringBuilder();
  /**
   * Maintains the stack of XML-elements in memory at a given moment.
   */
  private final Stack<String> elements = new Stack<>();

  /**
   * Set an object of {@link BlobListResultSchema} to populate from the parsing.
   * Set the url for which GetBlobList API is called.
   * @param listResultSchema Object to populate from the parsing.
   * @param url URL for which GetBlobList API is called.
   */
  public BlobListXmlParser(final BlobListResultSchema listResultSchema, final String url) {
    this.listResultSchema = listResultSchema;
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
    if (AbfsHttpConstants.XML_TAG_BLOB.equals(localName) || AbfsHttpConstants.XML_TAG_BLOB_PREFIX.equals(localName)) {
      currentBlobEntry = new BlobListResultEntrySchema();
    }
  }

  /**
   * <pre>Receive notification of the end of an element.</pre>
   * Whenever an XML-tag is closed, the parent-tag and current-tag shall be
   * checked and correct property shall be set in the active {@link #currentBlobEntry}.
   * If the current-tag is "Blob", it means that there are no more properties to
   * be set in the  the active {@link #currentBlobEntry}, and it shall be the
   * {@link #listResultSchema}.
   */
  @Override
  public void endElement(final String uri,
      final String localName,
      final String qName)
      throws SAXException {
    String currentNode = elements.pop();

    // Check if the ending tag is correct to the starting tag in the stack.
    if (!currentNode.equals(localName)) {
      throw new SAXException(AbfsHttpConstants.XML_TAG_INVALID_XML);
    }

    String parentNode = EMPTY_STRING;
    if (elements.size() > 0) {
      parentNode = elements.peek();
    }

    String value = bld.toString();
    if (value.isEmpty()) {
      value = null;
    }

    /*
     * If the closing tag is Blob, there are no more properties to be set in
     * currentBlobEntry.
     */
    if (AbfsHttpConstants.XML_TAG_BLOB.equals(currentNode)) {
      listResultSchema.addBlobListEntry(currentBlobEntry);
      currentBlobEntry = null;
    }

    /*
     * If the closing tag is BlobPrefix, there are no more properties to be set in
     * currentBlobEntry and this is a directory (implicit or explicit)
     * If implicit, it will be added only once/
     * If explicit it will be added with Blob Tag as well.
     */
    if (AbfsHttpConstants.XML_TAG_BLOB_PREFIX.equals(currentNode)) {
      currentBlobEntry.setIsDirectory(true);
      listResultSchema.addBlobListEntry(currentBlobEntry);
      currentBlobEntry = null;
    }

    /*
     * If the closing tag is Next Marker, it needs to be saved with the
     * list of blobs currently fetched
     */
    if (AbfsHttpConstants.XML_TAG_NEXT_MARKER.equals(currentNode)) {
      listResultSchema.setNextMarker(value);
    }

    /*
     * If the closing tag is Name, then it is either for a blob
     * or for a blob prefix denoting a directory. We will save this
     * in current BlobProperty for both
     */
    if (currentNode.equals(AbfsHttpConstants.XML_TAG_NAME)
        && (parentNode.equals(AbfsHttpConstants.XML_TAG_BLOB)
        || parentNode.equals(AbfsHttpConstants.XML_TAG_BLOB_PREFIX))) {
      // In case of BlobPrefix Name will have a slash at the end
      // Remove the "/" at the end of name
      if (value.endsWith(AbfsHttpConstants.FORWARD_SLASH)) {
        value = value.substring(0, value.length() - 1);
      }

      currentBlobEntry.setName(value);
      currentBlobEntry.setPath(new Path(AbfsHttpConstants.ROOT_PATH + value));
      currentBlobEntry.setUrl(url + AbfsHttpConstants.ROOT_PATH + value);
    }

    /*
     * For case:
     * <Blob>
     * ...
     *   <Metadata>
     *     <key1>value</key1>
     *     <keyN>value</keyN>
     *   </Metadata>
     *  ...
     * </Blob>
     * ParentNode will be Metadata for all key1, key2, ... , keyN.
     */
    if (parentNode.equals(AbfsHttpConstants.XML_TAG_METADATA)) {
      currentBlobEntry.addMetadata(currentNode, value);
      // For Marker blobs hdi_isFolder will be present as metadata
      if (AbfsHttpConstants.XML_TAG_HDI_ISFOLDER.equals(currentNode)) {
        currentBlobEntry.setIsDirectory(Boolean.valueOf(value));
      }
    }

    /*
     * For case:
     * <Blob>
     * ...
     *   <Properties>
     *     <Creation-Time>date-time-value</Creation-Time>
     *     <Last-Modified>date-time-value</Last-Modified>
     *     <Etag>Etag</Etag>
     *     <Owner>owner user id</Owner>
     *     <Group>owning group id</Group>
     *     <Permissions>permission string</Permissions>
     *     <Acl>access control list</Acl>
     *     <ResourceType>file | directory</ResourceType>
     *     <Content-Length>size-in-bytes</Content-Length>
     *     <CopyId>id</CopyId>
     *     <CopyStatus>pending | success | aborted | failed </CopyStatus>
     *     <CopySource>source url</CopySource>
     *     <CopyProgress>bytes copied/bytes total</CopyProgress>
     *     <CopyCompletionTime>datetime</CopyCompletionTime>
     *     <CopyStatusDescription>error string</CopyStatusDescription>
     *   </Properties>
     *  ...
     * </Blob>
     * ParentNode will be Properties for Content-Length, ResourceType.
     */
    if (parentNode.equals(AbfsHttpConstants.XML_TAG_PROPERTIES)) {
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_CREATION_TIME)) {
        currentBlobEntry.setCreationTime(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_LAST_MODIFIED_TIME)) {
        currentBlobEntry.setLastModifiedTime(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_ETAG)) {
        currentBlobEntry.setETag(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_OWNER)) {
        currentBlobEntry.setOwner(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_GROUP)) {
        currentBlobEntry.setGroup(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_PERMISSIONS)) {
        currentBlobEntry.setPermission(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_ACL)) {
        currentBlobEntry.setAcl(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_RESOURCE_TYPE)) {
        if (AbfsHttpConstants.DIRECTORY.equals(value)) {
          currentBlobEntry.setIsDirectory(true);
        }
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_CONTENT_LEN)) {
        currentBlobEntry.setContentLength(Long.parseLong(value));
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_COPY_ID)) {
        currentBlobEntry.setCopyId(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_COPY_STATUS)) {
        currentBlobEntry.setCopyStatus(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_COPY_SOURCE)) {
        currentBlobEntry.setCopySourceUrl(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_COPY_PROGRESS)) {
        currentBlobEntry.setCopyProgress(value);
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_COPY_COMPLETION_TIME)) {
        currentBlobEntry.setCopyCompletionTime(DateTimeUtils.parseLastModifiedTime(value));
      }
      if (currentNode.equals(AbfsHttpConstants.XML_TAG_COPY_STATUS_DESCRIPTION)) {
        currentBlobEntry.setCopyStatusDescription(value);
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
