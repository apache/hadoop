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

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * SAX parser for Azure Blob Storage "List Containers" REST API response.
 *
 * <p>
 * Parses the XML response returned by:
 * https://learn.microsoft.com/en-us/rest/api/storageservices/list-containers2
 * </p>
 *
 * <p>
 * This parser is streaming (SAX-based) to avoid loading the full XML into memory.
 * </p>
 */
public class ContainerListXmlParser extends DefaultHandler {

  /** Parsed response object */
  private final ContainerListResponseData responseData;

  /** Stack of active XML elements */
  private final Stack<String> elements = new Stack<>();

  /** Buffer for character data */
  private StringBuilder buffer = new StringBuilder();

  /** Currently parsed container entry */
  private ContainerListEntrySchema currentContainer;

  /**
   * Constructs a parser for List Containers response.
   *
   * @param responseData response object to populate
   */
  public ContainerListXmlParser(
      final ContainerListResponseData responseData) {
    this.responseData = responseData;
  }

  /**
   * {@inheritDoc}
   *
   * @param uri namespace URI
   * @param localName local element name
   * @param qName qualified name
   * @param attributes element attributes
   */
  @Override
  public void startElement(
      final String uri,
      final String localName,
      final String qName,
      final Attributes attributes) {
    elements.push(localName);
    if (AbfsHttpConstants.XML_TAG_CONTAINER.equals(localName)) {
      currentContainer = new ContainerListEntrySchema();
    }
  }

  /**
   * SAX handler implementation for parsing container list responses.
   * <p>
   * This method is invoked at the end of every XML element encountered
   * during SAX parsing. It validates element structure and delegates
   * processing to appropriate helper methods depending on whether the
   * element belongs to a container, container properties, metadata,
   * or top-level response fields.
   */
  @Override
  public void endElement(
      final String uri,
      final String localName,
      final String qName) throws SAXException {

    final String currentNode = elements.pop();
    // Validate XML structure
    if (!currentNode.equals(localName)) {
      throw new SAXException(AbfsHttpConstants.XML_TAG_INVALID_XML);
    }
    final String parentNode =
        elements.isEmpty() ? EMPTY_STRING : elements.peek();
    final String value = getTrimmedValue();
    // Handle container-related nodes
    if (currentContainer != null) {
      handleContainerNode(currentNode, parentNode, value);
    } else {
      // Handle response-level nodes
      handleResponseNode(currentNode, value);
    }
    // Reset character buffer
    buffer = new StringBuilder();
  }

  /**
   * Returns the trimmed character buffer value.
   *
   * @return trimmed string value or {@code null} if empty
   */
  private String getTrimmedValue() {
    String value = buffer.toString().trim();
    return value.isEmpty() ? null : value;
  }

  /**
   * Processes XML nodes related to a container element.
   *
   * @param currentNode the current XML element name
   * @param parentNode  the parent XML element name
   * @param value       the trimmed element value
   */
  private void handleContainerNode(
      String currentNode,
      String parentNode,
      String value) {
    // Container closing tag (must be under <Containers>)
    if (AbfsHttpConstants.XML_TAG_CONTAINER.equals(currentNode)
        && AbfsHttpConstants.XML_TAG_CONTAINERS.equals(parentNode)) {
      responseData.addContainer(currentContainer);
      currentContainer = null;
      return;
    }
    // Direct container fields (must be under <Container>)
    if (AbfsHttpConstants.XML_TAG_NAME.equals(currentNode)
        && AbfsHttpConstants.XML_TAG_CONTAINER.equals(parentNode)) {
      currentContainer.setName(value);
      return;
    }
    if (AbfsHttpConstants.XML_TAG_VERSION.equals(currentNode)
        && AbfsHttpConstants.XML_TAG_CONTAINER.equals(parentNode)) {
      currentContainer.setVersion(value);
      return;
    }
    if (AbfsHttpConstants.XML_TAG_DELETED.equals(currentNode)
        && AbfsHttpConstants.XML_TAG_CONTAINER.equals(parentNode)) {
      currentContainer.setDeleted(Boolean.parseBoolean(value));
      return;
    }
    // Properties section
    if (AbfsHttpConstants.XML_TAG_PROPERTIES.equals(parentNode)) {
      handleContainerProperties(currentNode, value);
      return;
    }
    // Metadata section
    if (AbfsHttpConstants.XML_TAG_METADATA.equals(parentNode)) {
      currentContainer.addMetadata(currentNode, value);
    }
  }

  /**
   * Handles container property fields under the &lt;Properties&gt; XML node.
   *
   * @param currentNode property XML tag name
   * @param value       property value (may be null)
   */
  private void handleContainerProperties(
      String currentNode,
      String value) {
    switch (currentNode) {
    case AbfsHttpConstants.XML_TAG_LAST_MODIFIED_TIME:
      if (value != null) {
        currentContainer.setLastModified(
            DateTimeUtils.parseLastModifiedTime(value));
      }
      break;
    case AbfsHttpConstants.XML_TAG_ETAG:
      currentContainer.setETag(value);
      break;
    case AbfsHttpConstants.XML_TAG_LEASE_STATUS:
      currentContainer.setLeaseStatus(value);
      break;
    case AbfsHttpConstants.XML_TAG_LEASE_STATE:
      currentContainer.setLeaseState(value);
      break;
    case AbfsHttpConstants.XML_TAG_LEASE_DURATION:
      currentContainer.setLeaseDuration(value);
      break;
    case AbfsHttpConstants.XML_TAG_PUBLIC_ACCESS:
      currentContainer.setPublicAccess(value);
      break;
    case AbfsHttpConstants.XML_TAG_HAS_IMMUTABILITY_POLICY:
      currentContainer.setHasImmutabilityPolicy(
          Boolean.parseBoolean(value));
      break;
    case AbfsHttpConstants.XML_TAG_HAS_LEGAL_HOLD:
      currentContainer.setHasLegalHold(
          Boolean.parseBoolean(value));
      break;
    case AbfsHttpConstants.XML_TAG_DELETED_TIME:
      if (value != null) {
        currentContainer.setDeletedTime(
            DateTimeUtils.parseLastModifiedTime(value));
      }
      break;
    case AbfsHttpConstants.XML_TAG_REMAINING_RETENTION_DAYS:
      if (value != null) {
        currentContainer.setRemainingRetentionDays(
            Integer.parseInt(value));
      }
      break;
    default:
      // Unknown property tag — safely ignore
      break;
    }
  }

  /**
   * Processes top-level response fields outside container elements.
   *
   * @param currentNode XML element name
   * @param value       element value (may be null)
   */
  private void handleResponseNode(
      String currentNode,
      String value) {
    switch (currentNode) {
    case AbfsHttpConstants.XML_TAG_PREFIX:
      responseData.setPrefix(value);
      break;
    case AbfsHttpConstants.XML_TAG_MARKER:
      responseData.setMarker(value);
      break;
    case AbfsHttpConstants.XML_TAG_MAX_RESULTS:
      if (value != null) {
        responseData.setMaxResults(Integer.parseInt(value));
      }
      break;
    case AbfsHttpConstants.XML_TAG_NEXT_MARKER:
      responseData.setContinuationToken(value);
      break;
    default:
      // Ignore unrelated nodes
      break;
    }
  }

  /**
   * {@inheritDoc}
   *
   * Accumulates character data for the current XML element during parsing.
   */
  @Override
  public void characters(
      final char[] ch,
      final int start,
      final int length) throws SAXException {
    buffer.append(ch, start, length);
  }
}
