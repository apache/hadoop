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
  private StringBuilder bld = new StringBuilder();

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

  @Override
  public void startElement(
      final String uri,
      final String localName,
      final String qName,
      final Attributes attributes) throws SAXException {

    elements.push(localName);

    if (AbfsHttpConstants.XML_TAG_CONTAINER.equals(localName)) {
      currentContainer = new ContainerListEntrySchema();
    }
  }

  @Override
  public void endElement(
      final String uri,
      final String localName,
      final String qName) throws SAXException {

    final String currentNode = elements.pop();

    if (!currentNode.equals(localName)) {
      throw new SAXException(AbfsHttpConstants.XML_TAG_INVALID_XML);
    }

    String parentNode = EMPTY_STRING;
    if (!elements.isEmpty()) {
      parentNode = elements.peek();
    }

    String value = bld.toString().trim();
    if (value.isEmpty()) {
      value = null;
    }

    /* ================= Container fields ================= */

    if (currentContainer != null) {

      if (AbfsHttpConstants.XML_TAG_NAME.equals(currentNode)
          && AbfsHttpConstants.XML_TAG_CONTAINER.equals(parentNode)) {
        currentContainer.setName(value);
      }

      if (AbfsHttpConstants.XML_TAG_VERSION.equals(currentNode)) {
        currentContainer.setVersion(value);
      }

      if (AbfsHttpConstants.XML_TAG_DELETED.equals(currentNode)) {
        currentContainer.setDeleted(Boolean.parseBoolean(value));
      }

      /* ================= Properties ================= */

      if (AbfsHttpConstants.XML_TAG_PROPERTIES.equals(parentNode)) {

        if (AbfsHttpConstants.XML_TAG_LAST_MODIFIED_TIME.equals(currentNode)
            && value != null) {
          currentContainer.setLastModified(
              DateTimeUtils.parseLastModifiedTime(value));
        }

        if (AbfsHttpConstants.XML_TAG_ETAG.equals(currentNode)) {
          currentContainer.setETag(value);
        }

        if (AbfsHttpConstants.XML_TAG_LEASE_STATUS.equals(currentNode)) {
          currentContainer.setLeaseStatus(value);
        }

        if (AbfsHttpConstants.XML_TAG_LEASE_STATE.equals(currentNode)) {
          currentContainer.setLeaseState(value);
        }

        if (AbfsHttpConstants.XML_TAG_LEASE_DURATION.equals(currentNode)) {
          currentContainer.setLeaseDuration(value);
        }

        if (AbfsHttpConstants.XML_TAG_PUBLIC_ACCESS.equals(currentNode)) {
          currentContainer.setPublicAccess(value);
        }

        if (AbfsHttpConstants.XML_TAG_HAS_IMMUTABILITY_POLICY.equals(currentNode)) {
          currentContainer.setHasImmutabilityPolicy(Boolean.parseBoolean(value));
        }

        if (AbfsHttpConstants.XML_TAG_HAS_LEGAL_HOLD.equals(currentNode)) {
          currentContainer.setHasLegalHold(Boolean.parseBoolean(value));
        }

        if (AbfsHttpConstants.XML_TAG_DELETED_TIME.equals(currentNode)
            && value != null) {
          currentContainer.setDeletedTime(
              DateTimeUtils.parseLastModifiedTime(value));
        }

        if (AbfsHttpConstants.XML_TAG_REMAINING_RETENTION_DAYS.equals(currentNode)
            && value != null) {
          currentContainer.setRemainingRetentionDays(
              Integer.parseInt(value));
        }
      }

      /* ================= Metadata ================= */

      if (AbfsHttpConstants.XML_TAG_METADATA.equals(parentNode)) {
        currentContainer.addMetadata(currentNode, value);
      }
    }

    /* ================= End Container ================= */

    if (AbfsHttpConstants.XML_TAG_CONTAINER.equals(currentNode)) {
      if (currentContainer != null) {
        responseData.addContainer(currentContainer);
      }
      currentContainer = null;
    }

    /* ================= Enumeration ================= */

    if (AbfsHttpConstants.XML_TAG_PREFIX.equals(currentNode)) {
      responseData.setPrefix(value);
    }

    if (AbfsHttpConstants.XML_TAG_MARKER.equals(currentNode)) {
      responseData.setMarker(value);
    }

    if (AbfsHttpConstants.XML_TAG_MAX_RESULTS.equals(currentNode)
        && value != null) {
      responseData.setMaxResults(Integer.parseInt(value));
    }

    if (AbfsHttpConstants.XML_TAG_NEXT_MARKER.equals(currentNode)) {
      responseData.setContinuationToken(value);
    }

    bld = new StringBuilder();
  }

  @Override
  public void characters(
      final char[] ch,
      final int start,
      final int length) throws SAXException {
    bld.append(ch, start, length);
  }
}
