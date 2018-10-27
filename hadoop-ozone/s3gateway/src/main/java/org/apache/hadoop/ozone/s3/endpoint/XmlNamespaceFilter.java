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
package org.apache.hadoop.ozone.s3.endpoint;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * SAX filter to force namespace usage.
 * <p>
 * This filter will read the XML content as namespace qualified content
 * independent from the current namespace usage.
 */
public class XmlNamespaceFilter extends XMLFilterImpl {

  private String namespace;

  /**
   * Create the filter.
   *
   * @param namespace to add to every elements.
   */
  public XmlNamespaceFilter(String namespace) {
    this.namespace = namespace;
  }

  @Override
  public void startElement(String uri, String localName, String qName,
      Attributes atts) throws SAXException {
    super.startElement(namespace, localName, qName, atts);
  }

  @Override
  public void endElement(String uri, String localName, String qName)
      throws SAXException {
    super.endElement(namespace, localName, qName);
  }
}
