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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXSource;
import java.io.StringReader;

/**
 * Parse XML and get GPU device information
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GpuDeviceInformationParser {
  private static final Logger LOG = LoggerFactory.getLogger(
      GpuDeviceInformationParser.class);
  public static final String GPU_SCRIPT_REFERENCE = "GPU device detection " +
      "script";

  private final Unmarshaller unmarshaller;
  private final XMLReader xmlReader;

  public GpuDeviceInformationParser() throws YarnException {
    try {
      final SAXParserFactory parserFactory = initSaxParserFactory();
      final JAXBContext jaxbContext = JAXBContext.newInstance(
          GpuDeviceInformation.class);
      this.xmlReader = parserFactory.newSAXParser().getXMLReader();
      this.unmarshaller = jaxbContext.createUnmarshaller();
    } catch (Exception e) {
      String msg = "Exception while initializing parser for " +
          GPU_SCRIPT_REFERENCE;
      LOG.error(msg, e);
      throw new YarnException(msg, e);
    }
  }

  /**
   * Disable external-dtd since by default nvidia-smi output contains
   * &lt;!DOCTYPE nvidia_smi_log SYSTEM "nvsmi_device_v8.dtd"> in header.
   */
  private SAXParserFactory initSaxParserFactory() throws Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    spf.setFeature(
        "http://apache.org/xml/features/nonvalidating/load-external-dtd",
        false);
    spf.setFeature("http://xml.org/sax/features/validation", false);
    return spf;
  }

  public synchronized GpuDeviceInformation parseXml(String xmlContent)
      throws YarnException {
    InputSource inputSource = new InputSource(new StringReader(xmlContent));
    SAXSource source = new SAXSource(xmlReader, inputSource);
    try {
      return (GpuDeviceInformation) unmarshaller.unmarshal(source);
    } catch (JAXBException e) {
      String msg = "Failed to parse XML output of " +
          GPU_SCRIPT_REFERENCE + "!";
      LOG.error(msg, e);
      throw new YarnException(msg, e);
    }
  }
}
