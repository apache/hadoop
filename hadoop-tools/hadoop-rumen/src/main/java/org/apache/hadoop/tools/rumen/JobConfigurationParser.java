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
package org.apache.hadoop.tools.rumen;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * {@link JobConfigurationParser} parses the job configuration xml file, and
 * extracts configuration properties. It parses the file using a
 * stream-parser and thus is more memory efficient. [This optimization may be
 * postponed for a future release]
 */
public class JobConfigurationParser {

  /**
   * Parse the job configuration file (as an input stream) and return a
   * {@link Properties} collection. The input stream will not be closed after
   * return from the call.
   * 
   * @param input
   *          The input data.
   * @return A {@link Properties} collection extracted from the job
   *         configuration xml.
   * @throws IOException
   */
  static Properties parse(InputStream input) throws IOException {
    Properties result = new Properties();

    try {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

      DocumentBuilder db = dbf.newDocumentBuilder();

      Document doc = db.parse(input);

      Element root = doc.getDocumentElement();

      if (!"configuration".equals(root.getTagName())) {
        System.out.print("root is not a configuration node");
        return null;
      }

      NodeList props = root.getChildNodes();

      for (int i = 0; i < props.getLength(); ++i) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element))
          continue;
        Element prop = (Element) propNode;
        if (!"property".equals(prop.getTagName())) {
          System.out.print("bad conf file: element not <property>");
        }
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        @SuppressWarnings("unused")
        boolean finalParameter = false;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element)) {
            continue;
          }

          Element field = (Element) fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
            attr = ((Text) field.getFirstChild()).getData().trim();
          }
          if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
            value = ((Text) field.getFirstChild()).getData();
          }
          if ("final".equals(field.getTagName()) && field.hasChildNodes()) {
            finalParameter =
                "true".equals(((Text) field.getFirstChild()).getData());
          }
        }

        if (attr != null && value != null) {
          result.put(attr, value);
        }
      }
    } catch (ParserConfigurationException e) {
      return null;
    } catch (SAXException e) {
      return null;
    }

    return result;
  }
}
