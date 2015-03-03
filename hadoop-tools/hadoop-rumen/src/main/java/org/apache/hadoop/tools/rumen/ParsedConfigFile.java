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

import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.nio.charset.Charset;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import org.xml.sax.SAXException;

class ParsedConfigFile {
  private static final Pattern jobIDPattern =
      Pattern.compile("_(job_[0-9]+_[0-9]+)_");
  private static final Pattern heapPattern =
      Pattern.compile("-Xmx([0-9]+)([mMgG])");
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  final int heapMegabytes;

  final String queue;
  final String jobName;

  final int clusterMapMB;
  final int clusterReduceMB;
  final int jobMapMB;
  final int jobReduceMB;

  final String jobID;

  final boolean valid;
  
  final Properties properties = new Properties();

  private int maybeGetIntValue(String propName, String attr, String value,
      int oldValue) {
    if (propName.equals(attr) && value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        return oldValue;
      }
    }

    return oldValue;
  }

  @SuppressWarnings("hiding")
  @Deprecated
  ParsedConfigFile(String filenameLine, String xmlString) {
    super();

    int heapMegabytes = -1;

    String queue = null;
    String jobName = null;

    int clusterMapMB = -1;
    int clusterReduceMB = -1;
    int jobMapMB = -1;
    int jobReduceMB = -1;

    String jobID = null;

    boolean valid = true;

    Matcher jobIDMatcher = jobIDPattern.matcher(filenameLine);

    if (jobIDMatcher.find()) {
      jobID = jobIDMatcher.group(1);
    }

    try {
      InputStream is = new ByteArrayInputStream(xmlString.getBytes(UTF_8));

      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

      DocumentBuilder db = dbf.newDocumentBuilder();

      Document doc = db.parse(is);

      Element root = doc.getDocumentElement();

      if (!"configuration".equals(root.getTagName())) {
        System.out.print("root is not a configuration node");
        valid = false;
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
        
        properties.setProperty(attr, value);

        if ("mapred.child.java.opts".equals(attr)) {
          Matcher matcher = heapPattern.matcher(value);
          if (matcher.find()) {
            String heapSize = matcher.group(1);

            heapMegabytes = Integer.parseInt(heapSize);

            if (matcher.group(2).equalsIgnoreCase("G")) {
              heapMegabytes *= 1024;
            }
          }
        }

        if (MRJobConfig.QUEUE_NAME.equals(attr)) {
          queue = value;
        }

        if (MRJobConfig.JOB_NAME.equals(attr)) {
          jobName = value;
        }

        clusterMapMB =
            maybeGetIntValue(MRConfig.MAPMEMORY_MB, attr, value, clusterMapMB);
        clusterReduceMB =
            maybeGetIntValue(MRConfig.REDUCEMEMORY_MB, attr, value,
                clusterReduceMB);
        jobMapMB =
            maybeGetIntValue(MRJobConfig.MAP_MEMORY_MB, attr, value, jobMapMB);
        jobReduceMB =
            maybeGetIntValue(MRJobConfig.REDUCE_MEMORY_MB, attr, value,
                jobReduceMB);
      }

      valid = true;
    } catch (ParserConfigurationException e) {
      valid = false;
    } catch (SAXException e) {
      valid = false;
    } catch (IOException e) {
      valid = false;
    }

    this.heapMegabytes = heapMegabytes;

    this.queue = queue;
    this.jobName = jobName;

    this.clusterMapMB = clusterMapMB;
    this.clusterReduceMB = clusterReduceMB;
    this.jobMapMB = jobMapMB;
    this.jobReduceMB = jobReduceMB;

    this.jobID = jobID;

    this.valid = valid;
  }
}
