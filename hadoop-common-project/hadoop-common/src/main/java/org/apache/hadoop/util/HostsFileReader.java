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

package org.apache.hadoop.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
// namenode/jobtracker.
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class HostsFileReader {
  private Set<String> includes;
  // exclude host list with optional timeout.
  // If the value is null, it indicates default timeout.
  private Map<String, Integer> excludes;
  private String includesFile;
  private String excludesFile;
  private WriteLock writeLock;
  private ReadLock readLock;
  
  private static final Log LOG = LogFactory.getLog(HostsFileReader.class);

  public HostsFileReader(String inFile, 
                         String exFile) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashMap<String, Integer>();
    includesFile = inFile;
    excludesFile = exFile;
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.writeLock = rwLock.writeLock();
    this.readLock = rwLock.readLock();
    refresh();
  }

  @Private
  public HostsFileReader(String includesFile, InputStream inFileInputStream,
      String excludesFile, InputStream exFileInputStream) throws IOException {
    includes = new HashSet<String>();
    excludes = new HashMap<String, Integer>();
    this.includesFile = includesFile;
    this.excludesFile = excludesFile;
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.writeLock = rwLock.writeLock();
    this.readLock = rwLock.readLock();
    refresh(inFileInputStream, exFileInputStream);
  }

  public static void readFileToSet(String type,
      String filename, Set<String> set) throws IOException {
    File file = new File(filename);
    FileInputStream fis = new FileInputStream(file);
    readFileToSetWithFileInputStream(type, filename, fis, set);
  }

  @Private
  public static void readFileToSetWithFileInputStream(String type,
      String filename, InputStream fileInputStream, Set<String> set)
      throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(
          new InputStreamReader(fileInputStream, StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodes[i].trim();
            if (nodes[i].startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!nodes[i].isEmpty()) {
              LOG.info("Adding a node \"" + nodes[i] + "\" to the list of "
                  + type + " hosts from " + filename);
              set.add(nodes[i]);
            }
          }
        }
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
      fileInputStream.close();
    }
  }

  public void refresh() throws IOException {
    this.writeLock.lock();
    try {
      refresh(includesFile, excludesFile);
    } finally {
      this.writeLock.unlock();
    }
  }

  public static void readFileToMap(String type,
      String filename, Map<String, Integer> map) throws IOException {
    File file = new File(filename);
    FileInputStream fis = new FileInputStream(file);
    readFileToMapWithFileInputStream(type, filename, fis, map);
  }

  public static void readFileToMapWithFileInputStream(String type,
      String filename, InputStream inputStream, Map<String, Integer> map)
          throws IOException {
    // The input file could be either simple text or XML.
    boolean xmlInput = filename.toLowerCase().endsWith(".xml");
    if (xmlInput) {
      readXmlFileToMapWithFileInputStream(type, filename, inputStream, map);
    } else {
      HashSet<String> nodes = new HashSet<String>();
      readFileToSetWithFileInputStream(type, filename, inputStream, nodes);
      for (String node : nodes) {
        map.put(node, null);
      }
    }
  }

  public static void readXmlFileToMapWithFileInputStream(String type,
      String filename, InputStream fileInputStream, Map<String, Integer> map)
          throws IOException {
    Document dom;
    DocumentBuilderFactory builder = DocumentBuilderFactory.newInstance();
    try {
      DocumentBuilder db = builder.newDocumentBuilder();
      dom = db.parse(fileInputStream);
      // Examples:
      // <host><name>host1</name></host>
      // <host><name>host2</name><timeout>123</timeout></host>
      // <host><name>host3</name><timeout>-1</timeout></host>
      // <host><name>host4, host5,host6</name><timeout>1800</timeout></host>
      Element doc = dom.getDocumentElement();
      NodeList nodes = doc.getElementsByTagName("host");
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);
        if (node.getNodeType() == Node.ELEMENT_NODE) {
          Element e= (Element) node;
          // Support both single host and comma-separated list of hosts.
          String v = readFirstTagValue(e, "name");
          String[] hosts = StringUtils.getTrimmedStrings(v);
          String str = readFirstTagValue(e, "timeout");
          Integer timeout = (str == null)? null : Integer.parseInt(str);
          for (String host : hosts) {
            map.put(host, timeout);
            LOG.info("Adding a node \"" + host + "\" to the list of "
                + type + " hosts from " + filename);
          }
        }
      }
    } catch (IOException|SAXException|ParserConfigurationException e) {
      LOG.fatal("error parsing " + filename, e);
      throw new RuntimeException(e);
    } finally {
      fileInputStream.close();
    }
  }

  static String readFirstTagValue(Element e, String tag) {
    NodeList nodes = e.getElementsByTagName(tag);
    return (nodes.getLength() == 0)? null : nodes.item(0).getTextContent();
  }

  public void refresh(String includeFiles, String excludeFiles)
      throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    this.writeLock.lock();
    try {
      // update instance variables
      updateFileNames(includeFiles, excludeFiles);
      Set<String> newIncludes = new HashSet<String>();
      Map<String, Integer> newExcludes = new HashMap<String, Integer>();
      boolean switchIncludes = false;
      boolean switchExcludes = false;
      if (includeFiles != null && !includeFiles.isEmpty()) {
        readFileToSet("included", includeFiles, newIncludes);
        switchIncludes = true;
      }
      if (excludeFiles != null && !excludeFiles.isEmpty()) {
        readFileToMap("excluded", excludeFiles, newExcludes);
        switchExcludes = true;
      }

      if (switchIncludes) {
        // switch the new hosts that are to be included
        includes = newIncludes;
      }
      if (switchExcludes) {
        // switch the excluded hosts
        excludes = newExcludes;
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Private
  public void refresh(InputStream inFileInputStream,
      InputStream exFileInputStream) throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    this.writeLock.lock();
    try {
      Set<String> newIncludes = new HashSet<String>();
      Map<String, Integer> newExcludes = new HashMap<String, Integer>();
      boolean switchIncludes = false;
      boolean switchExcludes = false;
      if (inFileInputStream != null) {
        readFileToSetWithFileInputStream("included", includesFile,
            inFileInputStream, newIncludes);
        switchIncludes = true;
      }
      if (exFileInputStream != null) {
        readFileToMapWithFileInputStream("excluded", excludesFile,
            exFileInputStream, newExcludes);
        switchExcludes = true;
      }
      if (switchIncludes) {
        // switch the new hosts that are to be included
        includes = newIncludes;
      }
      if (switchExcludes) {
        // switch the excluded hosts
        excludes = newExcludes;
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  public Set<String> getHosts() {
    this.readLock.lock();
    try {
      return includes;
    } finally {
      this.readLock.unlock();
    }
  }

  public Set<String> getExcludedHosts() {
    this.readLock.lock();
    try {
      return excludes.keySet();
    } finally {
      this.readLock.unlock();
    }
  }

  public void getHostDetails(Set<String> includes, Set<String> excludes) {
    this.readLock.lock();
    try {
      includes.addAll(this.includes);
      excludes.addAll(this.excludes.keySet());
    } finally {
      this.readLock.unlock();
    }
  }

  public void getHostDetails(Set<String> includeHosts,
                             Map<String, Integer> excludeHosts) {
    this.readLock.lock();
    try {
      includeHosts.addAll(this.includes);
      excludeHosts.putAll(this.excludes);
    } finally {
      this.readLock.unlock();
    }
  }

  public void setIncludesFile(String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    this.includesFile = includesFile;
  }
  
  public void setExcludesFile(String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    this.excludesFile = excludesFile;
  }

  public void updateFileNames(String includeFiles, String excludeFiles) {
    this.writeLock.lock();
    try {
      setIncludesFile(includeFiles);
      setExcludesFile(excludeFiles);
    } finally {
      this.writeLock.unlock();
    }
  }
}
