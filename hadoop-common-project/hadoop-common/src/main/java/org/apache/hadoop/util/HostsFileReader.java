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
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

// Keeps track of which datanodes/nodemanagers are allowed to connect to the
// namenode/resourcemanager.
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class HostsFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(HostsFileReader
      .class);

  private final AtomicReference<HostDetails> current;
  private final AtomicReference<HostDetails> lazyLoaded =
      new AtomicReference<>();

  public HostsFileReader(String inFile,
                         String exFile) throws IOException {
    HostDetails hostDetails = new HostDetails(
        inFile, Collections.emptySet(),
        exFile, Collections.emptyMap());
    current = new AtomicReference<>(hostDetails);
    refresh(inFile, exFile);
  }

  @Private
  public HostsFileReader(String includesFile, InputStream inFileInputStream,
      String excludesFile, InputStream exFileInputStream) throws IOException {
    HostDetails hostDetails = new HostDetails(
        includesFile, Collections.emptySet(),
        excludesFile, Collections.emptyMap());
    current = new AtomicReference<>(hostDetails);
    refresh(inFileInputStream, exFileInputStream);
  }

  public static void readFileToSet(String type,
      String filename, Set<String> set) throws IOException {
    File file = new File(filename);
    InputStream fis = Files.newInputStream(file.toPath());
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
    HostDetails hostDetails = current.get();
    refresh(hostDetails.includesFile, hostDetails.excludesFile);
  }

  public static void readFileToMap(String type,
      String filename, Map<String, Integer> map) throws IOException {
    File file = new File(filename);
    InputStream fis = Files.newInputStream(file.toPath());
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
      LOG.error("error parsing " + filename, e);
      throw new RuntimeException(e);
    } finally {
      fileInputStream.close();
    }
  }

  static String readFirstTagValue(Element e, String tag) {
    NodeList nodes = e.getElementsByTagName(tag);
    return (nodes.getLength() == 0)? null : nodes.item(0).getTextContent();
  }

  public void refresh(String includesFile, String excludesFile)
      throws IOException {
    refreshInternal(includesFile, excludesFile, false);
  }

  public void lazyRefresh(String includesFile, String excludesFile)
      throws IOException {
    refreshInternal(includesFile, excludesFile, true);
  }

  private void refreshInternal(String includesFile, String excludesFile,
      boolean lazy) throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list (lazy refresh = {})",
        lazy);
    HostDetails oldDetails = current.get();
    Set<String> newIncludes = oldDetails.includes;
    Map<String, Integer> newExcludes = oldDetails.excludes;
    if (includesFile != null && !includesFile.isEmpty()) {
      newIncludes = new HashSet<>();
      readFileToSet("included", includesFile, newIncludes);
      newIncludes = Collections.unmodifiableSet(newIncludes);
    }
    if (excludesFile != null && !excludesFile.isEmpty()) {
      newExcludes = new HashMap<>();
      readFileToMap("excluded", excludesFile, newExcludes);
      newExcludes = Collections.unmodifiableMap(newExcludes);
    }
    HostDetails newDetails = new HostDetails(includesFile, newIncludes,
        excludesFile, newExcludes);

    if (lazy) {
      lazyLoaded.set(newDetails);
    } else {
      current.set(newDetails);
    }
  }

  public void finishRefresh() {
    if (lazyLoaded.get() == null) {
      throw new IllegalStateException(
          "Cannot finish refresh - call lazyRefresh() first");
    }
    current.set(lazyLoaded.get());
    lazyLoaded.set(null);
  }

  @Private
  public void refresh(InputStream inFileInputStream,
      InputStream exFileInputStream) throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    HostDetails oldDetails = current.get();
    Set<String> newIncludes = oldDetails.includes;
    Map<String, Integer> newExcludes = oldDetails.excludes;
    if (inFileInputStream != null) {
      newIncludes = new HashSet<>();
      readFileToSetWithFileInputStream("included", oldDetails.includesFile,
          inFileInputStream, newIncludes);
      newIncludes = Collections.unmodifiableSet(newIncludes);
    }
    if (exFileInputStream != null) {
      newExcludes = new HashMap<>();
      readFileToMapWithFileInputStream("excluded", oldDetails.excludesFile,
          exFileInputStream, newExcludes);
      newExcludes = Collections.unmodifiableMap(newExcludes);
    }
    HostDetails newDetails = new HostDetails(
        oldDetails.includesFile, newIncludes,
        oldDetails.excludesFile, newExcludes);
    current.set(newDetails);
  }

  public Set<String> getHosts() {
    HostDetails hostDetails = current.get();
    return hostDetails.getIncludedHosts();
  }

  public Set<String> getExcludedHosts() {
    HostDetails hostDetails = current.get();
    return hostDetails.getExcludedHosts();
  }

  /**
   * Retrieve an atomic view of the included and excluded hosts.
   *
   * @param includes set to populate with included hosts
   * @param excludes set to populate with excluded hosts
   * @deprecated use {@link #getHostDetails() instead}
   */
  @Deprecated
  public void getHostDetails(Set<String> includes, Set<String> excludes) {
    HostDetails hostDetails = current.get();
    includes.addAll(hostDetails.getIncludedHosts());
    excludes.addAll(hostDetails.getExcludedHosts());
  }

  /**
   * Retrieve an atomic view of the included and excluded hosts.
   *
   * @param includeHosts set to populate with included hosts
   * @param excludeHosts map to populate with excluded hosts
   * @deprecated use {@link #getHostDetails() instead}
   */
  @Deprecated
  public void getHostDetails(Set<String> includeHosts,
                             Map<String, Integer> excludeHosts) {
    HostDetails hostDetails = current.get();
    includeHosts.addAll(hostDetails.getIncludedHosts());
    excludeHosts.putAll(hostDetails.getExcludedMap());
  }

  /**
   * Retrieve an atomic view of the included and excluded hosts.
   *
   * @return the included and excluded hosts
   */
  public HostDetails getHostDetails() {
    return current.get();
  }

  public HostDetails getLazyLoadedHostDetails() {
    return lazyLoaded.get();
  }

  public void setIncludesFile(String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    HostDetails oldDetails = current.get();
    HostDetails newDetails = new HostDetails(includesFile, oldDetails.includes,
        oldDetails.excludesFile, oldDetails.excludes);
    current.set(newDetails);
  }
  
  public void setExcludesFile(String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    HostDetails oldDetails = current.get();
    HostDetails newDetails = new HostDetails(
        oldDetails.includesFile, oldDetails.includes,
        excludesFile, oldDetails.excludes);
    current.set(newDetails);
  }

  public void updateFileNames(String includesFile, String excludesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    LOG.info("Setting the excludes file to " + excludesFile);
    HostDetails oldDetails = current.get();
    HostDetails newDetails = new HostDetails(includesFile, oldDetails.includes,
        excludesFile, oldDetails.excludes);
    current.set(newDetails);
  }

  /**
   * An atomic view of the included and excluded hosts
   */
  public static class HostDetails {
    private final String includesFile;
    private final Set<String> includes;
    private final String excludesFile;
    // exclude host list with optional timeout.
    // If the value is null, it indicates default timeout.
    private final Map<String, Integer> excludes;

    HostDetails(String includesFile, Set<String> includes,
        String excludesFile, Map<String, Integer> excludes) {
      this.includesFile = includesFile;
      this.includes = includes;
      this.excludesFile = excludesFile;
      this.excludes = excludes;
    }

    public String getIncludesFile() {
      return includesFile;
    }

    public Set<String> getIncludedHosts() {
      return includes;
    }

    public String getExcludesFile() {
      return excludesFile;
    }

    public Set<String> getExcludedHosts() {
      return excludes.keySet();
    }

    public Map<String, Integer> getExcludedMap() {
      return excludes;
    }
  }
}
