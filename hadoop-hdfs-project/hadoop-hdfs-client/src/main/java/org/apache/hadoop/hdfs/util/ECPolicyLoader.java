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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import org.w3c.dom.Element;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A EC policy loading tool that loads user defined EC policies from XML file.
 */
@InterfaceAudience.Private
public class ECPolicyLoader {

  private static final Logger LOG
      = LoggerFactory.getLogger(ECPolicyLoader.class);

  private static final int LAYOUT_VERSION = 1;

  /**
   * Load user defined EC policies from a XML configuration file.
   * @param policyFilePath path of EC policy file
   * @return all valid EC policies in EC policy file
   */
  public List<ErasureCodingPolicy> loadPolicy(String policyFilePath) {
    try {
      File policyFile = getPolicyFile(policyFilePath);
      if (!policyFile.exists()) {
        LOG.warn("Not found any EC policy file");
        return Collections.emptyList();
      }
      return loadECPolicies(policyFile);
    } catch (ParserConfigurationException | IOException | SAXException e) {
      throw new RuntimeException("Failed to load EC policy file: "
          + policyFilePath);
    }
  }

  /**
   * Load EC policies from a XML configuration file.
   * @param policyFile EC policy file
   * @return list of EC policies
   * @throws ParserConfigurationException if ParserConfigurationException happen
   * @throws IOException if no such EC policy file
   * @throws SAXException if the xml file has some invalid elements
   */
  private List<ErasureCodingPolicy> loadECPolicies(File policyFile)
      throws ParserConfigurationException, IOException, SAXException {

    LOG.info("Loading EC policy file " + policyFile);

    // Read and parse the EC policy file.
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setIgnoringComments(true);
    DocumentBuilder builder = dbf.newDocumentBuilder();
    Document doc = builder.parse(policyFile);
    Element root = doc.getDocumentElement();

    if (!"configuration".equals(root.getTagName())) {
      throw new RuntimeException("Bad EC policy configuration file: "
          + "top-level element not <configuration>");
    }

    List<ErasureCodingPolicy> policies;
    if (root.getElementsByTagName("layoutversion").getLength() > 0) {
      if (loadLayoutVersion(root) == LAYOUT_VERSION) {
        if (root.getElementsByTagName("schemas").getLength() > 0) {
          Map<String, ECSchema> schemas = loadSchemas(root);
          if (root.getElementsByTagName("policies").getLength() > 0) {
            policies = loadPolicies(root, schemas);
          } else {
            throw new RuntimeException("Bad EC policy configuration file: "
                + "no <policies> element");
          }
        } else {
          throw new RuntimeException("Bad EC policy configuration file: "
              + "no <schemas> element");
        }
      } else {
        throw new RuntimeException("The parse failed because of "
            + "bad layoutversion value");
      }
    } else {
      throw new RuntimeException("Bad EC policy configuration file: "
          + "no <layoutVersion> element");
    }

    return policies;
  }

  /**
   * Load layoutVersion from root element in the XML configuration file.
   * @param root root element
   * @return layout version
   */
  private int loadLayoutVersion(Element root) {
    int layoutVersion;
    Text text = (Text) root.getElementsByTagName("layoutversion")
        .item(0).getFirstChild();
    if (text != null) {
      String value = text.getData().trim();
      try {
        layoutVersion = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bad layoutVersion value "
            + value + " is found. It should be an integer");
      }
    } else {
      throw new IllegalArgumentException("Value of <layoutVersion> is null");
    }

    return layoutVersion;
  }

  /**
   * Load schemas from root element in the XML configuration file.
   * @param root root element
   * @return EC schema map
   */
  private Map<String, ECSchema> loadSchemas(Element root) {
    NodeList elements = root.getElementsByTagName("schemas")
        .item(0).getChildNodes();
    Map<String, ECSchema> schemas = new HashMap<String, ECSchema>();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        if ("schema".equals(element.getTagName())) {
          String schemaId = element.getAttribute("id");
          ECSchema schema = loadSchema(element);
          if (!schemas.containsValue(schema)) {
            schemas.put(schemaId, schema);
          } else {
            throw new RuntimeException("Repetitive schemas in EC policy"
                + " configuration file: " + schemaId);
          }
        } else {
          throw new RuntimeException("Bad element in EC policy"
              + " configuration file: " + element.getTagName());
        }
      }
    }

    return schemas;
  }

  /**
   * Load EC policies from root element in the XML configuration file.
   * @param root root element
   * @param schemas schema map
   * @return EC policy list
   */
  private List<ErasureCodingPolicy> loadPolicies(
      Element root, Map<String, ECSchema> schemas) {
    NodeList elements = root.getElementsByTagName("policies")
        .item(0).getChildNodes();
    List<ErasureCodingPolicy> policies = new ArrayList<ErasureCodingPolicy>();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        if ("policy".equals(element.getTagName())) {
          ErasureCodingPolicy policy = loadPolicy(element, schemas);
          if (!policies.contains(policy)) {
            policies.add(policy);
          } else {
            LOG.warn("Repetitive policies in EC policy configuration file: "
                + policy.toString());
          }
        } else {
          throw new RuntimeException("Bad element in EC policy configuration"
              + " file: " + element.getTagName());
        }
      }
    }

    return policies;
  }

  /**
   * Path to the XML file containing user defined EC policies. If the path is
   * relative, it is searched for in the classpath.
   * @param policyFilePath path of EC policy file
   * @return EC policy file
   */
  private File getPolicyFile(String policyFilePath)
      throws MalformedURLException {
    File policyFile = new File(policyFilePath);
    if (!policyFile.isAbsolute()) {
      URL url = new URL(policyFilePath);
      if (!url.getProtocol().equalsIgnoreCase("file")) {
        throw new RuntimeException(
            "EC policy file " + url
                + " found on the classpath is not on the local filesystem.");
      } else {
        policyFile = new File(url.getPath());
      }
    }

    return policyFile;
  }

  /**
   * Load a schema from a schema element in the XML configuration file.
   * @param element EC schema element
   * @return ECSchema
   */
  private ECSchema loadSchema(Element element) {
    Map<String, String> schemaOptions = new HashMap<String, String>();
    NodeList fields = element.getChildNodes();

    for (int i = 0; i < fields.getLength(); i++) {
      Node fieldNode = fields.item(i);
      if (fieldNode instanceof Element) {
        Element field = (Element) fieldNode;
        String tagName = field.getTagName();
        if ("k".equals(tagName)) {
          tagName = "numDataUnits";
        } else if ("m".equals(tagName)) {
          tagName = "numParityUnits";
        }

        // Get the nonnull text value.
        Text text = (Text) field.getFirstChild();
        if (text != null) {
          String value = text.getData().trim();
          schemaOptions.put(tagName, value);
        } else {
          throw new IllegalArgumentException("Value of <" + tagName
              + "> is null");
        }
      }
    }

    return new ECSchema(schemaOptions);
  }

  /**
   * Load a EC policy from a policy element in the XML configuration file.
   * @param element EC policy element
   * @param schemas all valid schemas of the EC policy file
   * @return EC policy
   */
  private ErasureCodingPolicy loadPolicy(Element element,
                                         Map<String, ECSchema> schemas) {
    NodeList fields = element.getChildNodes();
    ECSchema schema = null;
    int cellSize = 0;

    for (int i = 0; i < fields.getLength(); i++) {
      Node fieldNode = fields.item(i);
      if (fieldNode instanceof Element) {
        Element field = (Element) fieldNode;
        String tagName = field.getTagName();

        // Get the nonnull text value.
        Text text = (Text) field.getFirstChild();
        if (text != null) {
          if (!text.isElementContentWhitespace()) {
            String value = text.getData().trim();
            if ("schema".equals(tagName)) {
              schema = schemas.get(value);
            } else if ("cellsize".equals(tagName)) {
              try {
                cellSize = Integer.parseInt(value);
              } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Bad EC policy cellsize"
                    + " value " + value + " is found. It should be an integer");
              }
            } else {
              LOG.warn("Invalid tagName: " + tagName);
            }
          }
        } else {
          throw new IllegalArgumentException("Value of <" + tagName
              + "> is null");
        }
      }
    }

    if (schema != null && cellSize > 0) {
      return new ErasureCodingPolicy(schema, cellSize);
    } else {
      throw new RuntimeException("Bad policy is found in"
          + " EC policy configuration file");
    }
  }
}
