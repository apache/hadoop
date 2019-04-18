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
package org.apache.hadoop.hdds.scm.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import  org.apache.hadoop.hdds.scm.net.NodeSchema.LayerType;
import org.yaml.snakeyaml.Yaml;

/**
 * A Network topology layer schema loading tool that loads user defined network
 * layer schema data from a XML configuration file.
 */
public final class NodeSchemaLoader {
  private static final Logger LOG
      = LoggerFactory.getLogger(NodeSchemaLoader.class);
  private static final String CONFIGURATION_TAG = "configuration";
  private static final String LAYOUT_VERSION_TAG = "layoutversion";
  private static final String TOPOLOGY_TAG = "topology";
  private static final String TOPOLOGY_PATH = "path";
  private static final String TOPOLOGY_ENFORCE_PREFIX = "enforceprefix";
  private static final String LAYERS_TAG = "layers";
  private static final String LAYER_TAG = "layer";
  private static final String LAYER_ID = "id";
  private static final String LAYER_TYPE = "type";
  private static final String LAYER_COST = "cost";
  private static final String LAYER_PREFIX = "prefix";
  private static final String LAYER_DEFAULT_NAME = "default";

  private static final int LAYOUT_VERSION = 1;
  private volatile static NodeSchemaLoader instance = null;
  private NodeSchemaLoader() {}

  public static NodeSchemaLoader getInstance() {
    if (instance == null) {
      instance = new NodeSchemaLoader();
    }
    return instance;
  }

  /**
   * Class to house keep the result of parsing a network topology schema file.
   */
  public static class NodeSchemaLoadResult {
    private List<NodeSchema> schemaList;
    private boolean enforcePrefix;

    NodeSchemaLoadResult(List<NodeSchema> schemaList, boolean enforcePrefix) {
      this.schemaList = schemaList;
      this.enforcePrefix = enforcePrefix;
    }

    public boolean isEnforePrefix() {
      return enforcePrefix;
    }

    public List<NodeSchema> getSchemaList() {
      return schemaList;
    }
  }

  /**
   * Load user defined network layer schemas from a XML configuration file.
   * @param schemaFilePath path of schema file
   * @return all valid node schemas defined in schema file
   */
  public NodeSchemaLoadResult loadSchemaFromXml(String schemaFilePath)
      throws IllegalArgumentException {
    try {
      File schemaFile = new File(schemaFilePath);
      if (!schemaFile.exists()) {
        String msg = "Network topology layer schema file " + schemaFilePath +
            " is not found.";
        LOG.warn(msg);
        throw new IllegalArgumentException(msg);
      }
      return loadSchema(schemaFile);
    } catch (ParserConfigurationException | IOException | SAXException e) {
      throw new IllegalArgumentException("Fail to load network topology node"
          + " schema file: " + schemaFilePath + " , error:" + e.getMessage());
    }
  }

  /**
   * Load network topology layer schemas from a XML configuration file.
   * @param schemaFile schema file
   * @return all valid node schemas defined in schema file
   * @throws ParserConfigurationException ParserConfigurationException happen
   * @throws IOException no such schema file
   * @throws SAXException xml file has some invalid elements
   * @throws IllegalArgumentException xml file content is logically invalid
   */
  private NodeSchemaLoadResult loadSchema(File schemaFile) throws
      ParserConfigurationException, SAXException, IOException {
    LOG.info("Loading network topology layer schema file " + schemaFile);
    // Read and parse the schema file.
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setIgnoringComments(true);
    DocumentBuilder builder = dbf.newDocumentBuilder();
    Document doc = builder.parse(schemaFile);
    Element root = doc.getDocumentElement();

    if (!CONFIGURATION_TAG.equals(root.getTagName())) {
      throw new IllegalArgumentException("Bad network topology layer schema " +
          "configuration file: top-level element not <" + CONFIGURATION_TAG +
          ">");
    }
    NodeSchemaLoadResult schemaList;
    if (root.getElementsByTagName(LAYOUT_VERSION_TAG).getLength() == 1) {
      if (loadLayoutVersion(root) == LAYOUT_VERSION) {
        if (root.getElementsByTagName(LAYERS_TAG).getLength() == 1) {
          Map<String, NodeSchema> schemas = loadLayersSection(root);
          if (root.getElementsByTagName(TOPOLOGY_TAG).getLength() == 1) {
            schemaList = loadTopologySection(root, schemas);
          } else {
            throw new IllegalArgumentException("Bad network topology layer " +
                "schema configuration file: no or multiple <" + TOPOLOGY_TAG +
                "> element");
          }
        } else {
          throw new IllegalArgumentException("Bad network topology layer schema"
              + " configuration file: no or multiple <" + LAYERS_TAG +
              ">element");
        }
      } else {
        throw new IllegalArgumentException("The parse failed because of bad "
            + LAYOUT_VERSION_TAG + " value, expected:" + LAYOUT_VERSION);
      }
    } else {
      throw new IllegalArgumentException("Bad network topology layer schema " +
          "configuration file: no or multiple <" + LAYOUT_VERSION_TAG +
          "> elements");
    }
    return schemaList;
  }

  /**
   * Load user defined network layer schemas from a YAML configuration file.
   * @param schemaFilePath path of schema file
   * @return all valid node schemas defined in schema file
   */
  public NodeSchemaLoadResult loadSchemaFromYaml(String schemaFilePath)
          throws IllegalArgumentException {
    try {
      File schemaFile = new File(schemaFilePath);
      if (!schemaFile.exists()) {
        String msg = "Network topology layer schema file " + schemaFilePath +
                " is not found.";
        LOG.warn(msg);
        throw new IllegalArgumentException(msg);
      }
      return loadSchemaFromYaml(schemaFile);
    } catch (Exception e) {
      throw new IllegalArgumentException("Fail to load network topology node"
              + " schema file: " + schemaFilePath + " , error:"
              + e.getMessage());
    }
  }

  /**
   * Load network topology layer schemas from a YAML configuration file.
   * @param schemaFile schema file
   * @return all valid node schemas defined in schema file
   * @throws ParserConfigurationException ParserConfigurationException happen
   * @throws IOException no such schema file
   * @throws SAXException xml file has some invalid elements
   * @throws IllegalArgumentException xml file content is logically invalid
   */
  private NodeSchemaLoadResult loadSchemaFromYaml(File schemaFile) {
    LOG.info("Loading network topology layer schema file {}", schemaFile);
    NodeSchemaLoadResult finalSchema;

    try {
      Yaml yaml = new Yaml();
      NodeSchema nodeTree;

      try (FileInputStream fileInputStream = new FileInputStream(schemaFile)) {
        nodeTree = yaml.loadAs(fileInputStream, NodeSchema.class);
      }
      List<NodeSchema> schemaList = new ArrayList<>();
      if (nodeTree.getType() != LayerType.ROOT) {
        throw new IllegalArgumentException("First layer is not a ROOT node."
                + " schema file: " + schemaFile.getAbsolutePath());
      }
      schemaList.add(nodeTree);
      if (nodeTree.getSublayer() != null) {
        nodeTree = nodeTree.getSublayer().get(0);
      }

      while (nodeTree != null) {
        if (nodeTree.getType() == LayerType.LEAF_NODE
                && nodeTree.getSublayer() != null) {
          throw new IllegalArgumentException("Leaf node in the middle of path."
                  + " schema file: " + schemaFile.getAbsolutePath());
        }
        if (nodeTree.getType() == LayerType.ROOT) {
          throw new IllegalArgumentException("Multiple root nodes are defined."
                  + " schema file: " + schemaFile.getAbsolutePath());
        }
        schemaList.add(nodeTree);
        if (nodeTree.getSublayer() != null) {
          nodeTree = nodeTree.getSublayer().get(0);
        } else {
          break;
        }
      }
      finalSchema = new NodeSchemaLoadResult(schemaList, true);
    } catch (RuntimeException e) {
      throw  e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Fail to load network topology node"
              + " schema file: " + schemaFile.getAbsolutePath() + " , error:"
              + e.getMessage());
    }

    return finalSchema;
  }

  /**
   * Load layoutVersion from root element in the XML configuration file.
   * @param root root element
   * @return layout version
   */
  private int loadLayoutVersion(Element root) {
    int layoutVersion;
    Text text = (Text) root.getElementsByTagName(LAYOUT_VERSION_TAG)
        .item(0).getFirstChild();
    if (text != null) {
      String value = text.getData().trim();
      try {
        layoutVersion = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bad " + LAYOUT_VERSION_TAG +
            " value " + value + " is found. It should be an integer.");
      }
    } else {
      throw new IllegalArgumentException("Value of <" + LAYOUT_VERSION_TAG +
          "> is null");
    }
    return layoutVersion;
  }

  /**
   * Load layers from root element in the XML configuration file.
   * @param root root element
   * @return A map of node schemas with layer ID and layer schema
   */
  private Map<String, NodeSchema> loadLayersSection(Element root) {
    NodeList elements = root.getElementsByTagName(LAYER_TAG);
    Map<String, NodeSchema> schemas = new HashMap<String, NodeSchema>();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        if (LAYER_TAG.equals(element.getTagName())) {
          String layerId = element.getAttribute(LAYER_ID);
          NodeSchema schema = parseLayerElement(element);
          if (!schemas.containsValue(schema)) {
            schemas.put(layerId, schema);
          } else {
            throw new IllegalArgumentException("Repetitive layer in network " +
                "topology node schema configuration file: " + layerId);
          }
        } else {
          throw new IllegalArgumentException("Bad element in network topology "
              + "node schema configuration file: " + element.getTagName());
        }
      }
    }

    // Integrity check, only one ROOT and one LEAF is allowed
    boolean foundRoot = false;
    boolean foundLeaf = false;
    for(NodeSchema schema: schemas.values()) {
      if (schema.getType() == LayerType.ROOT) {
        if (foundRoot) {
          throw new IllegalArgumentException("Multiple ROOT layers are found" +
              " in network topology schema configuration file");
        } else {
          foundRoot = true;
        }
      }
      if (schema.getType() == LayerType.LEAF_NODE) {
        if (foundLeaf) {
          throw new IllegalArgumentException("Multiple LEAF layers are found" +
              " in network topology schema configuration file");
        } else {
          foundLeaf = true;
        }
      }
    }
    if (!foundRoot) {
      throw new IllegalArgumentException("No ROOT layer is found" +
          " in network topology schema configuration file");
    }
    if (!foundLeaf) {
      throw new IllegalArgumentException("No LEAF layer is found" +
          " in network topology schema configuration file");
    }
    return schemas;
  }

  /**
   * Load network topology from root element in the XML configuration file and
   * sort node schemas according to the topology path.
   * @param root root element
   * @param schemas schema map
   * @return all valid node schemas defined in schema file
   */
  private NodeSchemaLoadResult loadTopologySection(Element root,
      Map<String, NodeSchema> schemas) {
    NodeList elements = root.getElementsByTagName(TOPOLOGY_TAG)
        .item(0).getChildNodes();
    List<NodeSchema> schemaList = new ArrayList<NodeSchema>();
    boolean enforecePrefix = false;
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        String tagName = element.getTagName();
        // Get the nonnull text value.
        Text text = (Text) element.getFirstChild();
        String value;
        if (text != null) {
          value = text.getData().trim();
          if (value.isEmpty()) {
            // Element with empty value is ignored
            continue;
          }
        } else {
          throw new IllegalArgumentException("Value of <" + tagName
              + "> is null");
        }
        if (TOPOLOGY_PATH.equals(tagName)) {
          if(value.startsWith(NetConstants.PATH_SEPARATOR_STR)) {
            value = value.substring(1, value.length());
          }
          String[] layerIDs = value.split(NetConstants.PATH_SEPARATOR_STR);
          if (layerIDs == null || layerIDs.length != schemas.size()) {
            throw new IllegalArgumentException("Topology path depth doesn't "
                + "match layer element numbers");
          }
          for (int j = 0; j < layerIDs.length; j++) {
            if (schemas.get(layerIDs[j]) == null) {
              throw new IllegalArgumentException("No layer found for id " +
                  layerIDs[j]);
            }
          }
          if (schemas.get(layerIDs[0]).getType() != LayerType.ROOT) {
            throw new IllegalArgumentException("Topology path doesn't start "
                + "with ROOT layer");
          }
          if (schemas.get(layerIDs[layerIDs.length -1]).getType() !=
              LayerType.LEAF_NODE) {
            throw new IllegalArgumentException("Topology path doesn't end "
                + "with LEAF layer");
          }
          for (int j = 0; j < layerIDs.length; j++) {
            schemaList.add(schemas.get(layerIDs[j]));
          }
        } else if (TOPOLOGY_ENFORCE_PREFIX.equalsIgnoreCase(tagName)) {
          enforecePrefix = Boolean.parseBoolean(value);
        } else {
          throw new IllegalArgumentException("Unsupported Element <" +
              tagName + ">");
        }
      }
    }
    // Integrity check
    if (enforecePrefix) {
      // Every InnerNode should have prefix defined
      for (NodeSchema schema: schemas.values()) {
        if (schema.getType() == LayerType.INNER_NODE &&
            schema.getPrefix() == null) {
          throw new IllegalArgumentException("There is layer without prefix " +
              "defined while prefix is enforced.");
        }
      }
    }
    return new NodeSchemaLoadResult(schemaList, enforecePrefix);
  }

  /**
   * Load a layer from a layer element in the XML configuration file.
   * @param element network topology node layer element
   * @return ECSchema
   */
  private NodeSchema parseLayerElement(Element element) {
    NodeList fields = element.getChildNodes();
    LayerType type = null;
    int cost = 0;
    String prefix = null;
    String defaultName = null;
    for (int i = 0; i < fields.getLength(); i++) {
      Node fieldNode = fields.item(i);
      if (fieldNode instanceof Element) {
        Element field = (Element) fieldNode;
        String tagName = field.getTagName();
        // Get the nonnull text value.
        Text text = (Text) field.getFirstChild();
        String value;
        if (text != null) {
          value = text.getData().trim();
          if (value.isEmpty()) {
            // Element with empty value is ignored
            continue;
          }
        } else {
          continue;
        }
        if (LAYER_COST.equalsIgnoreCase(tagName)) {
          cost = Integer.parseInt(value);
          if (cost < 0) {
            throw new IllegalArgumentException(
                "Cost should be positive number or 0");
          }
        } else if (LAYER_TYPE.equalsIgnoreCase(tagName)) {
          type = NodeSchema.LayerType.getType(value);
          if (type == null) {
            throw new IllegalArgumentException(
                "Unsupported layer type:" + value);
          }
        } else if (LAYER_PREFIX.equalsIgnoreCase(tagName)) {
          prefix = value;
        } else if (LAYER_DEFAULT_NAME.equalsIgnoreCase(tagName)) {
          defaultName = value;
        } else {
          throw new IllegalArgumentException("Unsupported Element <" + tagName
              + ">");
        }
      }
    }
    // type is a mandatory property
    if (type == null) {
      throw new IllegalArgumentException("Missing type Element");
    }
    return new NodeSchema(type, cost, prefix, defaultName);
  }
}
