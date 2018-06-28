/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.yaml.snakeyaml.Yaml;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.File;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;

import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import static org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData.YAML_FIELDS;
import static org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData.YAML_TAG;

/**
 * Class for creating and reading .container files.
 */

public final class ContainerDataYaml {

  private ContainerDataYaml() {

  }
  /**
   * Creates a .container file in yaml format.
   *
   * @param containerFile
   * @param containerData
   * @throws IOException
   */
  public static void createContainerFile(ContainerProtos.ContainerType
                                             containerType, File containerFile,
                                         ContainerData containerData) throws
      IOException {

    Preconditions.checkNotNull(containerFile, "yamlFile cannot be null");
    Preconditions.checkNotNull(containerData, "containerData cannot be null");
    Preconditions.checkNotNull(containerType, "containerType cannot be null");

    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    switch(containerType) {
    case KeyValueContainer:
      Representer representer = new ContainerDataRepresenter();
      representer.setPropertyUtils(propertyUtils);
      representer.addClassTag(KeyValueContainerData.class,
          KeyValueContainerData.YAML_TAG);

      Constructor keyValueDataConstructor = new ContainerDataConstructor();

      Yaml yaml = new Yaml(keyValueDataConstructor, representer);
      Writer writer = new OutputStreamWriter(new FileOutputStream(
          containerFile), "UTF-8");
      yaml.dump(containerData, writer);
      writer.close();
      break;
    default:
      throw new StorageContainerException("Unrecognized container Type " +
          "format " + containerType, ContainerProtos.Result
          .UNKNOWN_CONTAINER_TYPE);
    }
  }

  /**
   * Read the yaml file, and return containerData.
   *
   * @param containerFile
   * @throws IOException
   */
  public static ContainerData readContainerFile(File containerFile)
      throws IOException {
    Preconditions.checkNotNull(containerFile, "containerFile cannot be null");

    InputStream input = null;
    ContainerData containerData;
    try {
      PropertyUtils propertyUtils = new PropertyUtils();
      propertyUtils.setBeanAccess(BeanAccess.FIELD);
      propertyUtils.setAllowReadOnlyProperties(true);

      Representer representer = new ContainerDataRepresenter();
      representer.setPropertyUtils(propertyUtils);

      Constructor containerDataConstructor = new ContainerDataConstructor();

      Yaml yaml = new Yaml(containerDataConstructor, representer);
      yaml.setBeanAccess(BeanAccess.FIELD);

      input = new FileInputStream(containerFile);
      containerData = (ContainerData)
          yaml.load(input);
    } finally {
      if (input!= null) {
        input.close();
      }
    }
    return containerData;
  }

  /**
   * Representer class to define which fields need to be stored in yaml file.
   */
  private static class ContainerDataRepresenter extends Representer {
    @Override
    protected Set<Property> getProperties(Class<? extends Object> type)
        throws IntrospectionException {
      Set<Property> set = super.getProperties(type);
      Set<Property> filtered = new TreeSet<Property>();

      // When a new Container type is added, we need to add what fields need
      // to be filtered here
      if (type.equals(KeyValueContainerData.class)) {
        // filter properties
        for (Property prop : set) {
          String name = prop.getName();
          if (YAML_FIELDS.contains(name)) {
            filtered.add(prop);
          }
        }
      }
      return filtered;
    }
  }

  /**
   * Constructor class for KeyValueData, which will be used by Yaml.
   */
  private static class ContainerDataConstructor extends Constructor {
    ContainerDataConstructor() {
      //Adding our own specific constructors for tags.
      // When a new Container type is added, we need to add yamlConstructor
      // for that
      this.yamlConstructors.put(YAML_TAG, new ConstructKeyValueContainerData());
      this.yamlConstructors.put(Tag.INT, new ConstructLong());
    }

    private class ConstructKeyValueContainerData extends AbstractConstruct {
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);

        //Needed this, as TAG.INT type is by default converted to Long.
        long layOutVersion = (long) nodes.get("layOutVersion");
        int lv = (int) layOutVersion;

        //When a new field is added, it needs to be added here.
        KeyValueContainerData kvData = new KeyValueContainerData((long) nodes
            .get("containerId"), lv);
        kvData.setContainerDBType((String)nodes.get("containerDBType"));
        kvData.setMetadataPath((String) nodes.get(
            "metadataPath"));
        kvData.setChunksPath((String) nodes.get("chunksPath"));
        Map<String, String> meta = (Map) nodes.get("metadata");
        meta.forEach((key, val) -> {
          try {
            kvData.addMetadata(key, val);
          } catch (IOException e) {
            throw new IllegalStateException("Unexpected " +
                "Key Value Pair " + "(" + key + "," + val +")in the metadata " +
                "for containerId " + (long) nodes.get("containerId"));
          }
        });
        String state = (String) nodes.get("state");
        switch (state) {
        case "OPEN":
          kvData.setState(ContainerProtos.ContainerLifeCycleState.OPEN);
          break;
        case "CLOSING":
          kvData.setState(ContainerProtos.ContainerLifeCycleState.CLOSING);
          break;
        case "CLOSED":
          kvData.setState(ContainerProtos.ContainerLifeCycleState.CLOSED);
          break;
        default:
          throw new IllegalStateException("Unexpected " +
              "ContainerLifeCycleState " + state + " for the containerId " +
              (long) nodes.get("containerId"));
        }
        return kvData;
      }
    }

    //Below code is taken from snake yaml, as snakeyaml tries to fit the
    // number if it fits in integer, otherwise returns long. So, slightly
    // modified the code to return long in all cases.
    private class ConstructLong extends AbstractConstruct {
      public Object construct(Node node) {
        String value = constructScalar((ScalarNode) node).toString()
            .replaceAll("_", "");
        int sign = +1;
        char first = value.charAt(0);
        if (first == '-') {
          sign = -1;
          value = value.substring(1);
        } else if (first == '+') {
          value = value.substring(1);
        }
        int base = 10;
        if ("0".equals(value)) {
          return Long.valueOf(0);
        } else if (value.startsWith("0b")) {
          value = value.substring(2);
          base = 2;
        } else if (value.startsWith("0x")) {
          value = value.substring(2);
          base = 16;
        } else if (value.startsWith("0")) {
          value = value.substring(1);
          base = 8;
        } else if (value.indexOf(':') != -1) {
          String[] digits = value.split(":");
          int bes = 1;
          int val = 0;
          for (int i = 0, j = digits.length; i < j; i++) {
            val += (Long.parseLong(digits[(j - i) - 1]) * bes);
            bes *= 60;
          }
          return createNumber(sign, String.valueOf(val), 10);
        } else {
          return createNumber(sign, value, 10);
        }
        return createNumber(sign, value, base);
      }
    }

    private Number createNumber(int sign, String number, int radix) {
      Number result;
      if (sign < 0) {
        number = "-" + number;
      }
      result = Long.valueOf(number, radix);
      return result;
    }
  }

}
