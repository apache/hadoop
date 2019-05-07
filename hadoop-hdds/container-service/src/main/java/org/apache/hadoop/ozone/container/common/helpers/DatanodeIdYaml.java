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
package org.apache.hadoop.ozone.container.common.helpers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Class for creating datanode.id file in yaml format.
 */
public final class DatanodeIdYaml {

  private DatanodeIdYaml() {
    // static helper methods only, no state.
  }

  /**
   * Creates a yaml file using DatnodeDetails. This method expects the path
   * validation to be performed by the caller.
   *
   * @param datanodeDetails {@link DatanodeDetails}
   * @param path            Path to datnode.id file
   */
  public static void createDatanodeIdFile(DatanodeDetails datanodeDetails,
                                          File path) throws IOException {
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
    Yaml yaml = new Yaml(options);

    try (Writer writer = new OutputStreamWriter(
        new FileOutputStream(path), "UTF-8")) {
      yaml.dump(getDatanodeDetailsYaml(datanodeDetails), writer);
    }
  }

  /**
   * Read datanode.id from file.
   */
  public static DatanodeDetails readDatanodeIdFile(File path)
      throws IOException {
    DatanodeDetails datanodeDetails;
    try (FileInputStream inputFileStream = new FileInputStream(path)) {
      Yaml yaml = new Yaml();
      DatanodeDetailsYaml datanodeDetailsYaml;
      try {
        datanodeDetailsYaml =
            yaml.loadAs(inputFileStream, DatanodeDetailsYaml.class);
      } catch (Exception e) {
        throw new IOException("Unable to parse yaml file.", e);
      }

      DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
      builder.setUuid(datanodeDetailsYaml.getUuid())
          .setIpAddress(datanodeDetailsYaml.getIpAddress())
          .setHostName(datanodeDetailsYaml.getHostName())
          .setCertSerialId(datanodeDetailsYaml.getCertSerialId());

      if (!MapUtils.isEmpty(datanodeDetailsYaml.getPortDetails())) {
        for (Map.Entry<String, Integer> portEntry :
            datanodeDetailsYaml.getPortDetails().entrySet()) {
          builder.addPort(DatanodeDetails.newPort(
              DatanodeDetails.Port.Name.valueOf(portEntry.getKey()),
              portEntry.getValue()));
        }
      }
      datanodeDetails = builder.build();
    }

    return datanodeDetails;
  }

  /**
   * Datanode details bean to be written to the yaml file.
   */
  public static class DatanodeDetailsYaml {
    private String uuid;
    private String ipAddress;
    private String hostName;
    private String certSerialId;
    private Map<String, Integer> portDetails;

    public DatanodeDetailsYaml() {
      // Needed for snake-yaml introspection.
    }

    private DatanodeDetailsYaml(String uuid, String ipAddress,
                                String hostName, String certSerialId,
                                Map<String, Integer> portDetails) {
      this.uuid = uuid;
      this.ipAddress = ipAddress;
      this.hostName = hostName;
      this.certSerialId = certSerialId;
      this.portDetails = portDetails;
    }

    public String getUuid() {
      return uuid;
    }

    public String getIpAddress() {
      return ipAddress;
    }

    public String getHostName() {
      return hostName;
    }

    public String getCertSerialId() {
      return certSerialId;
    }

    public Map<String, Integer> getPortDetails() {
      return portDetails;
    }

    public void setUuid(String uuid) {
      this.uuid = uuid;
    }

    public void setIpAddress(String ipAddress) {
      this.ipAddress = ipAddress;
    }

    public void setHostName(String hostName) {
      this.hostName = hostName;
    }

    public void setCertSerialId(String certSerialId) {
      this.certSerialId = certSerialId;
    }

    public void setPortDetails(Map<String, Integer> portDetails) {
      this.portDetails = portDetails;
    }
  }

  private static DatanodeDetailsYaml getDatanodeDetailsYaml(
      DatanodeDetails datanodeDetails) {

    Map<String, Integer> portDetails = new LinkedHashMap<>();
    if (!CollectionUtils.isEmpty(datanodeDetails.getPorts())) {
      for (DatanodeDetails.Port port : datanodeDetails.getPorts()) {
        portDetails.put(port.getName().toString(), port.getValue());
      }
    }

    return new DatanodeDetailsYaml(
        datanodeDetails.getUuid().toString(),
        datanodeDetails.getIpAddress(),
        datanodeDetails.getHostName(),
        datanodeDetails.getCertSerialId(),
        portDetails);
  }
}
