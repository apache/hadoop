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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TimerTask;
import java.util.Set;

/**
 * Configuration based node attributes provider.
 */
public class ConfigurationNodeAttributesProvider
    extends NodeAttributesProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigurationNodeAttributesProvider.class);

  private static final String NODE_ATTRIBUTES_DELIMITER = ":";
  private static final String NODE_ATTRIBUTE_DELIMITER = ",";

  public ConfigurationNodeAttributesProvider() {
    super("Configuration Based Node Attributes Provider");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    long taskInterval = conf.getLong(YarnConfiguration
            .NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration
            .DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS);
    this.setIntervalTime(taskInterval);
    super.serviceInit(conf);
  }

  private void updateNodeAttributesFromConfig(Configuration conf)
      throws IOException {
    String configuredNodeAttributes = conf.get(
        YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES, null);
    setDescriptors(parseAttributes(configuredNodeAttributes));
  }

  @VisibleForTesting
  public Set<NodeAttribute> parseAttributes(String config)
      throws IOException {
    if (Strings.isNullOrEmpty(config)) {
      return ImmutableSet.of();
    }
    Set<NodeAttribute> attributeSet = new HashSet<>();
    // Configuration value should be in one line, format:
    // "ATTRIBUTE_NAME,ATTRIBUTE_TYPE,ATTRIBUTE_VALUE",
    // multiple node-attributes are delimited by ":".
    // Each attribute str should not container any space.
    String[] attributeStrs = config.split(NODE_ATTRIBUTES_DELIMITER);
    for (String attributeStr : attributeStrs) {
      String[] fields = attributeStr.split(NODE_ATTRIBUTE_DELIMITER);
      if (fields.length != 3) {
        throw new IOException("Invalid value for "
            + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES
            + "=" + config);
      }

      // We don't allow user config to overwrite our dist prefix,
      // so disallow any prefix set in the configuration.
      if (fields[0].contains("/")) {
        throw new IOException("Node attribute set in "
            + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES
            + " should not contain any prefix.");
      }

      // Make sure attribute type is valid.
      if (!EnumUtils.isValidEnum(NodeAttributeType.class, fields[1])) {
        throw new IOException("Invalid node attribute type: "
            + fields[1] + ", valid values are "
            + Arrays.asList(NodeAttributeType.values()));
      }

      // Automatically setup prefix for collected attributes
      NodeAttribute na = NodeAttribute.newInstance(
          NodeAttribute.PREFIX_DISTRIBUTED,
          fields[0],
          NodeAttributeType.valueOf(fields[1]),
          fields[2]);

      // Since a NodeAttribute is identical with another one as long as
      // their prefix and name are same, to avoid attributes getting
      // overwritten by ambiguous attribute, make sure it fails in such
      // case.
      if (!attributeSet.add(na)) {
        throw new IOException("Ambiguous node attribute is found: "
            + na.toString() + ", a same attribute already exists");
      }
    }

    // Before updating the attributes to the provider,
    // verify if they are valid
    try {
      NodeLabelUtil.validateNodeAttributes(attributeSet);
    } catch (IOException e) {
      throw new IOException("Node attributes set by configuration property: "
          + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES
          + " is not valid. Detail message: " + e.getMessage());
    }
    return attributeSet;
  }

  private class ConfigurationMonitorTimerTask extends TimerTask {
    @Override
    public void run() {
      try {
        updateNodeAttributesFromConfig(new YarnConfiguration());
      } catch (Exception e) {
        LOG.error("Failed to update node attributes from "
            + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES, e);
      }
    }
  }

  @Override
  protected void cleanUp() throws Exception {
    // Nothing to cleanup
  }

  @Override
  public TimerTask createTimerTask() {
    return new ConfigurationMonitorTimerTask();
  }
}
