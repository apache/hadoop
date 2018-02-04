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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

import static org.apache.hadoop.yarn.conf.YarnConfiguration
    .NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_OPTS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS;

/**
 * Node attribute provider that periodically runs a script to collect
 * node attributes.
 */
public class ScriptBasedNodeAttributesProvider extends NodeAttributesProvider{

  private static final String NODE_ATTRIBUTE_PATTERN = "NODE_ATTRIBUTE:";
  private static final String NODE_ATTRIBUTE_DELIMITER = ",";

  private NodeAttributeScriptRunner runner;

  public ScriptBasedNodeAttributesProvider() {
    super(ScriptBasedNodeAttributesProvider.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    String nodeAttributeProviderScript = conf.get(
        NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PATH);
    long scriptTimeout = conf.getLong(
        NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS,
        DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS);
    String[] scriptArgs = conf.getStrings(
        NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_OPTS,
        new String[] {});
    verifyConfiguredScript(nodeAttributeProviderScript);

    long intervalTime = conf.getLong(
        NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS,
        DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS);
    this.setIntervalTime(intervalTime);

    this.runner = new NodeAttributeScriptRunner(nodeAttributeProviderScript,
        scriptArgs, scriptTimeout, this);
  }

  @Override
  protected void cleanUp() throws Exception {
    runner.cleanUp();
  }

  @Override
  public TimerTask createTimerTask() {
    return runner;
  }

  private static class NodeAttributeScriptRunner extends
      NodeDescriptorsScriptRunner<NodeAttribute> {

    NodeAttributeScriptRunner(String scriptPath, String[] scriptArgs,
        long scriptTimeout, ScriptBasedNodeAttributesProvider provider) {
      super(scriptPath, scriptArgs, scriptTimeout, provider);
    }

    @Override
    Set<NodeAttribute> parseOutput(String scriptOutput) throws IOException {
      Set<NodeAttribute> attributeSet = new HashSet<>();
      // TODO finalize format

      // each line is a record of ndoe attribute like following:
      // NODE_ATTRIBUTE:ATTRIBUTE_NAME,ATTRIBUTE_TYPE,ATTRIBUTE_VALUE
      String[] splits = scriptOutput.split("\n");
      for (String line : splits) {
        String trimmedLine = line.trim();
        if (trimmedLine.startsWith(NODE_ATTRIBUTE_PATTERN)) {
          String nodeAttribute = trimmedLine
              .substring(NODE_ATTRIBUTE_PATTERN.length());
          String[] attributeStrs = nodeAttribute
              .split(NODE_ATTRIBUTE_DELIMITER);
          if (attributeStrs.length != 3) {
            throw new IOException("Malformed output, expecting format "
                + NODE_ATTRIBUTE_PATTERN + ":" + "ATTRIBUTE_NAME"
                + NODE_ATTRIBUTE_DELIMITER + "ATTRIBUTE_TYPE"
                + NODE_ATTRIBUTE_DELIMITER + "ATTRIBUTE_VALUE; but get "
                + nodeAttribute);
          }
          NodeAttribute na = NodeAttribute
              .newInstance(attributeStrs[0],
                  NodeAttributeType.valueOf(attributeStrs[1]),
                  attributeStrs[2]);
          attributeSet.add(na);
        }
      }
      return attributeSet;
    }
  }
}
