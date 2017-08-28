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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * The class which provides functionality of getting the labels of the node
 * using the configured node labels provider script. "NODE_PARTITION:" is the
 * pattern which will be used to search node label partition from the out put of
 * the NodeLabels provider script
 */
public class ScriptBasedNodeLabelsProvider extends AbstractNodeLabelsProvider {
  /** Absolute path to the node labels script. */
  private String nodeLabelsScriptPath;

  /** Time after which the script should be timed out */
  private long scriptTimeout;

  /** ShellCommandExecutor used to execute monitoring script */
  ShellCommandExecutor shexec = null;

  /** Pattern used for searching in the output of the node labels script */
  public static final String NODE_LABEL_PARTITION_PATTERN = "NODE_PARTITION:";

  private String[] scriptArgs;

  public ScriptBasedNodeLabelsProvider() {
    super(ScriptBasedNodeLabelsProvider.class.getName());
  }

  /*
   * Method which initializes the values for the script path and interval time.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.nodeLabelsScriptPath =
        conf.get(YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PATH);
    this.scriptTimeout =
        conf.getLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS,
            YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS);
    scriptArgs = conf.getStrings(
        YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_SCRIPT_OPTS,
        new String[] {});

    verifyConfiguredScript();
  }

  /**
   * Method used to determine if or not node labels fetching script is
   * configured and whether it is fit to run. Returns true if following
   * conditions are met:
   *
   * <ol>
   * <li>Path to Node Labels fetch script is not empty</li>
   * <li>Node Labels fetch script file exists</li>
   * </ol>
   *
   * @throws IOException
   */
  private void verifyConfiguredScript()
      throws IOException {
    boolean invalidConfiguration = false;
    if (nodeLabelsScriptPath == null
        || nodeLabelsScriptPath.trim().isEmpty()) {
      invalidConfiguration = true;
    } else {
      File f = new File(nodeLabelsScriptPath);
      invalidConfiguration = !f.exists() || !FileUtil.canExecute(f);
    }
    if (invalidConfiguration) {
      throw new IOException(
          "Distributed Node labels provider script \"" + nodeLabelsScriptPath
              + "\" is not configured properly. Please check whether the script "
              + "path exists, owner and the access rights are suitable for NM "
              + "process to execute it");
    }
  }

  /**
   * Method used to terminate the Node Labels Fetch script.
   */
  @Override
  public void cleanUp() {
    if (shexec != null) {
      Process p = shexec.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
  }

  @Override
  public TimerTask createTimerTask() {
    return new NodeLabelsScriptRunner();
  }

  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * node labels script.
   */
  private class NodeLabelsScriptRunner extends TimerTask {

    private final Logger LOG =
        LoggerFactory.getLogger(NodeLabelsScriptRunner.class);

    public NodeLabelsScriptRunner() {
      ArrayList<String> execScript = new ArrayList<String>();
      execScript.add(nodeLabelsScriptPath);
      if (scriptArgs != null) {
        execScript.addAll(Arrays.asList(scriptArgs));
      }
      shexec = new ShellCommandExecutor(
          execScript.toArray(new String[execScript.size()]), null, null,
          scriptTimeout);
    }

    @Override
    public void run() {
      try {
        shexec.execute();
        setNodeLabels(fetchLabelsFromScriptOutput(shexec.getOutput()));
      } catch (Exception e) {
        if (shexec.isTimedOut()) {
          LOG.warn("Node Labels script timed out, Caught exception : "
              + e.getMessage(), e);
        } else {
          LOG.warn("Execution of Node Labels script failed, Caught exception : "
              + e.getMessage(), e);
        }
      }
    }

    /**
     * Method which collect lines from the output string which begins with
     * Patterns provided.
     *
     * @param scriptOutput string
     * @return true if output string has error pattern in it.
     * @throws IOException
     */
    private Set<NodeLabel> fetchLabelsFromScriptOutput(String scriptOutput)
        throws IOException {
      String nodePartitionLabel = null;
      String[] splits = scriptOutput.split("\n");
      for (String line : splits) {
        String trimmedLine = line.trim();
        if (trimmedLine.startsWith(NODE_LABEL_PARTITION_PATTERN)) {
          nodePartitionLabel =
              trimmedLine.substring(NODE_LABEL_PARTITION_PATTERN.length());
        }
      }
      return convertToNodeLabelSet(nodePartitionLabel);
    }
  }
}
