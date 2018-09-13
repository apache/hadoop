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

import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TimerTask;

/**
 * A node descriptors script runner periodically runs a script,
 * parses the output to collect desired descriptors, and then
 * post these descriptors to the given {@link NodeDescriptorsProvider}.
 * @param <T> a certain type of descriptor.
 */
public abstract class NodeDescriptorsScriptRunner<T> extends TimerTask {

  private final static Logger LOG = LoggerFactory
      .getLogger(NodeDescriptorsScriptRunner.class);

  private final Shell.ShellCommandExecutor exec;
  private final NodeDescriptorsProvider provider;

  public NodeDescriptorsScriptRunner(String scriptPath,
      String[] scriptArgs, long scriptTimeout,
      NodeDescriptorsProvider ndProvider) {
    ArrayList<String> execScript = new ArrayList<>();
    execScript.add(scriptPath);
    if (scriptArgs != null) {
      execScript.addAll(Arrays.asList(scriptArgs));
    }
    this.provider = ndProvider;
    this.exec = new Shell.ShellCommandExecutor(
        execScript.toArray(new String[execScript.size()]), null, null,
        scriptTimeout);
  }

  @Override
  public void run() {
    try {
      exec.execute();
      provider.setDescriptors(parseOutput(exec.getOutput()));
    } catch (Exception e) {
      if (exec.isTimedOut()) {
        LOG.warn("Node Labels script timed out, Caught exception : "
            + e.getMessage(), e);
      } else {
        LOG.warn("Execution of Node Labels script failed, Caught exception : "
            + e.getMessage(), e);
      }
    }
  }

  public void cleanUp() {
    if (exec != null) {
      Process p = exec.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
  }

  abstract Set<T> parseOutput(String scriptOutput) throws IOException;
}
