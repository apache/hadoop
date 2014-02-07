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
package org.apache.hadoop.yarn.client.cli;

import java.io.PrintStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@Private
@Unstable
public abstract class YarnCLI extends Configured implements Tool {

  public static final String STATUS_CMD = "status";
  public static final String LIST_CMD = "list";
  public static final String KILL_CMD = "kill";
  public static final String MOVE_TO_QUEUE_CMD = "movetoqueue";
  public static final String HELP_CMD = "help";
  protected PrintStream sysout;
  protected PrintStream syserr;
  protected YarnClient client;

  public YarnCLI() {
    super(new YarnConfiguration());
    client = YarnClient.createYarnClient();
    client.init(getConf());
    client.start();
  }

  public void setSysOutPrintStream(PrintStream sysout) {
    this.sysout = sysout;
  }

  public void setSysErrPrintStream(PrintStream syserr) {
    this.syserr = syserr;
  }

  public YarnClient getClient() {
    return client;
  }

  public void setClient(YarnClient client) {
    this.client = client;
  }

  public void stop() {
    this.client.stop();
  }
}