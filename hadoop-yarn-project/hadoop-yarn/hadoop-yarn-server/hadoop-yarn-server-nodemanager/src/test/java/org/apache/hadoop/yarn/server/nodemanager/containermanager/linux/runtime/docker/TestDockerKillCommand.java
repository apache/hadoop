/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the docker kill command and its command line arguments.
 */
public class TestDockerKillCommand {

  private DockerKillCommand dockerKillCommand;

  private static final String SIGNAL = "SIGUSR2";
  private static final String CONTAINER_NAME = "foo";

  @Before
  public void setup() {
    dockerKillCommand = new DockerKillCommand(CONTAINER_NAME);
  }

  @Test
  public void testGetCommandOption() {
    assertEquals("kill", dockerKillCommand.getCommandOption());
  }

  @Test
  public void testSetGracePeriod() {
    dockerKillCommand.setSignal(SIGNAL);
    assertEquals("kill", StringUtils.join(",",
        dockerKillCommand.getDockerCommandWithArguments()
            .get("docker-command")));
    assertEquals("foo", StringUtils.join(",",
        dockerKillCommand.getDockerCommandWithArguments().get("name")));
    assertEquals("SIGUSR2", StringUtils.join(",",
        dockerKillCommand.getDockerCommandWithArguments().get("signal")));
    assertEquals(3, dockerKillCommand.getDockerCommandWithArguments().size());
  }
}