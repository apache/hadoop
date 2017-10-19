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
 * Tests the docker stop command and its command
 * line arguments.
 */
public class TestDockerStopCommand {

  private DockerStopCommand dockerStopCommand;

  private static final int GRACE_PERIOD = 10;
  private static final String CONTAINER_NAME = "foo";

  @Before
  public void setup() {
    dockerStopCommand = new DockerStopCommand(CONTAINER_NAME);
  }

  @Test
  public void testGetCommandOption() {
    assertEquals("stop", dockerStopCommand.getCommandOption());
  }

  @Test
  public void testSetGracePeriod() throws Exception {
    dockerStopCommand.setGracePeriod(GRACE_PERIOD);
    assertEquals("stop", StringUtils.join(",",
        dockerStopCommand.getDockerCommandWithArguments()
            .get("docker-command")));
    assertEquals("foo", StringUtils.join(",",
        dockerStopCommand.getDockerCommandWithArguments().get("name")));
    assertEquals("10", StringUtils.join(",",
        dockerStopCommand.getDockerCommandWithArguments().get("time")));
    assertEquals(3, dockerStopCommand.getDockerCommandWithArguments().size());
  }
}
