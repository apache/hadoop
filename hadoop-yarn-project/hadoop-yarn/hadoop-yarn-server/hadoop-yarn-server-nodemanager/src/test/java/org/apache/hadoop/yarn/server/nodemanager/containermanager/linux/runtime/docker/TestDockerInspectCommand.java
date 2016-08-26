/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the docker inspect command and its command
 * line arguments.
 */
public class TestDockerInspectCommand {

  private DockerInspectCommand dockerInspectCommand;

  private static final String CONTAINER_NAME = "foo";

  @Before
  public void setup() {
    dockerInspectCommand = new DockerInspectCommand(CONTAINER_NAME);
  }

  @Test
  public void testGetCommandOption() {
    assertEquals("inspect", dockerInspectCommand.getCommandOption());
  }

  @Test
  public void testGetContainerStatus() throws Exception {
    dockerInspectCommand.getContainerStatus();
    assertEquals("inspect --format='{{.State.Status}}' foo",
        dockerInspectCommand.getCommandWithArguments());
  }

  @Test
  public void testGetIpAndHost() throws Exception {
    dockerInspectCommand.getIpAndHost();
    assertEquals(
        "inspect --format='{{range(.NetworkSettings.Networks)}}{{.IPAddress}}"
            + ",{{end}}{{.Config.Hostname}}' foo",
        dockerInspectCommand.getCommandWithArguments());
  }
}