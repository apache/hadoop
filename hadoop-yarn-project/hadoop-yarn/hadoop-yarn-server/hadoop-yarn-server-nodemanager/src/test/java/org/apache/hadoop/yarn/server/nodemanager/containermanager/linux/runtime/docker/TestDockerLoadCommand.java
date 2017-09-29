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

import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests the docker load command and its command
 * line arguments.
 */
public class TestDockerLoadCommand {
  private DockerLoadCommand dockerLoadCommand;

  private static final String LOCAL_IMAGE_NAME = "foo";

  @Before
  public void setup() {
    dockerLoadCommand = new DockerLoadCommand(LOCAL_IMAGE_NAME);
  }

  @Test
  public void testGetCommandOption() {
    assertEquals("load", dockerLoadCommand.getCommandOption());
  }

  @Test
  public void testGetCommandWithArguments() {
    assertEquals("load", StringUtils.join(",",
        dockerLoadCommand.getDockerCommandWithArguments()
            .get("docker-command")));
    assertEquals("foo", StringUtils.join(",",
        dockerLoadCommand.getDockerCommandWithArguments().get("image")));
    assertEquals(2, dockerLoadCommand.getDockerCommandWithArguments().size());
  }
}
