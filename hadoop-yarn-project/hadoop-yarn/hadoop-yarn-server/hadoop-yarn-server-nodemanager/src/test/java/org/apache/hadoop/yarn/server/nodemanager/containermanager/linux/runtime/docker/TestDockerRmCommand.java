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

import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the docker rm command and any command
 * line arguments.
 */
public class TestDockerRmCommand {

  private DockerRmCommand dockerRmCommand;
  private DockerRmCommand dockerRmCommandWithCgroupArg;
  private DockerRmCommand dockerRmCommandWithEmptyCgroupArg;

  private static final String CONTAINER_NAME = "foo";
  private static final String CGROUP_HIERARCHY_NAME = "hadoop-yarn";

  @Before
  public void setUp() {
    dockerRmCommand = new DockerRmCommand(CONTAINER_NAME, null);
    dockerRmCommandWithCgroupArg =
        new DockerRmCommand(CONTAINER_NAME, CGROUP_HIERARCHY_NAME);
    dockerRmCommandWithEmptyCgroupArg =
        new DockerRmCommand(CONTAINER_NAME, "");
  }

  @Test
  public void testGetCommandOption() {
    assertEquals("rm", dockerRmCommand.getCommandOption());
  }

  @Test
  public void testGetCommandWithArguments() {
    assertEquals("rm", StringUtils.join(",",
        dockerRmCommand.getDockerCommandWithArguments().get("docker-command")));
    assertEquals("foo", StringUtils.join(",",
        dockerRmCommand.getDockerCommandWithArguments().get("name")));
    assertEquals(2, dockerRmCommand.getDockerCommandWithArguments().size());
  }

  @Test
  public void testGetCommandWithCgroup() {
    assertEquals("rm", StringUtils.join(",",
        dockerRmCommandWithCgroupArg.getDockerCommandWithArguments()
            .get("docker-command")));
    assertEquals("foo", StringUtils.join(",",
        dockerRmCommandWithCgroupArg.getDockerCommandWithArguments()
            .get("name")));
    assertEquals(CGROUP_HIERARCHY_NAME, StringUtils.join(",",
        dockerRmCommandWithCgroupArg.getDockerCommandWithArguments()
            .get("hierarchy")));
    assertEquals(3,
        dockerRmCommandWithCgroupArg.getDockerCommandWithArguments().size());
  }

  @Test
  public void testGetCommandWithEmptyCgroup() {
    assertEquals("rm", StringUtils.join(",",
        dockerRmCommandWithEmptyCgroupArg
            .getDockerCommandWithArguments().get("docker-command")));
    assertEquals("foo", StringUtils.join(",",
        dockerRmCommandWithEmptyCgroupArg
            .getDockerCommandWithArguments().get("name")));
    assertEquals(2, dockerRmCommandWithEmptyCgroupArg.
        getDockerCommandWithArguments().size());
  }
}
