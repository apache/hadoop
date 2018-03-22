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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext.Attribute;

import java.util.List;
import java.util.Map;

public final class LinuxContainerRuntimeConstants {
  private LinuxContainerRuntimeConstants() {
  }

  /**
   * Linux container runtime types for {@link DelegatingLinuxContainerRuntime}.
   */
  public enum RuntimeType {
    DEFAULT,
    DOCKER,
    JAVASANDBOX;
  }

  public static final Attribute<Map> LOCALIZED_RESOURCES = Attribute
      .attribute(Map.class, "localized_resources");
  public static final Attribute<List> CONTAINER_LAUNCH_PREFIX_COMMANDS =
      Attribute.attribute(List.class, "container_launch_prefix_commands");
  public static final Attribute<String> RUN_AS_USER =
      Attribute.attribute(String.class, "run_as_user");
  public static final Attribute<String> USER = Attribute.attribute(String.class,
      "user");
  public static final Attribute<String> APPID =
      Attribute.attribute(String.class, "appid");
  public static final Attribute<String> CONTAINER_ID_STR = Attribute
      .attribute(String.class, "container_id_str");
  public static final Attribute<Path> CONTAINER_WORK_DIR = Attribute
      .attribute(Path.class, "container_work_dir");
  public static final Attribute<Path> NM_PRIVATE_CONTAINER_SCRIPT_PATH =
      Attribute.attribute(Path.class, "nm_private_container_script_path");
  public static final Attribute<Path> NM_PRIVATE_TOKENS_PATH = Attribute
      .attribute(Path.class, "nm_private_tokens_path");
  public static final Attribute<Path> PID_FILE_PATH = Attribute.attribute(
      Path.class, "pid_file_path");
  public static final Attribute<List> LOCAL_DIRS = Attribute.attribute(
      List.class, "local_dirs");
  public static final Attribute<List> LOG_DIRS = Attribute.attribute(
      List.class, "log_dirs");
  public static final Attribute<List> FILECACHE_DIRS = Attribute.attribute(
      List.class, "filecache_dirs");
  public static final Attribute<List> USER_LOCAL_DIRS = Attribute.attribute(
      List.class, "user_local_dirs");
  public static final Attribute<List> CONTAINER_LOCAL_DIRS = Attribute
      .attribute(List.class, "container_local_dirs");
  public static final Attribute<List> USER_FILECACHE_DIRS = Attribute
      .attribute(List.class, "user_filecache_dirs");
  public static final Attribute<List> APPLICATION_LOCAL_DIRS = Attribute
      .attribute(List.class, "application_local_dirs");
  public static final Attribute<List> CONTAINER_LOG_DIRS = Attribute.attribute(
      List.class, "container_log_dirs");
  public static final Attribute<String> RESOURCES_OPTIONS = Attribute.attribute(
      String.class, "resources_options");
  public static final Attribute<String> TC_COMMAND_FILE = Attribute.attribute(
      String.class, "tc_command_file");
  public static final Attribute<List> CONTAINER_RUN_CMDS = Attribute.attribute(
      List.class, "container_run_cmds");
  public static final Attribute<String> CGROUP_RELATIVE_PATH = Attribute
      .attribute(String.class, "cgroup_relative_path");

  public static final Attribute<String> PID = Attribute.attribute(
      String.class, "pid");
  public static final Attribute<ContainerExecutor.Signal> SIGNAL = Attribute
      .attribute(ContainerExecutor.Signal.class, "signal");
}