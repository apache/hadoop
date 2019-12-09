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
package org.apache.hadoop.test;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for stat/permission utility methods. Forks processes to query
 * permission info.
 */
public class StatUtils {
  public static class Permission {
    private String owner;
    private String group;
    private FsPermission fsPermission;

    public Permission(String owner, String group, FsPermission fsPermission) {
      this.owner = owner;
      this.group = group;
      this.fsPermission = fsPermission;
    }

    public String getOwner() {
      return owner;
    }

    public String getGroup() {
      return group;
    }

    public FsPermission getFsPermission() {
      return fsPermission;
    }
  }

  public static Permission getPermissionFromProcess(String filePath)
      throws Exception {
    String[] shellCommand = Shell.getGetPermissionCommand();
    String sPerm = getPermissionStringFromProcess(shellCommand, filePath);

    StringTokenizer tokenizer =
        new StringTokenizer(sPerm, Shell.TOKEN_SEPARATOR_REGEX);
    String symbolicPermission = tokenizer.nextToken();
    tokenizer.nextToken(); // skip hard link
    String owner = tokenizer.nextToken();
    String group = tokenizer.nextToken();
    if (Shell.WINDOWS) {
      owner = removeDomain(owner);
      group = removeDomain(group);
    }

    Permission permission =
        new Permission(owner, group, FsPermission.valueOf(symbolicPermission));

    return permission;
  }

  public static void setPermissionFromProcess(String chmod, String filePath)
      throws Exception {
    setPermissionFromProcess(chmod, false, filePath);
  }

  public static void setPermissionFromProcess(String chmod, boolean recursive,
      String filePath) throws Exception {
    String[] shellCommand = Shell.getSetPermissionCommand(chmod, recursive);
    getPermissionStringFromProcess(shellCommand, filePath);
  }

  private static String removeDomain(String str) {
    int index = str.indexOf("\\");
    if (index != -1) {
      str = str.substring(index + 1);
    }
    return str;
  }

  private static String getPermissionStringFromProcess(String[] shellCommand,
      String testFilePath) throws Exception {
    List<String> cmd = new ArrayList(Arrays.asList(shellCommand));
    cmd.add(testFilePath);

    ProcessBuilder processBuilder = new ProcessBuilder(cmd);
    Process process = processBuilder.start();

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.awaitTermination(2000, TimeUnit.MILLISECONDS);
    try {
      Future<String> future =
          executorService.submit(() -> new BufferedReader(
              new InputStreamReader(process.getInputStream(),
                  Charset.defaultCharset())).lines().findFirst().orElse(""));
      return future.get();
    } finally {
      process.destroy();
      executorService.shutdown();
    }
  }
}
