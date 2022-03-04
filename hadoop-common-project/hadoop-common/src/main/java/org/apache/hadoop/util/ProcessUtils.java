/*
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

package org.apache.hadoop.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Process related utilities.
 */
@InterfaceAudience.Private
public final class ProcessUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessUtils.class);

  private ProcessUtils() {
    // no-op
  }

  public static Integer getPid() {
    // JVM_PID can be exported in service start script
    String pidStr = System.getenv("JVM_PID");

    // In case if it is not set correctly, fallback to mxbean which is implementation specific.
    if (pidStr == null || pidStr.trim().isEmpty()) {
      String name = ManagementFactory.getRuntimeMXBean().getName();
      if (name != null) {
        int idx = name.indexOf("@");
        if (idx != -1) {
          pidStr = name.substring(0, name.indexOf("@"));
        }
      }
    }
    try {
      if (pidStr != null) {
        return Integer.valueOf(pidStr);
      }
    } catch (NumberFormatException ignored) {
      // ignore
    }
    return null;
  }

  public static Process runCmdAsync(List<String> cmd) {
    try {
      LOG.info("Running command async: {}", cmd);
      return new ProcessBuilder(cmd).inheritIO().start();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
