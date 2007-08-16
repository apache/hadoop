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

package org.apache.hadoop.eclipse;

import java.util.Properties;

import org.eclipse.core.runtime.Platform;

import com.jcraft.jsch.JSch;

/**
 * Creates a JSCH object so that we can use the JSCH methods for connecting to
 * remote servers via SSH/SCP.
 */

public class JSchUtilities {

  static String SSH_HOME_DEFAULT = null;
  static {
    String ssh_dir_name = ".ssh"; //$NON-NLS-1$

    // Windows doesn't like files or directories starting with a dot.
    if (Platform.getOS().equals(Platform.OS_WIN32)) {
      ssh_dir_name = "ssh"; //$NON-NLS-1$
    }

    SSH_HOME_DEFAULT = System.getProperty("user.home"); //$NON-NLS-1$
    if (SSH_HOME_DEFAULT != null) {
      SSH_HOME_DEFAULT = SSH_HOME_DEFAULT + java.io.File.separator
          + ssh_dir_name;
    } else {

    }
  }

  public synchronized static JSch createJSch() {

    // IPreferenceStore store = CVSSSH2Plugin.getDefault().getPreferenceStore();
    // String ssh_home = store.getString(SSH_HOME_DEFAULT);
    String ssh_home = SSH_HOME_DEFAULT;

    Properties props = new Properties();
    props.setProperty("StrictHostKeyChecking", "no");

    JSch jsch = new JSch();
    JSch.setConfig(props);
    /*
     * JSch.setLogger(new Logger() { public boolean isEnabled(int level) {
     * return true; }
     * 
     * public void log(int level, String message) { System.out.println("JSCH
     * Level " + level + ": " + message); } });
     */

    try {
      java.io.File file;
      file = new java.io.File(ssh_home, "known_hosts"); //$NON-NLS-1$
      jsch.setKnownHosts(file.getPath());
    } catch (Exception e) {
    }

    return jsch;
  }
}
