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

package org.apache.hadoop.streaming;

import java.io.*;
import java.util.*;

/**
 * A simulation of some Java SE 6 File methods.
 * http://java.sun.com/developer/technicalArticles/J2SE/Desktop/mustang/enhancements/
 *
 * Limitations of this version: requires Cygwin on Windows, does not perform SecurityManager checks,
 *  always returns true (success) without verifying that the operation worked.
 *
 * Note: not specifying ownerOnly maps to ownerOnly = false
 * From man chmod: If no user specs are given, the effect is as if `a' were given. 
 * This class is mainly used to change permissions when files are unjarred from the 
 * job.jar. The executable specified in the mappper/reducer is set to be executable 
 * using this class.
 */
public class MustangFile extends File {

  public MustangFile(File parent, String child) {
    super(parent, child);
  }

  public MustangFile(String pathname) {
    super(pathname);
  }

  public MustangFile(String parent, String child) {
    super(parent, child);
  }

  public boolean setReadable(boolean readable, boolean ownerOnly) {
    chmod("r", readable, ownerOnly);
    return SUCCESS;
  }

  public boolean setReadable(boolean readable) {
    chmod("r", readable, false);
    return SUCCESS;
  }

  public boolean setWritable(boolean writable, boolean ownerOnly) {
    chmod("w", writable, ownerOnly);
    return SUCCESS;
  }

  public boolean setWritable(boolean writable) {
    chmod("w", writable, false);
    return SUCCESS;
  }

  public boolean setExecutable(boolean executable, boolean ownerOnly) {
    chmod("x", executable, ownerOnly);
    return SUCCESS;
  }

  public boolean setExecutable(boolean executable) {
    chmod("x", executable, false);
    return SUCCESS;
  }

  void chmod(String perms, boolean plus, boolean ownerOnly) {
    String[] argv = new String[3];
    argv[0] = "/bin/chmod";
    String spec = ownerOnly ? "u" : "ugoa";
    spec += (plus ? "+" : "-");
    spec += perms;
    argv[1] = spec;
    argv[2] = getAbsolutePath();
    StreamUtil.exec(argv, System.err);
  }

  final static boolean SUCCESS = true;
}
