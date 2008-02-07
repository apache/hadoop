/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.hql;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Shell;

/**
 * This is intended as a replacement for the default system manager. The goal is
 * to intercept System.exit calls and make it throw an exception instead so that
 * a System.exit in a jar command program does not fully terminate Shell.
 * 
 * @see ExitException
 */
public class HQLSecurityManager extends SecurityManager {

  /**
   * Override SecurityManager#checkExit. This throws an ExitException(status)
   * exception.
   * 
   * @param status the exit status
   */
  @SuppressWarnings("static-access")
  public void checkExit(int status) {
    if (status != Constants.FLAG_EXIT) {
      // throw new ExitException(status);

      // I didn't figure out How can catch the ExitException in shell main.
      // So, I just Re-launching the shell.
      Shell shell = new Shell();

      List<String> argList = new ArrayList<String>();
      argList.add(String.valueOf(Constants.FLAG_RELAUNCH));
      if(Shell.HTML_OPTION != null)
        argList.add(Shell.HTML_OPTION);
      if(Shell.IP != null && Shell.PORT != -1)
        argList.add("--master:" + Shell.IP + ":" + Shell.PORT);

      try {
        shell.main(argList.toArray(new String[] {}));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Override SecurityManager#checkPermission. This does nothing.
   * 
   * @param perm the requested permission.
   */
  public void checkPermission(Permission perm) {
  }
}
