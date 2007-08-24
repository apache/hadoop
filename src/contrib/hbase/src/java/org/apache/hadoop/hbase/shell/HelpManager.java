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
package org.apache.hadoop.hbase.shell;

import java.util.HashMap;
import java.util.Map;

/**
 * Prints a usage message for the program to the given stream.
 */
public class HelpManager {
  /** application name */
  public static final String APP_NAME = "HBase Shell";

  /** version of the code */
  public static final String APP_VERSION = "0.0.1";

  /** help contents map */
  public static final Map<String, String[]> help = new HashMap<String, String[]>();

  public HelpManager() {
    help.putAll(HelpContents.Load());
  }

  /** Print out the program version. */
  public void printVersion() {
    ClearCommand.clear();
    System.out.println(APP_NAME + ", " + APP_VERSION + " version.\n"
        + "Copyright (c) 2007 by udanax, "
        + "licensed to Apache Software Foundation.\n"
        + "Type 'help;' for usage.\n");
  }

  public static void printHelp(String cmd) {
    if (cmd.equals("")) {
      System.out.println("Type 'help <command>;' to see command-specific "
          + "usage.\n");
      for (Map.Entry<String, String[]> helpMap : help.entrySet()) {
        wrapping(helpMap.getKey(), helpMap.getValue(), false);
      }
    } else {
      if (help.containsKey(cmd.toUpperCase())) {
        String[] msg = help.get(cmd.toUpperCase());
        wrapping(cmd.toUpperCase(), msg, true);
      } else {
        System.out.println("Unknown Command : Type 'help' for usage.");
      }
    }
  }

  public static void wrapping(String cmd, String[] cmdType, boolean example) {
    System.out.printf("%-10s", cmd);
    if (cmdType[0].length() > 55) {
      System.out.println(cmdType[0].substring(0, 55));
      System.out.printf("%13s", "");
      System.out.println(cmdType[0].substring(55, cmdType[1].length()));
    } else {
      System.out.println(cmdType[0]);
    }

    if (example)
      System.out.println("\n>>> " + cmdType[1]);
  }
}
