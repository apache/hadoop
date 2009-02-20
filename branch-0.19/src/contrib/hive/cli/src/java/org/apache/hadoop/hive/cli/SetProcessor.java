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

package org.apache.hadoop.hive.cli;

import java.util.*;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.CommandProcessor;

public class SetProcessor implements CommandProcessor {

  private static String prefix = "set: ";

  public static boolean getBoolean(String value) {
    if(value.equals("on") || value.equals("true"))
      return true;
    if(value.equals("off") || value.equals("false"))
      return false;
    throw new IllegalArgumentException(prefix + "'" + value + "' is not a boolean");
  }

  private void dumpOptions(Properties p) {
    SessionState ss = SessionState.get();

    ss.out.println("silent=" + (ss.getIsSilent() ? "on" : "off"));
    for(Object one: p.keySet()) {
      String oneProp = (String)one;
      String oneValue = p.getProperty(oneProp);
      ss.out.println(oneProp+"="+oneValue);
    }
  }

  private void dumpOption(Properties p, String s) {
    SessionState ss = SessionState.get();
    
    if(p.getProperty(s) != null) {
      ss.out.println(s+"="+p.getProperty(s));
    } else {
      ss.out.println(s+" is undefined");
    }
  }

  public int run(String command) {
    SessionState ss = SessionState.get();

    String nwcmd = command.trim();
    if(nwcmd.equals("")) {
      dumpOptions(ss.getConf().getChangedProperties());
      return 0;
    }

    if(nwcmd.equals("-v")) {
      dumpOptions(ss.getConf().getAllProperties());
      return 0;
    }

    String[] part = new String [2];

    int eqIndex = nwcmd.indexOf('=');
    if(eqIndex == -1) {
      // no equality sign - print the property out
      dumpOption(ss.getConf().getAllProperties(), nwcmd);
      return (0);
    } else if (eqIndex == nwcmd.length()-1) {
      part[0] = nwcmd.substring(0, nwcmd.length()-1);
      part[1] = "";
    } else {
      part[0] = nwcmd.substring(0, eqIndex);
      part[1] = nwcmd.substring(eqIndex+1);
    }

    try {
      if (part[0].equals("silent")) {
        boolean val = getBoolean(part[1]);
        ss.setIsSilent(val);
      } else {
        ss.getConf().set(part[0], part[1]);
      }
    } catch (IllegalArgumentException err) {
      ss.err.println(err.getMessage());
      return 1;
    }
    return 0;
  }
}
