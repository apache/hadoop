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

package org.apache.hadoop.net;

import java.util.*;
import java.io.*;
import java.net.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.conf.*;

/**
 * This class implements the {@link DNSToSwitchMapping} interface using a 
 * script configured via topology.script.file.name .
 */
public final class ScriptBasedMapping implements Configurable, 
DNSToSwitchMapping
{
  private String scriptName;
  private Configuration conf;
  private int maxArgs; //max hostnames per call of the script
  private Map<String, String> cache = new TreeMap<String, String>();
  private static Log LOG = 
    LogFactory.getLog("org.apache.hadoop.net.ScriptBasedMapping");
  public void setConf (Configuration conf) {
    this.scriptName = conf.get("topology.script.file.name");
    this.maxArgs = conf.getInt("topology.script.number.args", 20);
    this.conf = conf;
  }
  public Configuration getConf () {
    return conf;
  }

  public ScriptBasedMapping() {}
  
  public List<String> resolve(List<String> names) {
    List <String> m = new ArrayList<String>(names.size());
    
    if (scriptName == null) {
      for (int i = 0; i < names.size(); i++) {
        m.add(NetworkTopology.DEFAULT_RACK);
      }
      return m;
    }
    List<String> hosts = new ArrayList<String>(names.size());
    for (String name : names) {
      name = getHostName(name);
      if (cache.get(name) == null) {
        hosts.add(name);
      } 
    }
    
    int i = 0;
    String output = runResolveCommand(hosts);
    if (output != null) {
      StringTokenizer allSwitchInfo = new StringTokenizer(output);
      while (allSwitchInfo.hasMoreTokens()) {
        String switchInfo = allSwitchInfo.nextToken();
        cache.put(hosts.get(i++), switchInfo);
      }
    }
    for (String name : names) {
      //now everything is in the cache
      name = getHostName(name);
      if (cache.get(name) != null) {
        m.add(cache.get(name));
      } else { //resolve all or nothing
        return null;
      }
    }
    return m;
  }
  
  private String runResolveCommand(List<String> args) {
    InetAddress ipaddr = null;
    int loopCount = 0;
    if (args.size() == 0) {
      return null;
    }
    StringBuffer allOutput = new StringBuffer();
    int numProcessed = 0;
    while (numProcessed != args.size()) {
      int start = maxArgs * loopCount;
      List <String> cmdList = new ArrayList<String>();
      cmdList.add(scriptName);
      for (numProcessed = start; numProcessed < (start + maxArgs) && 
           numProcessed < args.size(); numProcessed++) {
        try {
          ipaddr = InetAddress.getByName(args.get(numProcessed));
        } catch (UnknownHostException uh) {
          return null;
        }
        cmdList.add(ipaddr.getHostAddress()); 
      }
      File dir = null;
      String userDir;
      if ((userDir = System.getProperty("user.dir")) != null) {
        dir = new File(userDir);
      }
      ShellCommandExecutor s = new ShellCommandExecutor(
                                   cmdList.toArray(new String[0]), dir);
      try {
        s.execute();
        allOutput.append(s.getOutput() + " ");
      } catch (Exception e) {
        LOG.warn(StringUtils.stringifyException(e));
        return null;
      }
      loopCount++; 
    }
    return allOutput.toString();
  }
  private String getHostName(String hostWithPort) {
    int j;
    if ((j = hostWithPort.indexOf(':')) != -1) {
      hostWithPort = hostWithPort.substring(0, j);
    }
    return hostWithPort;
  }
}
