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

package org.apache.hadoop.examples.terasort;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;

import com.google.common.base.Charsets;

class TeraScheduler {
  static String USE = "mapreduce.terasort.use.terascheduler";
  private static final Log LOG = LogFactory.getLog(TeraScheduler.class);
  private Split[] splits;
  private List<Host> hosts = new ArrayList<Host>();
  private int slotsPerHost;
  private int remainingSplits = 0;
  private FileSplit[] realSplits = null;

  static class Split {
    String filename;
    boolean isAssigned = false;
    List<Host> locations = new ArrayList<Host>();
    Split(String filename) {
      this.filename = filename;
    }
    public String toString() {
      StringBuffer result = new StringBuffer();
      result.append(filename);
      result.append(" on ");
      for(Host host: locations) {
        result.append(host.hostname);
        result.append(", ");
      }
      return result.toString();
    }
  }
  static class Host {
    String hostname;
    List<Split> splits = new ArrayList<Split>();
    Host(String hostname) {
      this.hostname = hostname;
    }
    public String toString() {
      StringBuffer result = new StringBuffer();
      result.append(splits.size());
      result.append(" ");
      result.append(hostname);
      return result.toString();
    }
  }

  List<String> readFile(String filename) throws IOException {
    List<String> result = new ArrayList<String>(10000);
    BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(filename), Charsets.UTF_8));
    String line = in.readLine();
    while (line != null) {
      result.add(line);
      line = in.readLine();
    }
    in.close();
    return result;
  }

  public TeraScheduler(String splitFilename, 
                       String nodeFilename) throws IOException {
    slotsPerHost = 4;
    // get the hosts
    Map<String, Host> hostIds = new HashMap<String,Host>();
    for(String hostName: readFile(nodeFilename)) {
      Host host = new Host(hostName);
      hosts.add(host);
      hostIds.put(hostName, host);
    }
    // read the blocks
    List<String> splitLines = readFile(splitFilename);
    splits = new Split[splitLines.size()];
    remainingSplits = 0;
    for(String line: splitLines) {
      StringTokenizer itr = new StringTokenizer(line);
      Split newSplit = new Split(itr.nextToken());
      splits[remainingSplits++] = newSplit;
      while (itr.hasMoreTokens()) {
        Host host = hostIds.get(itr.nextToken());
        newSplit.locations.add(host);
        host.splits.add(newSplit);
      }
    }
  }

  public TeraScheduler(FileSplit[] realSplits,
                       Configuration conf) throws IOException {
    this.realSplits = realSplits;
    this.slotsPerHost = conf.getInt(TTConfig.TT_MAP_SLOTS, 4);
    Map<String, Host> hostTable = new HashMap<String, Host>();
    splits = new Split[realSplits.length];
    for(FileSplit realSplit: realSplits) {
      Split split = new Split(realSplit.getPath().toString());
      splits[remainingSplits++] = split;
      for(String hostname: realSplit.getLocations()) {
        Host host = hostTable.get(hostname);
        if (host == null) {
          host = new Host(hostname);
          hostTable.put(hostname, host);
          hosts.add(host);
        }
        host.splits.add(split);
        split.locations.add(host);
      }
    }
  }

  Host pickBestHost() {
    Host result = null;
    int splits = Integer.MAX_VALUE;
    for(Host host: hosts) {
      if (host.splits.size() < splits) {
        result = host;
        splits = host.splits.size();
      }
    }
    if (result != null) {
      hosts.remove(result);
      LOG.debug("picking " + result);
    }
    return result;
  }

  void pickBestSplits(Host host) {
    int tasksToPick = Math.min(slotsPerHost, 
                               (int) Math.ceil((double) remainingSplits / 
                                               hosts.size()));
    Split[] best = new Split[tasksToPick];
    for(Split cur: host.splits) {
      LOG.debug("  examine: " + cur.filename + " " + cur.locations.size());
      int i = 0;
      while (i < tasksToPick && best[i] != null && 
             best[i].locations.size() <= cur.locations.size()) {
        i += 1;
      }
      if (i < tasksToPick) {
        for(int j = tasksToPick - 1; j > i; --j) {
          best[j] = best[j-1];
        }
        best[i] = cur;
      }
    }
    // for the chosen blocks, remove them from the other locations
    for(int i=0; i < tasksToPick; ++i) {
      if (best[i] != null) {
        LOG.debug(" best: " + best[i].filename);
        for (Host other: best[i].locations) {
          other.splits.remove(best[i]);
        }
        best[i].locations.clear();
        best[i].locations.add(host);
        best[i].isAssigned = true;
        remainingSplits -= 1;
      }
    }
    // for the non-chosen blocks, remove this host
    for(Split cur: host.splits) {
      if (!cur.isAssigned) {
        cur.locations.remove(host);
      }
    }
  }
  
  void solve() throws IOException {
    Host host = pickBestHost();
    while (host != null) {
      pickBestSplits(host);
      host = pickBestHost();
    }
  }

  /**
   * Solve the schedule and modify the FileSplit array to reflect the new
   * schedule. It will move placed splits to front and unplacable splits
   * to the end.
   * @return a new list of FileSplits that are modified to have the
   *    best host as the only host.
   * @throws IOException
   */
  public List<InputSplit> getNewFileSplits() throws IOException {
    solve();
    FileSplit[] result = new FileSplit[realSplits.length];
    int left = 0;
    int right = realSplits.length - 1;
    for(int i=0; i < splits.length; ++i) {
      if (splits[i].isAssigned) {
        // copy the split and fix up the locations
        String[] newLocations = {splits[i].locations.get(0).hostname};
        realSplits[i] = new FileSplit(realSplits[i].getPath(),
            realSplits[i].getStart(), realSplits[i].getLength(), newLocations);
        result[left++] = realSplits[i];
      } else {
        result[right--] = realSplits[i];
      }
    }
    List<InputSplit> ret = new ArrayList<InputSplit>();
    for (FileSplit fs : result) {
      ret.add(fs);
    }
    return ret;
  }

  public static void main(String[] args) throws IOException {
    TeraScheduler problem = new TeraScheduler("block-loc.txt", "nodes");
    for(Host host: problem.hosts) {
      System.out.println(host);
    }
    LOG.info("starting solve");
    problem.solve();
    List<Split> leftOvers = new ArrayList<Split>();
    for(int i=0; i < problem.splits.length; ++i) {
      if (problem.splits[i].isAssigned) {
        System.out.println("sched: " + problem.splits[i]);        
      } else {
        leftOvers.add(problem.splits[i]);
      }
    }
    for(Split cur: leftOvers) {
      System.out.println("left: " + cur);
    }
    System.out.println("left over: " + leftOvers.size());
    LOG.info("done");
  }

}
