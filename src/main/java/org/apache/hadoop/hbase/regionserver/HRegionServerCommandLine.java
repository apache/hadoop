/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.util.ServerCommandLine;

/**
 * Class responsible for parsing the command line and starting the
 * RegionServer.
 */
public class HRegionServerCommandLine extends ServerCommandLine {
  private static final Log LOG = LogFactory.getLog(HRegionServerCommandLine.class);

  private final Class<? extends HRegionServer> regionServerClass;

  private static final String USAGE =
    "Usage: HRegionServer [-D conf.param=value] start";

  public HRegionServerCommandLine(Class<? extends HRegionServer> clazz) {
    this.regionServerClass = clazz;
  }

  protected String getUsage() {
    return USAGE;
  }

  private int start() throws Exception {
    Configuration conf = getConf();

    // If 'local', don't start a region server here. Defer to
    // LocalHBaseCluster. It manages 'local' clusters.
    if (LocalHBaseCluster.isLocal(conf)) {
      LOG.warn("Not starting a distinct region server because "
               + HConstants.CLUSTER_DISTRIBUTED + " is false");
    } else {
      logJVMInfo();
      HRegionServer hrs = HRegionServer.constructRegionServer(regionServerClass, conf);
      HRegionServer.startRegionServer(hrs);
    }
    return 0;
  }

  public int run(String args[]) throws Exception {
    if (args.length != 1) {
      usage(null);
      return -1;
    }

    String cmd = args[0];

    if ("start".equals(cmd)) {
      return start();
    } else if ("stop".equals(cmd)) {
      System.err.println(
        "To shutdown the regionserver run " +
        "bin/hbase-daemon.sh stop regionserver or send a kill signal to" +
        "the regionserver pid");
      return -1;
    } else {
      usage("Unknown command: " + args[0]);
      return -1;
    }
  }
}