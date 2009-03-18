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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * An HMaster that runs out of memory.
 * Everytime a region server reports in, add to the retained heap of memory.
 * Needs to be started manually as in
 * <code>${HBASE_HOME}/bin/hbase ./bin/hbase org.apache.hadoop.hbase.OOMEHMaster start/code>.
 */
public class OOMEHMaster extends HMaster {
  private List<byte []> retainer = new ArrayList<byte[]>();
  
  public OOMEHMaster(HBaseConfiguration conf) throws IOException {
    super(conf);
  }
  
  @Override
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg[] msgs, 
    HRegionInfo[] mostLoadedRegions)
  throws IOException {
    // Retain 1M.
    this.retainer.add(new byte [1024 * 1024]);
    return super.regionServerReport(serverInfo, msgs, mostLoadedRegions);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    doMain(args, OOMEHMaster.class);
  }
}
