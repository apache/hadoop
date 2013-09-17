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
package org.apache.hadoop.fi;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.util.List;
import java.util.ArrayList;

public class Pipeline {
  private final List<String> datanodes = new ArrayList<String>();

  Pipeline(LocatedBlock lb) {
    for(DatanodeInfo d : lb.getLocations()) {
      datanodes.add(d.getName());
    }
  }

  /** Does the pipeline contains d? */
  public boolean contains(DatanodeID d) {
    return datanodes.contains(d.getName());
  }

  /** Does the pipeline contains d at the n th position? */
  public boolean contains(int n, DatanodeID d) {
    return d.getName().equals(datanodes.get(n));
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + datanodes;
  }
}
