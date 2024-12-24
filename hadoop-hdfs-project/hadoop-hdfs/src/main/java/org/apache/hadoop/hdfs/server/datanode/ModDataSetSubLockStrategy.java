/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ModDataSetSubLockStrategy implements DataSetSubLockStrategy {
  public static final Logger LOG = LoggerFactory.getLogger(DataSetSubLockStrategy.class);

  private static final String LOCK_NAME_PERFIX = "SubLock";
  private long modFactor;

  public ModDataSetSubLockStrategy(long mod) {
    if (mod <= 0) {
      mod = 1L;
    }
    this.modFactor = mod;
  }

  @Override
  public String blockIdToSubLock(long blockid) {
    return LOCK_NAME_PERFIX + (blockid % modFactor);
  }

  @Override
  public List<String> getAllSubLockName() {
    List<String> res = new ArrayList<>();
    for (long i = 0L; i < modFactor; i++) {
      res.add(LOCK_NAME_PERFIX + i);
    }
    return res;
  }
}
