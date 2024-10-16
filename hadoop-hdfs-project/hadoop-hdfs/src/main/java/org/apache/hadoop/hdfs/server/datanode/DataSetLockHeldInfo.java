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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;


public class DataSetLockHeldInfo {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetLockHeldInfo.class);

  private List<LockHeldInfo> lockInfo = new LinkedList<>();

  public LockHeldInfo addNewLockInfo(boolean isBpLevel, boolean isReadLock) {
    if (isBpLevel) {
      LockHeldInfo lockHeldInfo = new LockHeldInfo();
      lockHeldInfo.setBpLevel(isBpLevel);
      lockHeldInfo.setReadLock(isReadLock);
      lockInfo.add(lockHeldInfo);
    }
    validateLockInfo();
    return lockInfo.get(lockInfo.size() - 1);
  }

  public LockHeldInfo getLastLockInfo() {
    validateLockInfo();
    return lockInfo.get(lockInfo.size() - 1);
  }

  private void validateLockInfo() {
    if (lockInfo.size() <= 0) {
      LOG.error("Found inValidate lock held info, " +
          StringUtils.getStackTrace(Thread.currentThread()));
    }
  }

  public void printHeldInfo(long timeThreshold) {
    if (lockInfo.size() <= 0) {
      return;
    }
    LockHeldInfo lockHeldInfo = lockInfo.remove(lockInfo.size() - 1);
    if (lockHeldInfo.getLockHeldInterval() > timeThreshold) {
      LOG.warn(lockHeldInfo.toString());
    }
  }

  public int getLockInfoSize() {
    return lockInfo.size();
  }
}
