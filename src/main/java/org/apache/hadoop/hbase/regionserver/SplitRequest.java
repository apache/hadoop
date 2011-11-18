/**
 * Copyright 2011 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Handles processing region splits. Put in a queue, owned by HRegionServer.
 */
class SplitRequest implements Runnable {
  static final Log LOG = LogFactory.getLog(SplitRequest.class);
  private final HRegion parent;
  private final byte[] midKey;
  private final HRegionServer server;

  SplitRequest(HRegion region, byte[] midKey, HRegionServer hrs) {
    Preconditions.checkNotNull(hrs);
    this.parent = region;
    this.midKey = midKey;
    this.server = hrs;
  }

  @Override
  public String toString() {
    return "regionName=" + parent + ", midKey=" + Bytes.toStringBinary(midKey);
  }

  @Override
  public void run() {
    if (this.server.isStopping() || this.server.isStopped()) {
      LOG.debug("Skipping split because server is stopping=" +
        this.server.isStopping() + " or stopped=" + this.server.isStopped());
      return;
    }
    try {
      final long startTime = System.currentTimeMillis();
      SplitTransaction st = new SplitTransaction(parent, midKey);
      // If prepare does not return true, for some reason -- logged inside in
      // the prepare call -- we are not ready to split just now. Just return.
      if (!st.prepare()) return;
      try {
        st.execute(this.server, this.server);
      } catch (Exception e) {
        try {
          LOG.info("Running rollback/cleanup of failed split of " +
            parent.getRegionNameAsString() + "; " + e.getMessage());
          if (st.rollback(this.server, this.server)) {
            LOG.info("Successful rollback of failed split of " +
              parent.getRegionNameAsString());
          } else {
            this.server.abort("Abort; we got an error after point-of-no-return");
          }
        } catch (RuntimeException ee) {
          String msg = "Failed rollback of failed split of " +
            parent.getRegionNameAsString() + " -- aborting server";
          // If failed rollback, kill this server to avoid having a hole in table.
          LOG.info(msg, ee);
          this.server.abort(msg);
        }
        return;
      }
      LOG.info("Region split, META updated, and report to master. Parent="
          + parent.getRegionInfo().getRegionNameAsString() + ", new regions: "
          + st.getFirstDaughter().getRegionNameAsString() + ", "
          + st.getSecondDaughter().getRegionNameAsString() + ". Split took "
          + StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
    } catch (IOException ex) {
      LOG.error("Split failed " + this, RemoteExceptionHandler
          .checkIOException(ex));
      server.checkFileSystem();
    }
  }
}