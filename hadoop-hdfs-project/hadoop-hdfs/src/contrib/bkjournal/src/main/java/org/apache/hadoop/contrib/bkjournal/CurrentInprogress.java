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
package org.apache.hadoop.contrib.bkjournal;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Distributed write permission lock, using ZooKeeper. Read the version number
 * and return the current inprogress node path available in CurrentInprogress
 * path. If it exist, caller can treat that some other client already operating
 * on it. Then caller can take action. If there is no inprogress node exist,
 * then caller can treat that there is no client operating on it. Later same
 * caller should update the his newly created inprogress node path. At this
 * point, if some other activities done on this node, version number might
 * change, so update will fail. So, this read, update api will ensure that there
 * is only node can continue further after checking with CurrentInprogress.
 */

class CurrentInprogress {
  private static final String CONTENT_DELIMITER = ",";

  static final Log LOG = LogFactory.getLog(CurrentInprogress.class);

  private final ZooKeeper zkc;
  private final String currentInprogressNode;
  private volatile int versionNumberForPermission = -1;
  private static final int CURRENT_INPROGRESS_LAYOUT_VERSION = -1; 
  private final String hostName = InetAddress.getLocalHost().toString();

  CurrentInprogress(ZooKeeper zkc, String lockpath) throws IOException {
    this.currentInprogressNode = lockpath;
    this.zkc = zkc;
    try {
      Stat isCurrentInprogressNodeExists = zkc.exists(lockpath, false);
      if (isCurrentInprogressNodeExists == null) {
        try {
          zkc.create(lockpath, null, Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
          // Node might created by other process at the same time. Ignore it.
          if (LOG.isDebugEnabled()) {
            LOG.debug(lockpath + " already created by other process.", e);
          }
        }
      }
    } catch (KeeperException e) {
      throw new IOException("Exception accessing Zookeeper", e);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted accessing Zookeeper", ie);
    }
  }

  /**
   * Update the path with prepending version number and hostname
   * 
   * @param path
   *          - to be updated in zookeeper
   * @throws IOException
   */
  void update(String path) throws IOException {
    String content = CURRENT_INPROGRESS_LAYOUT_VERSION
        + CONTENT_DELIMITER + hostName + CONTENT_DELIMITER + path;
    try {
      zkc.setData(this.currentInprogressNode, content.getBytes(),
          this.versionNumberForPermission);
    } catch (KeeperException e) {
      throw new IOException("Exception when setting the data "
          + "[layout version number,hostname,inprogressNode path]= [" + content
          + "] to CurrentInprogress. ", e);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while setting the data "
          + "[layout version number,hostname,inprogressNode path]= [" + content
          + "] to CurrentInprogress", e);
    }
    LOG.info("Updated data[layout version number,hostname,inprogressNode path]"
        + "= [" + content + "] to CurrentInprogress");
  }

  /**
   * Read the CurrentInprogress node data from Zookeeper and also get the znode
   * version number. Return the 3rd field from the data. i.e saved path with
   * #update api
   * 
   * @return available inprogress node path. returns null if not available.
   * @throws IOException
   */
  String read() throws IOException {
    Stat stat = new Stat();
    byte[] data = null;
    try {
      data = zkc.getData(this.currentInprogressNode, false, stat);
    } catch (KeeperException e) {
      throw new IOException("Exception while reading the data from "
          + currentInprogressNode, e);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while reading data from "
          + currentInprogressNode, e);
    }
    this.versionNumberForPermission = stat.getVersion();
    if (data != null) {
      String stringData = new String(data);
      LOG.info("Read data[layout version number,hostname,inprogressNode path]"
          + "= [" + stringData + "] from CurrentInprogress");
      String[] contents = stringData.split(CONTENT_DELIMITER);
      assert contents.length == 3 : "As per the current data format, "
          + "CurrentInprogress node data should contain 3 fields. "
          + "i.e layout version number,hostname,inprogressNode path";
      String layoutVersion = contents[0];
      if (Long.valueOf(layoutVersion) > CURRENT_INPROGRESS_LAYOUT_VERSION) {
        throw new IOException(
            "Supported layout version of CurrentInprogress node is : "
                + CURRENT_INPROGRESS_LAYOUT_VERSION
                + " . Layout version of CurrentInprogress node in ZK is : "
                + layoutVersion);
      }
      String inprogressNodePath = contents[2];
      return inprogressNodePath;
    } else {
      LOG.info("No data available in CurrentInprogress");
    }
    return null;
  }

  /** Clear the CurrentInprogress node data */
  void clear() throws IOException {
    try {
      zkc.setData(this.currentInprogressNode, null, versionNumberForPermission);
    } catch (KeeperException e) {
      throw new IOException(
          "Exception when setting the data to CurrentInprogress node", e);
    } catch (InterruptedException e) {
      throw new IOException(
          "Interrupted when setting the data to CurrentInprogress node", e);
    }
    LOG.info("Cleared the data from CurrentInprogress");
  }

}
