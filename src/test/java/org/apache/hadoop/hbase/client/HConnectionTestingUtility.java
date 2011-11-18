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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectionKey;
import org.mockito.Mockito;

/**
 * {@link HConnection} testing utility.
 */
public class HConnectionTestingUtility {
  /*
   * Not part of {@link HBaseTestingUtility} because this class is not
   * in same package as {@link HConnection}.  Would have to reveal ugly
   * {@link HConnectionManager} innards to HBaseTestingUtility to give it access.
   */
  /**
   * Get a Mocked {@link HConnection} that goes with the passed <code>conf</code>
   * configuration instance.  Minimally the mock will return
   * <code>conf</conf> when {@link HConnection#getConfiguration()} is invoked.
   * Be sure to shutdown the connection when done by calling
   * {@link HConnectionManager#deleteConnection(Configuration, boolean)} else it
   * will stick around; this is probably not what you want.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection getMockedConnection(final Configuration conf)
  throws ZooKeeperConnectionException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (HConnectionManager.HBASE_INSTANCES) {
      HConnectionImplementation connection =
        HConnectionManager.HBASE_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = Mockito.mock(HConnectionImplementation.class);
        Mockito.when(connection.getConfiguration()).thenReturn(conf);
        HConnectionManager.HBASE_INSTANCES.put(connectionKey, connection);
      }
      return connection;
    }
  }

  /**
   * Get a Mockito spied-upon {@link HConnection} that goes with the passed
   * <code>conf</code> configuration instance.
   * Be sure to shutdown the connection when done by calling
   * {@link HConnectionManager#deleteConnection(Configuration, boolean)} else it
   * will stick around; this is probably not what you want.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   * @see http://mockito.googlecode.com/svn/branches/1.6/javadoc/org/mockito/Mockito.html#spy(T)
   */
  public static HConnection getSpiedConnection(final Configuration conf)
  throws ZooKeeperConnectionException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (HConnectionManager.HBASE_INSTANCES) {
      HConnectionImplementation connection =
        HConnectionManager.HBASE_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = Mockito.spy(new HConnectionImplementation(conf, true));
        HConnectionManager.HBASE_INSTANCES.put(connectionKey, connection);
      }
      return connection;
    }
  }
}
