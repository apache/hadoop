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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.FiHFlushTestUtil;
import org.apache.hadoop.fi.FiHFlushTestUtil.DerrAction;
import org.apache.hadoop.fi.FiHFlushTestUtil.HFlushTest;
import org.apache.hadoop.fi.FiTestUtil;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.io.IOException;

/** Class provides basic fault injection tests according to the test plan
    of HDFS-265
 */
public class TestFiHFlush {
  
  /** Methods initializes a test and sets required actions to be used later by
   * an injected advice
   * @param conf mini cluster configuration
   * @param methodName String representation of a test method invoking this 
   * method
   * @param block_size needed size of file's block
   * @param a is an action to be set for the set
   * @throws IOException in case of any errors
   */
  private static void runDiskErrorTest (final Configuration conf, 
      final String methodName, final int block_size, DerrAction a, int index,
      boolean trueVerification)
      throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final HFlushTest hft = (HFlushTest) FiHFlushTestUtil.initTest();
    hft.fiCallHFlush.set(a);
    hft.fiErrorOnCallHFlush.set(new DataTransferTestUtil.VerificationAction(methodName, index));
    TestHFlush.doTheJob(conf, methodName, block_size, (short)3);
    if (trueVerification)
      assertTrue("Some of expected conditions weren't detected", hft.isSuccess());
  }
  
  /** The tests calls 
   * {@link #runDiskErrorTest(Configuration, String, int, DerrAction, int, boolean)}
   * to make a number of writes within a block boundaries.
   * Although hflush() is called the test shouldn't expect an IOException
   * in this case because the invocation is happening after write() call 
   * is complete when pipeline doesn't exist anymore.
   * Thus, injected fault won't be triggered for 0th datanode
   */
  @Test
  public void hFlushFi01_a() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runDiskErrorTest(new HdfsConfiguration(), methodName, 
        AppendTestUtil.BLOCK_SIZE, new DerrAction(methodName, 0), 0, false);
  }

  /** The tests calls 
   * {@link #runDiskErrorTest(Configuration, String, int, DerrAction, int, boolean)}
   * to make a number of writes across a block boundaries.
   * hflush() is called after each write() during a pipeline life time.
   * Thus, injected fault ought to be triggered for 0th datanode
   */
  @Test
  public void hFlushFi01_b() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    runDiskErrorTest(conf, methodName, 
        customBlockSize, new DerrAction(methodName, 0), 0, true);
  }
  
  /** Similar to {@link #hFlushFi01_b()} but writing happens
   * across block and checksum's boundaries
   */
  @Test
  public void hFlushFi01_c() throws Exception { 
    final String methodName = FiTestUtil.getMethodName();
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 400;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    runDiskErrorTest(conf, methodName, 
        customBlockSize, new DerrAction(methodName, 0), 0, true);
  }

  /** Similar to {@link #hFlushFi01_a()} but for a pipeline's 1st datanode
   */
  @Test
  public void hFlushFi02_a() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runDiskErrorTest(new HdfsConfiguration(), methodName,
        AppendTestUtil.BLOCK_SIZE, new DerrAction(methodName, 1), 1, false);
  }

  /** Similar to {@link #hFlushFi01_b()} but for a pipeline's 1st datanode
   */
  @Test
  public void hFlushFi02_b() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    runDiskErrorTest(conf, methodName,
        customBlockSize, new DerrAction(methodName, 1), 1, true);
  }

  /** Similar to {@link #hFlushFi01_c()} but for a pipeline's 1st datanode
   */
  @Test
  public void hFlushFi02_c() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 400;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    runDiskErrorTest(conf, methodName,
        customBlockSize, new DerrAction(methodName, 1), 1, true);
  }
  
  /** Similar to {@link #hFlushFi01_a()} but for a pipeline's 2nd datanode
   */
  @Test
  public void hFlushFi03_a() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runDiskErrorTest(new HdfsConfiguration(), methodName,
        AppendTestUtil.BLOCK_SIZE, new DerrAction(methodName, 2), 2, false);
  }
  
  /** Similar to {@link #hFlushFi01_b()} but for a pipeline's 2nd datanode
   */
  @Test
  public void hFlushFi03_b() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    runDiskErrorTest(conf, methodName,
        customBlockSize, new DerrAction(methodName, 2), 2, true);
  }

  /** Similar to {@link #hFlushFi01_c()} but for a pipeline's 2nd datanode
   */
  @Test
  public void hFlushFi03_c() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 400;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    runDiskErrorTest(conf, methodName,
        customBlockSize, new DerrAction(methodName, 2), 2, true);
  }
}
