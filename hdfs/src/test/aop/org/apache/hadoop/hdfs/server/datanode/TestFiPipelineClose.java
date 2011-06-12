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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.DataTransferTestUtil.DataNodeAction;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.fi.DataTransferTestUtil.DatanodeMarkingAction;
import org.apache.hadoop.fi.DataTransferTestUtil.IoeAction;
import org.apache.hadoop.fi.DataTransferTestUtil.OomAction;
import org.apache.hadoop.fi.DataTransferTestUtil.SleepAction;
import org.apache.hadoop.fi.FiTestUtil;
import org.apache.hadoop.fi.FiTestUtil.Action;
import org.apache.hadoop.fi.FiTestUtil.ConstraintSatisfactionAction;
import org.apache.hadoop.fi.FiTestUtil.MarkerConstraint;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.junit.Test;

/** Test DataTransferProtocol with fault injection. */
public class TestFiPipelineClose {
  private static void runPipelineCloseTest(String methodName,
      Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiPipelineClose.set(a);
    TestFiDataTransferProtocol.write1byte(methodName);
  }

  /**
   * Pipeline close:
   * DN0 never responses after received close request from client.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_36() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new SleepAction(methodName, 0, 0));
  }

  /**
   * Pipeline close:
   * DN1 never responses after received close request from client.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_37() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new SleepAction(methodName, 1, 0));
  }

  /**
   * Pipeline close:
   * DN2 never responses after received close request from client.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_38() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new SleepAction(methodName, 2, 0));
  }

  private static void run41_43(String name, int i) throws IOException {
    runPipelineCloseTest(name, new SleepAction(name, i, 3000));
  }

  private static void runPipelineCloseAck(String name, int i, DataNodeAction a
      ) throws IOException {
    FiTestUtil.LOG.info("Running " + name + " ...");
    final DataTransferTest t = (DataTransferTest)DataTransferTestUtil.initTest();
    final MarkerConstraint marker = new MarkerConstraint(name);
    t.fiPipelineClose.set(new DatanodeMarkingAction(name, i, marker));
    t.fiPipelineAck.set(new ConstraintSatisfactionAction<DatanodeID, IOException>(a, marker));
    TestFiDataTransferProtocol.write1byte(name);
  }

  private static void run39_40(String name, int i) throws IOException {
    runPipelineCloseAck(name, i, new SleepAction(name, i, 0));
  }

  /**
   * Pipeline close:
   * DN1 never responses after received close ack DN2.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_39() throws IOException {
    run39_40(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close:
   * DN0 never responses after received close ack DN1.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_40() throws IOException {
    run39_40(FiTestUtil.getMethodName(), 0);
  }
  
  /**
   * Pipeline close with DN0 very slow but it won't lead to timeout.
   * Client finishes close successfully.
   */
  @Test
  public void pipeline_Fi_41() throws IOException {
    run41_43(FiTestUtil.getMethodName(), 0);
  }

  /**
   * Pipeline close with DN1 very slow but it won't lead to timeout.
   * Client finishes close successfully.
   */
  @Test
  public void pipeline_Fi_42() throws IOException {
    run41_43(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close with DN2 very slow but it won't lead to timeout.
   * Client finishes close successfully.
   */
  @Test
  public void pipeline_Fi_43() throws IOException {
    run41_43(FiTestUtil.getMethodName(), 2);
  }

  /**
   * Pipeline close:
   * DN0 throws an OutOfMemoryException
   * right after it received a close request from client.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_44() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new OomAction(methodName, 0));
  }

  /**
   * Pipeline close:
   * DN1 throws an OutOfMemoryException
   * right after it received a close request from client.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_45() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new OomAction(methodName, 1));
  }

  /**
   * Pipeline close:
   * DN2 throws an OutOfMemoryException
   * right after it received a close request from client.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_46() throws IOException {
    final String methodName = FiTestUtil.getMethodName();
    runPipelineCloseTest(methodName, new OomAction(methodName, 2));
  }

  private static void run47_48(String name, int i) throws IOException {
    runPipelineCloseAck(name, i, new OomAction(name, i));
  }

  /**
   * Pipeline close:
   * DN1 throws an OutOfMemoryException right after
   * it received a close ack from DN2.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_47() throws IOException {
    run47_48(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close:
   * DN0 throws an OutOfMemoryException right after
   * it received a close ack from DN1.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_48() throws IOException {
    run47_48(FiTestUtil.getMethodName(), 0);
  }

  private static void runBlockFileCloseTest(String methodName,
      Action<DatanodeID, IOException> a) throws IOException {
    FiTestUtil.LOG.info("Running " + methodName + " ...");
    final DataTransferTest t = (DataTransferTest) DataTransferTestUtil
        .initTest();
    t.fiBlockFileClose.set(a);
    TestFiDataTransferProtocol.write1byte(methodName);
  }

  private static void run49_51(String name, int i) throws IOException {
    runBlockFileCloseTest(name, new IoeAction(name, i, "DISK ERROR"));
  }

  /**
   * Pipeline close:
   * DN0 throws a disk error exception when it is closing the block file.
   * Client gets an IOException and determine DN0 bad.
   */
  @Test
  public void pipeline_Fi_49() throws IOException {
    run49_51(FiTestUtil.getMethodName(), 0);
  }


  /**
   * Pipeline close:
   * DN1 throws a disk error exception when it is closing the block file.
   * Client gets an IOException and determine DN1 bad.
   */
  @Test
  public void pipeline_Fi_50() throws IOException {
    run49_51(FiTestUtil.getMethodName(), 1);
  }

  /**
   * Pipeline close:
   * DN2 throws a disk error exception when it is closing the block file.
   * Client gets an IOException and determine DN2 bad.
   */
  @Test
  public void pipeline_Fi_51() throws IOException {
    run49_51(FiTestUtil.getMethodName(), 2);
  }
}
