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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fi.FiTestUtil.Action;
import org.apache.hadoop.fi.FiTestUtil.ActionContainer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * Utilities for DataTransferProtocol related tests,
 * e.g. TestFiDataTransferProtocol.
 */
public class DataTransferTestUtil {
  private static DataTransferTest thepipelinetest;
  /** initialize pipeline test */
  public static DataTransferTest initTest() {
    return thepipelinetest = new DataTransferTest();
  }
  /** get the pipeline test object */
  public static DataTransferTest getPipelineTest() {
    return thepipelinetest;
  }

  /**
   * The DataTransferTest class includes a pipeline
   * and some actions.
   */
  public static class DataTransferTest {
    private Pipeline thepipeline;
    /** Simulate action for the receiverOpWriteBlock pointcut */
    public final ActionContainer<DataNode> fiReceiverOpWriteBlock
        = new ActionContainer<DataNode>();
    /** Simulate action for the callReceivePacket pointcut */
    public final ActionContainer<DataNode> fiCallReceivePacket
        = new ActionContainer<DataNode>();
    /** Simulate action for the statusRead pointcut */
    public final ActionContainer<DataNode> fiStatusRead
        = new ActionContainer<DataNode>();

    /** Initialize the pipeline. */
    public Pipeline initPipeline(LocatedBlock lb) {
      if (thepipeline != null) {
        throw new IllegalStateException("thepipeline != null");
      }
      return thepipeline = new Pipeline(lb);
    }

    /** Return the pipeline. */
    public Pipeline getPipeline() {
      if (thepipeline == null) {
        throw new IllegalStateException("thepipeline == null");
      }
      return thepipeline;
    }
  }

  /** A pipeline contains a list of datanodes. */
  public static class Pipeline {
    private final List<String> datanodes = new ArrayList<String>();
    
    private Pipeline(LocatedBlock lb) {
      for(DatanodeInfo d : lb.getLocations()) {
        datanodes.add(d.getName());
      }
    }

    /** Does the pipeline contains d at the n th position? */
    public boolean contains(int n, DatanodeID d) {
      return d.getName().equals(datanodes.get(n));
    }

    /** {@inheritDoc} */
    public String toString() {
      return getClass().getSimpleName() + datanodes;
    }
  }

  /** Action for DataNode */
  public static abstract class DataNodeAction implements Action<DataNode> {
    /** The name of the test */
    final String currentTest;
    /** The index of the datanode */
    final int index;

    /**
     * @param currentTest The name of the test
     * @param index The index of the datanode
     */
    private DataNodeAction(String currentTest, int index) {
      this.currentTest = currentTest;
      this.index = index;
    }

    /** {@inheritDoc} */
    public String toString() {
      return currentTest + ", index=" + index;
    }

    /** {@inheritDoc} */
    String toString(DataNode datanode) {
      return "FI: " + this + ", datanode="
          + datanode.getDatanodeRegistration().getName();
    }
  }

  /** Throws OutOfMemoryError. */
  public static class OomAction extends DataNodeAction {
    /** Create an action for datanode i in the pipeline. */
    public OomAction(String currentTest, int i) {
      super(currentTest, i);
    }

    @Override
    public void run(DataNode datanode) {
      final Pipeline p = getPipelineTest().getPipeline();
      if (p.contains(index, datanode.getDatanodeRegistration())) {
        final String s = toString(datanode);
        FiTestUtil.LOG.info(s);
        throw new OutOfMemoryError(s);
      }
    }
  }

  /** Throws DiskOutOfSpaceException. */
  public static class DoosAction extends DataNodeAction {
    /** Create an action for datanode i in the pipeline. */
    public DoosAction(String currentTest, int i) {
      super(currentTest, i);
    }

    @Override
    public void run(DataNode datanode) throws DiskOutOfSpaceException {
      final Pipeline p = getPipelineTest().getPipeline();
      if (p.contains(index, datanode.getDatanodeRegistration())) {
        final String s = toString(datanode);
        FiTestUtil.LOG.info(s);
        throw new DiskOutOfSpaceException(s);
      }
    }
  }

  /**
   * Sleep some period of time so that it slows down the datanode
   * or sleep forever so that datanode becomes not responding.
   */
  public static class SleepAction extends DataNodeAction {
    /** In milliseconds, duration <= 0 means sleeping forever.*/
    final long duration;

    /**
     * Create an action for datanode i in the pipeline.
     * @param duration In milliseconds, duration <= 0 means sleeping forever.
     */
    public SleepAction(String currentTest, int i, long duration) {
      super(currentTest, i);
      this.duration = duration;
    }

    @Override
    public void run(DataNode datanode) {
      final Pipeline p = getPipelineTest().getPipeline();
      if (p.contains(index, datanode.getDatanodeRegistration())) {
        final String s = toString(datanode) + ", duration=" + duration;
        FiTestUtil.LOG.info(s);
        if (duration <= 0) {
          for(; true; FiTestUtil.sleep(1000)); //sleep forever
        } else {
          FiTestUtil.sleep(duration);
        }
      }
    }
  }
}