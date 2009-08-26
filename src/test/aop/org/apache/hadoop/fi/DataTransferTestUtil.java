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

import org.apache.hadoop.fi.FiTestUtil.Action;
import org.apache.hadoop.fi.FiTestUtil.ActionContainer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for DataTransferProtocol related tests,
 * e.g. TestFiDataTransferProtocol.
 */
public class DataTransferTestUtil {
  protected static PipelineTest thepipelinetest;
  /** initialize pipeline test */
  public static PipelineTest initTest() {
    return thepipelinetest = new DataTransferTest();
  }
  /** get the pipeline test object */
  public static PipelineTest getPipelineTest() {
    return thepipelinetest;
  }
  /** get the pipeline test object cast to DataTransferTest */
  public static DataTransferTest getDataTransferTest() {
    return (DataTransferTest)getPipelineTest();
  }

  /**
   * The DataTransferTest class includes a pipeline
   * and some actions.
   */
  public static class DataTransferTest implements PipelineTest {
    private List<Pipeline> pipelines = new ArrayList<Pipeline>();
    private volatile boolean isSuccess = false;

    /** Simulate action for the receiverOpWriteBlock pointcut */
    public final ActionContainer<DatanodeID> fiReceiverOpWriteBlock
        = new ActionContainer<DatanodeID>();
    /** Simulate action for the callReceivePacket pointcut */
    public final ActionContainer<DatanodeID> fiCallReceivePacket
        = new ActionContainer<DatanodeID>();
    /** Simulate action for the statusRead pointcut */
    public final ActionContainer<DatanodeID> fiStatusRead
        = new ActionContainer<DatanodeID>();
    /** Verification action for the pipelineInitNonAppend pointcut */
    public final ActionContainer<Integer> fiPipelineInitErrorNonAppend
        = new ActionContainer<Integer>();
    /** Verification action for the pipelineErrorAfterInit pointcut */
    public final ActionContainer<Integer> fiPipelineErrorAfterInit
        = new ActionContainer<Integer>();

    /** Get test status */
    public boolean isSuccess() {
      return this.isSuccess;
    }

    /** Set test status */
    public void markSuccess() {
      this.isSuccess = true;
    }

    /** Initialize the pipeline. */
    public Pipeline initPipeline(LocatedBlock lb) {
      final Pipeline pl = new Pipeline(lb);
      if (pipelines.contains(pl)) {
        throw new IllegalStateException("thepipeline != null");
      }
      pipelines.add(pl);
      return pl;
    }

    /** Return the pipeline. */
    public Pipeline getPipeline(DatanodeID id) {
      if (pipelines == null) {
        throw new IllegalStateException("thepipeline == null");
      }
      StringBuilder dnString = new StringBuilder();
      for (Pipeline pipeline : pipelines) {
        for (DatanodeInfo dni : pipeline.getDataNodes())
          dnString.append(dni.getStorageID());
        if (dnString.toString().contains(id.getStorageID()))
          return pipeline;
      }
      return null;
    }
  }

  /** Action for DataNode */
  public static abstract class DataNodeAction implements Action<DatanodeID> {
    /** The name of the test */
    final String currentTest;
    /** The index of the datanode */
    final int index;

    /**
     * @param currentTest The name of the test
     * @param index The index of the datanode
     */
    protected DataNodeAction(String currentTest, int index) {
      this.currentTest = currentTest;
      this.index = index;
    }

    /** {@inheritDoc} */
    public String toString() {
      return currentTest + ", index=" + index;
    }

    /** {@inheritDoc}
     * @param datanodeID*/
    String toString(DatanodeID datanodeID) {
      return "FI: " + this + ", datanode="
          + datanodeID.getName();
    }
  }

  /** Throws OutOfMemoryError. */
  public static class OomAction extends DataNodeAction {
    /** Create an action for datanode i in the pipeline. */
    public OomAction(String currentTest, int i) {
      super(currentTest, i);
    }

    @Override
    public void run(DatanodeID id) {
      final DataTransferTest test = getDataTransferTest();
      final Pipeline p = test.getPipeline(id);
      if (!test.isSuccess() && p.contains(index, id)) {
        final String s = toString(id);
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
    public void run(DatanodeID id) throws DiskOutOfSpaceException {
      final DataTransferTest test = getDataTransferTest();
      final Pipeline p = test.getPipeline(id);
      if (p.contains(index, id)) {
        final String s = toString(id);
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
    public void run(DatanodeID id) {
      final DataTransferTest test = getDataTransferTest();
      final Pipeline p = test.getPipeline(id);
      if (!test.isSuccess() && p.contains(index, id)) {
        final String s = toString(id) + ", duration=" + duration;
        FiTestUtil.LOG.info(s);
        if (duration <= 0) {
          for(; true; FiTestUtil.sleep(1000)); //sleep forever
        } else {
          FiTestUtil.sleep(duration);
        }
      }
    }
  }

  /** Action for pipeline error verification */
  public static class VerificationAction implements Action<Integer> {
    /** The name of the test */
    final String currentTest;
    /** The error index of the datanode */
    final int errorIndex;

    /**
     * Create a verification action for errors at datanode i in the pipeline.
     * 
     * @param currentTest The name of the test
     * @param i The error index of the datanode
     */
    public VerificationAction(String currentTest, int i) {
      this.currentTest = currentTest;
      this.errorIndex = i;
    }

    /** {@inheritDoc} */
    public String toString() {
      return currentTest + ", errorIndex=" + errorIndex;
    }

    @Override
    public void run(Integer i) {
      if (i == errorIndex) {
        FiTestUtil.LOG.info(this + ", successfully verified.");
        getDataTransferTest().markSuccess();
      }
    }
  }
}