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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fi.FiTestUtil.Action;
import org.apache.hadoop.fi.FiTestUtil.ActionContainer;
import org.apache.hadoop.fi.FiTestUtil.ConstraintSatisfactionAction;
import org.apache.hadoop.fi.FiTestUtil.CountdownConstraint;
import org.apache.hadoop.fi.FiTestUtil.MarkerConstraint;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

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
    private final List<Pipeline> pipelines = new ArrayList<Pipeline>();
    private volatile boolean isSuccess = false;

    /** Simulate action for the receiverOpWriteBlock pointcut */
    public final ActionContainer<DatanodeID, IOException> fiReceiverOpWriteBlock
        = new ActionContainer<DatanodeID, IOException>();
    /** Simulate action for the callReceivePacket pointcut */
    public final ActionContainer<DatanodeID, IOException> fiCallReceivePacket
        = new ActionContainer<DatanodeID, IOException>();
    /** Simulate action for the callWritePacketToDisk pointcut */
    public final ActionContainer<DatanodeID, IOException> fiCallWritePacketToDisk
        = new ActionContainer<DatanodeID, IOException>();
    /** Simulate action for the statusRead pointcut */
    public final ActionContainer<DatanodeID, IOException> fiStatusRead
        = new ActionContainer<DatanodeID, IOException>();
    /** Simulate action for the afterDownstreamStatusRead pointcut */
    public final ActionContainer<DatanodeID, IOException> fiAfterDownstreamStatusRead
        = new ActionContainer<DatanodeID, IOException>();
    /** Simulate action for the pipelineAck pointcut */
    public final ActionContainer<DatanodeID, IOException> fiPipelineAck
        = new ActionContainer<DatanodeID, IOException>();
    /** Simulate action for the pipelineClose pointcut */
    public final ActionContainer<DatanodeID, IOException> fiPipelineClose
        = new ActionContainer<DatanodeID, IOException>();
    /** Simulate action for the blockFileClose pointcut */
    public final ActionContainer<DatanodeID, IOException> fiBlockFileClose
        = new ActionContainer<DatanodeID, IOException>();

    /** Verification action for the pipelineInitNonAppend pointcut */
    public final ActionContainer<Integer, RuntimeException> fiPipelineInitErrorNonAppend
        = new ActionContainer<Integer, RuntimeException>();
    /** Verification action for the pipelineErrorAfterInit pointcut */
    public final ActionContainer<Integer, RuntimeException> fiPipelineErrorAfterInit
        = new ActionContainer<Integer, RuntimeException>();

    /** Get test status */
    public boolean isSuccess() {
      return this.isSuccess;
    }

    /** Set test status */
    public void markSuccess() {
      this.isSuccess = true;
    }

    /** Initialize the pipeline. */
    @Override
    public synchronized Pipeline initPipeline(LocatedBlock lb) {
      final Pipeline pl = new Pipeline(lb);
      if (pipelines.contains(pl)) {
        throw new IllegalStateException("thepipeline != null");
      }
      pipelines.add(pl);
      return pl;
    }

    /** Return the pipeline for the datanode. */
    @Override
    public synchronized Pipeline getPipelineForDatanode(DatanodeID id) {
      for (Pipeline p : pipelines) {
        if (p.contains(id)){
          return p;
        }
      }
      FiTestUtil.LOG.info("FI: pipeline not found; id=" + id
          + ", pipelines=" + pipelines);
      return null;
    }

    /**
     * Is the test not yet success
     * and the last pipeline contains the given datanode?
     */
    private synchronized boolean isNotSuccessAndLastPipelineContains(
        int index, DatanodeID id) {
      if (isSuccess()) {
        return false;
      }
      final int n = pipelines.size();
      return n == 0? false: pipelines.get(n-1).contains(index, id);
    }
  }

  /** Action for DataNode */
  public static abstract class DataNodeAction implements
      Action<DatanodeID, IOException> {
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
      return getClass().getSimpleName() + ":" + currentTest
          + ", index=" + index;
    }

    /** return a String with this object and the datanodeID. */
    String toString(DatanodeID datanodeID) {
      return "FI: " + this + ", datanode="
          + datanodeID.getName();
    }
  }

  /** An action to set a marker if the DatanodeID is matched. */
  public static class DatanodeMarkingAction extends DataNodeAction {
    private final MarkerConstraint marker;

    /** Construct an object. */
    public DatanodeMarkingAction(String currentTest, int index,
        MarkerConstraint marker) {
      super(currentTest, index);
      this.marker = marker;
    }

    /** Set the marker if the DatanodeID is matched. */
    @Override
    public void run(DatanodeID datanodeid) throws IOException {
      final DataTransferTest test = getDataTransferTest();
      if (test.isNotSuccessAndLastPipelineContains(index, datanodeid)) {
        marker.mark();
      }
    }

    /** {@inheritDoc} */
    public String toString() {
      return super.toString() + ", " + marker;
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
      if (test.isNotSuccessAndLastPipelineContains(index, id)) {
        final String s = toString(id);
        FiTestUtil.LOG.info(s);
        throw new OutOfMemoryError(s);
      }
    }
  }

  /** Throws OutOfMemoryError if the count is zero. */
  public static class CountdownOomAction extends OomAction {
    private final CountdownConstraint countdown;

    /** Create an action for datanode i in the pipeline with count down. */
    public CountdownOomAction(String currentTest, int i, int count) {
      super(currentTest, i);
      countdown = new CountdownConstraint(count);
    }

    @Override
    public void run(DatanodeID id) {
      final DataTransferTest test = getDataTransferTest();
      if (test.isNotSuccessAndLastPipelineContains(index, id)
          && countdown.isSatisfied()) {
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
      if (test.isNotSuccessAndLastPipelineContains(index, id)) {
        final String s = toString(id);
        FiTestUtil.LOG.info(s);
        throw new DiskOutOfSpaceException(s);
      }
    }
  }

  /** Throws an IOException. */
  public static class IoeAction extends DataNodeAction {
    private final String error; 

    /** Create an action for datanode i in the pipeline. */
    public IoeAction(String currentTest, int i, String error) {
      super(currentTest, i);
      this.error = error;
    }

    @Override
    public void run(DatanodeID id) throws IOException {
      final DataTransferTest test = getDataTransferTest();
      if (test.isNotSuccessAndLastPipelineContains(index, id)) {
        final String s = toString(id);
        FiTestUtil.LOG.info(s);
        throw new IOException(s);
      }
    }

    @Override
    public String toString() {
      return error + " " + super.toString();
    }
  }

  /** Throws DiskOutOfSpaceException if the count is zero. */
  public static class CountdownDoosAction extends DoosAction {
    private final CountdownConstraint countdown;

    /** Create an action for datanode i in the pipeline with count down. */
    public CountdownDoosAction(String currentTest, int i, int count) {
      super(currentTest, i);
      countdown = new CountdownConstraint(count);
    }

    @Override
    public void run(DatanodeID id) throws DiskOutOfSpaceException {
      final DataTransferTest test = getDataTransferTest();
      if (test.isNotSuccessAndLastPipelineContains(index, id)
          && countdown.isSatisfied()) {
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
    /** In milliseconds;
     * must have (0 <= minDuration < maxDuration) or (maxDuration <= 0).
     */
    final long minDuration;
    /** In milliseconds; maxDuration <= 0 means sleeping forever.*/
    final long maxDuration;

    /**
     * Create an action for datanode i in the pipeline.
     * @param duration In milliseconds, duration <= 0 means sleeping forever.
     */
    public SleepAction(String currentTest, int i, long duration) {
      this(currentTest, i, duration, duration <= 0? duration: duration+1);
    }

    /**
     * Create an action for datanode i in the pipeline.
     * @param minDuration minimum sleep time
     * @param maxDuration maximum sleep time
     */
    public SleepAction(String currentTest, int i,
        long minDuration, long maxDuration) {
      super(currentTest, i);

      if (maxDuration > 0) {
        if (minDuration < 0) {
          throw new IllegalArgumentException("minDuration = " + minDuration
              + " < 0 but maxDuration = " + maxDuration + " > 0");
        }
        if (minDuration >= maxDuration) {
          throw new IllegalArgumentException(
              minDuration + " = minDuration >= maxDuration = " + maxDuration);
        }
      }
      this.minDuration = minDuration;
      this.maxDuration = maxDuration;
    }

    @Override
    public void run(DatanodeID id) {
      final DataTransferTest test = getDataTransferTest();
      if (test.isNotSuccessAndLastPipelineContains(index, id)) {
        FiTestUtil.LOG.info(toString(id));
        if (maxDuration <= 0) {
          for(; FiTestUtil.sleep(1000); ); //sleep forever until interrupt
        } else {
          FiTestUtil.sleep(minDuration, maxDuration);
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return super.toString() + ", duration="
          + (maxDuration <= 0? "infinity": "[" + minDuration + ", " + maxDuration + ")");
    }
  }

  /**
   * When the count is zero,
   * sleep some period of time so that it slows down the datanode
   * or sleep forever so that datanode becomes not responding.
   */
  public static class CountdownSleepAction extends SleepAction {
    private final CountdownConstraint countdown;

    /**
     * Create an action for datanode i in the pipeline.
     * @param duration In milliseconds, duration <= 0 means sleeping forever.
     */
    public CountdownSleepAction(String currentTest, int i,
        long duration, int count) {
      this(currentTest, i, duration, duration+1, count);
    }

    /** Create an action for datanode i in the pipeline with count down. */
    public CountdownSleepAction(String currentTest, int i,
        long minDuration, long maxDuration, int count) {
      super(currentTest, i, minDuration, maxDuration);
      countdown = new CountdownConstraint(count);
    }

    @Override
    public void run(DatanodeID id) {
      final DataTransferTest test = getDataTransferTest();
      if (test.isNotSuccessAndLastPipelineContains(index, id)
          && countdown.isSatisfied()) {
        final String s = toString(id) + ", duration = ["
        + minDuration + "," + maxDuration + ")";
        FiTestUtil.LOG.info(s);
        if (maxDuration <= 1) {
          for(; FiTestUtil.sleep(1000); ); //sleep forever until interrupt
        } else {
          FiTestUtil.sleep(minDuration, maxDuration);
        }
      }
    }
  }

  /** Action for pipeline error verification */
  public static class VerificationAction implements
      Action<Integer, RuntimeException> {
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

  /**
   *  Create a OomAction with a CountdownConstraint
   *  so that it throws OutOfMemoryError if the count is zero.
   */
  public static ConstraintSatisfactionAction<DatanodeID, IOException>
      createCountdownOomAction(
        String currentTest, int i, int count) {
    return new ConstraintSatisfactionAction<DatanodeID, IOException>(
        new OomAction(currentTest, i), new CountdownConstraint(count));
  }

  /**
   *  Create a DoosAction with a CountdownConstraint
   *  so that it throws DiskOutOfSpaceException if the count is zero.
   */
  public static ConstraintSatisfactionAction<DatanodeID, IOException>
    createCountdownDoosAction(
      String currentTest, int i, int count) {
    return new ConstraintSatisfactionAction<DatanodeID, IOException>(
        new DoosAction(currentTest, i), new CountdownConstraint(count));
  }

  /**
   * Create a SleepAction with a CountdownConstraint
   * for datanode i in the pipeline.
   * When the count is zero,
   * sleep some period of time so that it slows down the datanode
   * or sleep forever so the that datanode becomes not responding.
   */
  public static ConstraintSatisfactionAction<DatanodeID, IOException> createCountdownSleepAction(
      String currentTest, int i, long minDuration, long maxDuration, int count) {
    return new ConstraintSatisfactionAction<DatanodeID, IOException>(
        new SleepAction(currentTest, i, minDuration, maxDuration),
        new CountdownConstraint(count));
  }

  /**
   * Same as
   * createCountdownSleepAction(currentTest, i, duration, duration+1, count).
   */
  public static ConstraintSatisfactionAction<DatanodeID, IOException> createCountdownSleepAction(
      String currentTest, int i, long duration, int count) {
    return createCountdownSleepAction(currentTest, i, duration, duration+1,
        count);
  }
}