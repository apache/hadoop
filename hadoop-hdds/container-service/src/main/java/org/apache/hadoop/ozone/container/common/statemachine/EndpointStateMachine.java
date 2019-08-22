/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.scm.HddsServerUtil.getLogWarnInterval;
import static org.apache.hadoop.hdds.scm.HddsServerUtil.getScmHeartbeatInterval;

/**
 * Endpoint is used as holder class that keeps state around the RPC endpoint.
 */
public class EndpointStateMachine
    implements Closeable, EndpointStateMachineMBean {
  static final Logger
      LOG = LoggerFactory.getLogger(EndpointStateMachine.class);
  private final StorageContainerDatanodeProtocolClientSideTranslatorPB endPoint;
  private final AtomicLong missedCount;
  private final InetSocketAddress address;
  private final Lock lock;
  private final Configuration conf;
  private EndPointStates state;
  private VersionResponse version;
  private ZonedDateTime lastSuccessfulHeartbeat;

  /**
   * Constructs RPC Endpoints.
   *
   * @param endPoint - RPC endPoint.
   */
  public EndpointStateMachine(InetSocketAddress address,
      StorageContainerDatanodeProtocolClientSideTranslatorPB endPoint,
      Configuration conf) {
    this.endPoint = endPoint;
    this.missedCount = new AtomicLong(0);
    this.address = address;
    state = EndPointStates.getInitState();
    lock = new ReentrantLock();
    this.conf = conf;
  }

  /**
   * Takes a lock on this EndPoint so that other threads don't use this while we
   * are trying to communicate via this endpoint.
   */
  public void lock() {
    lock.lock();
  }

  /**
   * Unlocks this endpoint.
   */
  public void unlock() {
    lock.unlock();
  }

  /**
   * Returns the version that we read from the server if anyone asks .
   *
   * @return - Version Response.
   */
  public VersionResponse getVersion() {
    return version;
  }

  /**
   * Sets the Version reponse we recieved from the SCM.
   *
   * @param version VersionResponse
   */
  public void setVersion(VersionResponse version) {
    this.version = version;
  }

  /**
   * Returns the current State this end point is in.
   *
   * @return - getState.
   */
  public EndPointStates getState() {
    return state;
  }

  @Override
  public int getVersionNumber() {
    if (version != null) {
      return version.getProtobufMessage().getSoftwareVersion();
    } else {
      return -1;
    }
  }

  /**
   * Sets the endpoint state.
   *
   * @param epState - end point state.
   */
  public EndPointStates setState(EndPointStates epState) {
    this.state = epState;
    return this.state;
  }

  /**
   * Closes the connection.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (endPoint != null) {
      endPoint.close();
    }
  }

  /**
   * We maintain a count of how many times we missed communicating with a
   * specific SCM. This is not made atomic since the access to this is always
   * guarded by the read or write lock. That is, it is serialized.
   */
  public void incMissed() {
    this.missedCount.incrementAndGet();
  }

  /**
   * Returns the value of the missed count.
   *
   * @return int
   */
  public long getMissedCount() {
    return this.missedCount.get();
  }

  @Override
  public String getAddressString() {
    return getAddress().toString();
  }

  public void zeroMissedCount() {
    this.missedCount.set(0);
  }

  /**
   * Returns the InetAddress of the endPoint.
   *
   * @return - EndPoint.
   */
  public InetSocketAddress getAddress() {
    return this.address;
  }

  /**
   * Returns real RPC endPoint.
   *
   * @return rpc client.
   */
  public StorageContainerDatanodeProtocolClientSideTranslatorPB
      getEndPoint() {
    return endPoint;
  }

  /**
   * Returns the string that represents this endpoint.
   *
   * @return - String
   */
  public String toString() {
    return address.toString();
  }

  /**
   * Logs exception if needed.
   *  @param ex         - Exception
   */
  public void logIfNeeded(Exception ex) {
    if (this.getMissedCount() % getLogWarnInterval(conf) == 0) {
      LOG.error(
          "Unable to communicate to SCM server at {} for past {} seconds.",
          this.getAddress().getHostString() + ":" + this.getAddress().getPort(),
          TimeUnit.MILLISECONDS.toSeconds(
              this.getMissedCount() * getScmHeartbeatInterval(this.conf)), ex);
    }
    LOG.trace("Incrementing the Missed count. Ex : {}", ex);
    this.incMissed();
  }


  /**
   * States that an Endpoint can be in.
   * <p>
   * This is a sorted list of states that EndPoint will traverse.
   * <p>
   * GetNextState will move this enum from getInitState to getLastState.
   */
  public enum EndPointStates {
    GETVERSION(1),
    REGISTER(2),
    HEARTBEAT(3),
    SHUTDOWN(4); // if you add value after this please edit getLastState too.
    private final int value;

    /**
     * Constructs endPointStates.
     *
     * @param value  state.
     */
    EndPointStates(int value) {
      this.value = value;
    }

    /**
     * Returns the first State.
     *
     * @return First State.
     */
    public static EndPointStates getInitState() {
      return GETVERSION;
    }

    /**
     * The last state of endpoint states.
     *
     * @return last state.
     */
    public static EndPointStates getLastState() {
      return SHUTDOWN;
    }

    /**
     * returns the numeric value associated with the endPoint.
     *
     * @return int.
     */
    public int getValue() {
      return value;
    }

    /**
     * Returns the next logical state that endPoint should move to.
     * The next state is computed by adding 1 to the current state.
     *
     * @return NextState.
     */
    public EndPointStates getNextState() {
      if (this.getValue() < getLastState().getValue()) {
        int stateValue = this.getValue() + 1;
        for (EndPointStates iter : values()) {
          if (stateValue == iter.getValue()) {
            return iter;
          }
        }
      }
      return getLastState();
    }
  }

  public long getLastSuccessfulHeartbeat() {
    return lastSuccessfulHeartbeat == null ?
        0 :
        lastSuccessfulHeartbeat.toEpochSecond();
  }

  public void setLastSuccessfulHeartbeat(
      ZonedDateTime lastSuccessfulHeartbeat) {
    this.lastSuccessfulHeartbeat = lastSuccessfulHeartbeat;
  }
}
