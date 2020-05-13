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

package org.apache.hadoop.applications.mawo.server.worker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Define WorkerId for Workers.
 */
public class WorkerId implements Writable {
  /**
   * WorkerId is a unique identifier for workers.
   */
  private Text workerId = new Text();
  /**
   * Hostname of worker node.
   */
  private Text hostname = new Text();
  /**
   * Ip address of worker node.
   */
  private Text ipAdd = new Text();

  /**
   * Default constructor for workerId.
   * Set Hostname and Ip address of the machine where worker is running.
   */
  public WorkerId() {
    try {
      this.hostname =
          new Text(InetAddress.getLocalHost().getHostName());
      this.ipAdd =
          new Text(InetAddress.getLocalHost().getHostAddress().toString());
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  /**
   * Get hostname for Worker.
   * @return hostname of worker node
   */
  public final Text getHostname() {
    return hostname;
  }

  /**
   * Set hostname for Worker.
   * @param wkhostname : Hostname of worker
   */
  public final void setHostname(final Text wkhostname) {
    this.hostname = wkhostname;
  }

  /**
   * Get Worker IP address.
   * @return IP address of worker node
   */
  public final String getIPAddress() {
    return this.ipAdd.toString();
  }

  /**
   * Print workerId.
   * @return workeId in string
   */
  @Override
  public final String toString() {
    return workerId.toString();
  }

  /**
   * Get workerId.
   * @return workerId : Worker identifier
   */
  public final String getWorkerId() {
    return this.workerId.toString();
  }

  /**
   * Set workerId.
   * @param localworkerId : Worker identifier
   */
  public final void setWorkerId(final String localworkerId) {
    this.workerId = new Text(localworkerId);
  }

  @Override
  /**
   * Implememt equals method for WorkerId.
   */
  public final boolean equals(final Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    WorkerId x = (WorkerId) o;
    return x.getHostname().equals(this.hostname);
  }

  /** {@inheritDoc} */
  @Override
  public final void write(final DataOutput dataOutput) throws IOException {
    workerId.write(dataOutput);
    hostname.write(dataOutput);
    ipAdd.write(dataOutput);
  }

  /** {@inheritDoc} */
  @Override
  public final void readFields(final DataInput dataInput) throws IOException {
    workerId.readFields(dataInput);
    hostname.readFields(dataInput);
    ipAdd.readFields(dataInput);
  }

  @Override
  /**
   * Override hashcode method for WorkerId.
   */
  public final int hashCode() {
    final int prime = 31;
    int result = 1;
    int workerHash = 0;
    if (workerId == null) {
      workerHash = 0;
    } else {
      workerHash = workerId.hashCode();
    }
    int hostHash = 0;
    if (hostname == null) {
      hostHash = 0;
    } else {
      hostHash = hostname.hashCode();
    }
    int ipHash = 0;
    if (ipAdd == null) {
      ipHash = 0;
    } else {
      ipHash = ipAdd.hashCode();
    }
    result = prime * result + workerHash;
    result = prime * result + hostHash;
    result = prime * result + ipHash;
    return result;
  }
}
