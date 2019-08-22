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

package org.apache.hadoop.applications.mawo.server.master.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Define MaWo JobId.
 */
public class JobId implements Writable {

  /**
   * MaWo job prefix.
   */
  private static final String JOB_PREFIX = "mawo_job_";

  /**
   * Create unique random JobId.
   * @return unique random JobId
   */
  static JobId newJobId() {
    Random rn = new Random();
    final int range = 900000;
    final int randomadd = 100000;
    int randomNum = rn.nextInt(range) + randomadd;
    return new JobId(randomNum);
  }

  /**
   * Unique Id.
   */
  private int jobIdentifier;

  /**
   * JobId default constructor.
   */
  public JobId() {

  }

  /**
   * JobId constructor with Id.
   * @param id : unique id
   */
  public JobId(final int id) {
    this.jobIdentifier = id;
  }

  /**
   * Get JobId.
   * @return unique ID
   */
  public final int getID() {
    return jobIdentifier;
  }

  /**
   * Print JobId.
   * @return JobId
   */
  public final String toString() {
    return JOB_PREFIX + jobIdentifier;
  }

  @Override
  /**
   * Hashcode for jobId.
   */
  public final int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + jobIdentifier;
    return result;
  }

  @Override
  /**
   * Implement equals method for jobId.
   */
  public final boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    JobId other = (JobId) obj;
    if (jobIdentifier != other.jobIdentifier) {
      return false;
    }
    return true;
  }

  /** {@inheritDoc} */
  public final void write(final DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, jobIdentifier);
  }

  /** {@inheritDoc} */
  public final void readFields(final DataInput in) throws IOException {
    this.jobIdentifier = WritableUtils.readVInt(in);
  }
}
