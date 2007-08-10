/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This class encapsulates metrics for determining the load on a HRegionServer
 */
public class HServerLoad implements WritableComparable {
  private int numberOfRequests;         // number of requests since last report
  private int numberOfRegions;          // number of regions being served
  
  /*
   * TODO: Other metrics that might be considered when the master is actually
   * doing load balancing instead of merely trying to decide where to assign
   * a region:
   * <ul>
   *   <li># of CPUs, heap size (to determine the "class" of machine). For
   *       now, we consider them to be homogeneous.</li>
   *   <li>#requests per region (Map<{String|HRegionInfo}, Integer>)</li>
   *   <li>#compactions and/or #splits (churn)</li>
   *   <li>server death rate (maybe there is something wrong with this server)</li>
   * </ul>
   */
  
  /** default constructior (used by Writable) */
  public HServerLoad() {}
  
  /**
   * Constructor
   * @param numberOfRequests
   * @param numberOfRegions
   */
  public HServerLoad(int numberOfRequests, int numberOfRegions) {
    this.numberOfRequests = numberOfRequests;
    this.numberOfRegions = numberOfRegions;
  }
  
  /**
   * @return load factor for this server
   */
  public int getLoad() {
    int load = numberOfRequests == 0 ? 1 : numberOfRequests;
    load *= numberOfRegions == 0 ? 1 : numberOfRegions;
    return load;
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "requests: " + numberOfRequests + " regions: " + numberOfRegions;
  }
  
  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    return compareTo(o) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = Integer.valueOf(numberOfRequests).hashCode();
    result ^= Integer.valueOf(numberOfRegions).hashCode();
    return result;
  }
  
  // Getters
  
  /**
   * @return the numberOfRegions
   */
  public int getNumberOfRegions() {
    return numberOfRegions;
  }

  /**
   * @return the numberOfRequests
   */
  public int getNumberOfRequests() {
    return numberOfRequests;
  }

  // Setters
  
  /**
   * @param numberOfRegions the numberOfRegions to set
   */
  public void setNumberOfRegions(int numberOfRegions) {
    this.numberOfRegions = numberOfRegions;
  }

  // Writable

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    numberOfRequests = in.readInt();
    numberOfRegions = in.readInt();
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeInt(numberOfRequests);
    out.writeInt(numberOfRegions);
  }
  
  // Comparable

  /** {@inheritDoc} */
  public int compareTo(Object o) {
    HServerLoad other = (HServerLoad) o;
    return this.getLoad() - other.getLoad();
  }
}
