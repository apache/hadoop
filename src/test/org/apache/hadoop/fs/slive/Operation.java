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

package org.apache.hadoop.fs.slive;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;

/**
 * An operation provides these abstractions and if it desires to perform any
 * operations it must implement a override of the run() function to provide
 * varying output to be captured.
 */
abstract class Operation {

  private ConfigExtractor config;
  private PathFinder finder;
  private String type;
  private Random rnd;

  protected Operation(String type, ConfigExtractor cfg, Random rnd) {
    this.config = cfg;
    this.type = type;
    this.rnd = rnd;
    // Use a new Random instance so that the sequence of file names produced is
    // the same even in case of unsuccessful operations
    this.finder = new PathFinder(cfg, new Random(rnd.nextInt()));
  }

  /**
   * Gets the configuration object this class is using
   * 
   * @return ConfigExtractor
   */
  protected ConfigExtractor getConfig() {
    return this.config;
  }

  /**
   * Gets the random number generator to use for this operation
   * 
   * @return Random
   */
  protected Random getRandom() {
    return this.rnd;
  }

  /**
   * Gets the type of operation that this class belongs to
   * 
   * @return String
   */
  String getType() {
    return type;
  }

  /**
   * Gets the path finding/generating instance that this class is using
   * 
   * @return PathFinder
   */
  protected PathFinder getFinder() {
    return this.finder;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return getType();
  }

  /**
   * This run() method simply sets up the default output container and adds in a
   * data member to keep track of the number of operations that occurred
   * 
   * @param fs
   *          FileSystem object to perform operations with
   * 
   * @return List of operation outputs to be collected and output in the overall
   *         map reduce operation (or empty or null if none)
   */
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = new LinkedList<OperationOutput>();
    out.add(new OperationOutput(OutputType.LONG, getType(),
        ReportWriter.OP_COUNT, 1L));
    return out;
  }

}
