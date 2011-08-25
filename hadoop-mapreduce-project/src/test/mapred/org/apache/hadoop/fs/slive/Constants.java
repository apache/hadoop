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

/**
 * Constants used in various places in slive
 */
class Constants {

  /**
   * This class should be static members only - no construction allowed
   */
  private Constants() {
  }

  /**
   * The distributions supported (or that maybe supported)
   */
  enum Distribution {
    BEG, END, UNIFORM, MID;
    String lowerName() {
      return this.name().toLowerCase();
    }
  }

  /**
   * Allowed operation types
   */
  enum OperationType {
    READ, APPEND, RENAME, LS, MKDIR, DELETE, CREATE;
    String lowerName() {
      return this.name().toLowerCase();
    }
  }

  // program info
  static final String PROG_NAME = SliveTest.class.getSimpleName();
  static final String PROG_VERSION = "0.0.2";

  // useful constants
  static final int MEGABYTES = 1048576;

  // must be a multiple of
  // BYTES_PER_LONG - used for reading and writing buffer sizes
  static final int BUFFERSIZE = 64 * 1024;

  // 8 bytes per long
  static final int BYTES_PER_LONG = 8;

  // used for finding the reducer file for a given number
  static final String REDUCER_FILE = "part-%s";

  // this is used to ensure the blocksize is a multiple of this config setting
  static final String BYTES_PER_CHECKSUM = "io.bytes.per.checksum";

  // min replication setting for verification
  static final String MIN_REPLICATION = "dfs.namenode.replication.min";

  // used for getting an option description given a set of distributions
  // to substitute
  static final String OP_DESCR = "pct,distribution where distribution is one of %s";

  // keys for looking up a specific operation in the hadoop config
  static final String OP_PERCENT = "slive.op.%s.pct";
  static final String OP = "slive.op.%s";
  static final String OP_DISTR = "slive.op.%s.dist";

  // path constants
  static final String BASE_DIR = "slive";
  static final String DATA_DIR = "data";
  static final String OUTPUT_DIR = "output";

  // whether whenever data is written a flush should occur
  static final boolean FLUSH_WRITES = false;

}
