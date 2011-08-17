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

import org.apache.commons.cli.Option;

/**
 * Class which extends the basic option object and adds in the configuration id
 * and a default value so a central place can be used for retrieval of these as
 * needed
 */
class ConfigOption<T> extends Option {

  private static final long serialVersionUID = 7218954906367671150L;

  // config starts with this prefix
  private static final String SLIVE_PREFIX = "slive";

  // command line options and descriptions and config option name
  static final ConfigOption<Integer> MAPS = new ConfigOption<Integer>(
      "maps", true, "Number of maps", SLIVE_PREFIX + ".maps", 10);

  static final ConfigOption<Integer> REDUCES = new ConfigOption<Integer>(
      "reduces", true, "Number of reduces", SLIVE_PREFIX + ".reduces", 1);

  static final ConfigOption<Integer> OPS = new ConfigOption<Integer>(
      "ops", true, "Max number of operations per map", SLIVE_PREFIX
          + ".map.ops", 1000);

  static final ConfigOption<Integer> DURATION = new ConfigOption<Integer>(
      "duration", true,
      "Duration of a map task in seconds (MAX_INT for no limit)", SLIVE_PREFIX
          + ".duration", Integer.MAX_VALUE);

  static final ConfigOption<Boolean> EXIT_ON_ERROR = new ConfigOption<Boolean>(
      "exitOnError", false, "Exit on first error", SLIVE_PREFIX
          + ".exit.on.error", false);

  static final ConfigOption<Integer> FILES = new ConfigOption<Integer>(
      "files", true, "Max total number of files",
      SLIVE_PREFIX + ".total.files", 10);

  static final ConfigOption<Integer> DIR_SIZE = new ConfigOption<Integer>(
      "dirSize", true, "Max files per directory", SLIVE_PREFIX + ".dir.size",
      32);

  static final ConfigOption<String> BASE_DIR = new ConfigOption<String>(
      "baseDir", true, "Base directory path", SLIVE_PREFIX + ".base.dir",
      "/test/slive");

  static final ConfigOption<String> RESULT_FILE = new ConfigOption<String>(
      "resFile", true, "Result file name", SLIVE_PREFIX + ".result.file",
      "part-0000");

  static final ConfigOption<Short> REPLICATION_AM = new ConfigOption<Short>(
      "replication", true, "Min,max value for replication amount", SLIVE_PREFIX
          + ".file.replication", (short) 3);

  static final ConfigOption<Long> BLOCK_SIZE = new ConfigOption<Long>(
      "blockSize", true, "Min,max for dfs file block size", SLIVE_PREFIX
          + ".block.size", 64L * Constants.MEGABYTES);

  static final ConfigOption<Long> READ_SIZE = new ConfigOption<Long>(
      "readSize", true,
      "Min,max for size to read (min=max=MAX_LONG=read entire file)",
      SLIVE_PREFIX + ".op.read.size", null);

  static final ConfigOption<Long> WRITE_SIZE = new ConfigOption<Long>(
      "writeSize", true,
      "Min,max for size to write (min=max=MAX_LONG=blocksize)", SLIVE_PREFIX
          + ".op.write.size", null);

  static final ConfigOption<Long> SLEEP_TIME = new ConfigOption<Long>(
      "sleep",
      true,
      "Min,max for millisecond of random sleep to perform (between operations)",
      SLIVE_PREFIX + ".op.sleep.range", null);

  static final ConfigOption<Long> APPEND_SIZE = new ConfigOption<Long>(
      "appendSize", true,
      "Min,max for size to append (min=max=MAX_LONG=blocksize)", SLIVE_PREFIX
          + ".op.append.size", null);

  static final ConfigOption<Long> RANDOM_SEED = new ConfigOption<Long>(
      "seed", true, "Random number seed", SLIVE_PREFIX + ".seed", null);

  // command line only options
  static final Option HELP = new Option("help", false,
      "Usage information");

  static final Option CLEANUP = new Option("cleanup", true,
      "Cleanup & remove directory after reporting");

  // non slive specific settings
  static final ConfigOption<String> QUEUE_NAME = new ConfigOption<String>(
      "queue", true, "Queue name", "mapred.job.queue.name", "default");

  static final ConfigOption<String> PACKET_SIZE = new ConfigOption<String>(
      "packetSize", true, "Dfs write packet size", "dfs.write.packet.size",
      null);

  /**
   * Hadoop configuration property name
   */
  private String cfgOption;

  /**
   * Default value if no value is located by other means
   */
  private T defaultValue;

  ConfigOption(String cliOption, boolean hasArg, String description,
      String cfgOption, T def) {
    super(cliOption, hasArg, description);
    this.cfgOption = cfgOption;
    this.defaultValue = def;
  }

  /**
   * @return the configuration option name to lookup in Configuration objects
   *         for this option
   */
  String getCfgOption() {
    return cfgOption;
  }

  /**
   * @return the default object for this option
   */
  T getDefault() {
    return defaultValue;
  }

}