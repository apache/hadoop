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

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.Constants.OperationType;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple access layer onto of a configuration object that extracts the slive
 * specific configuration values needed for slive running
 */
class ConfigExtractor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigExtractor.class);

  private Configuration config;

  ConfigExtractor(Configuration cfg) {
    this.config = cfg;
  }

  /**
   * @return the wrapped configuration that this extractor will use
   */
  Configuration getConfig() {
    return this.config;
  }

  /**
   * @return the location of where data should be written to
   */
  Path getDataPath() {
    Path base = getBaseDirectory();
    if (base == null) {
      return null;
    }
    return new Path(base, Constants.DATA_DIR);
  }

  /**
   * @return the location of where the reducer should write its data to
   */
  Path getOutputPath() {
    Path base = getBaseDirectory();
    if (base == null) {
      return null;
    }
    return new Path(base, Constants.OUTPUT_DIR);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * 
   * @return the base directory where output & data should be stored using
   *         primary,config,default (in that order)
   */
  Path getBaseDirectory(String primary) {
    String path = primary;
    if (path == null) {
      path = config.get(ConfigOption.BASE_DIR.getCfgOption());
    }
    if (path == null) {
      path = ConfigOption.BASE_DIR.getDefault();
    }
    if (path == null) {
      return null;
    }
    return new Path(path);
  }

  /**
   * @return the base directory using only config and default values
   */
  Path getBaseDirectory() {
    return getBaseDirectory(null);
  }

  /**
   * @return whether the mapper or reducer should exit when they get there first
   *         error using only config and default values
   */
  boolean shouldExitOnFirstError() {
    return shouldExitOnFirstError(null);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * 
   * @return the boolean of whether the mapper/reducer should exit when they
   *         first error from primary,config,default (in that order)
   */
  boolean shouldExitOnFirstError(String primary) {
    String val = primary;
    if (val == null) {
      val = config.get(ConfigOption.EXIT_ON_ERROR.getCfgOption());
    }
    if (val == null) {
      return ConfigOption.EXIT_ON_ERROR.getDefault();
    }
    return Boolean.parseBoolean(val);
  }

  /**
   * @return whether the mapper or reducer should wait for truncate recovery
   */
  boolean shouldWaitOnTruncate() {
    return shouldWaitOnTruncate(null);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   *
   * @return whether the mapper or reducer should wait for truncate recovery
   */
  boolean shouldWaitOnTruncate(String primary) {
    String val = primary;
    if (val == null) {
      val = config.get(ConfigOption.EXIT_ON_ERROR.getCfgOption());
    }
    if (val == null) {
      return ConfigOption.EXIT_ON_ERROR.getDefault();
    }
    return Boolean.parseBoolean(val);
  }

  /**
   * @return the number of reducers to use
   */
  Integer getReducerAmount() {
    // should be slive.reduces
    return getInteger(null, ConfigOption.REDUCES);
  }

  /**
   * @return the number of mappers to use using config and default values for
   *         lookup
   */
  Integer getMapAmount() {
    return getMapAmount(null);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the reducer amount to use
   */
  Integer getMapAmount(String primary) {
    return getInteger(primary, ConfigOption.MAPS);
  }

  /**
   * @return the duration in seconds (or null or Integer.MAX for no limit) using
   *         the configuration and default as lookup
   */
  Integer getDuration() {
    return getDuration(null);
  }

  /**
   * @return the duration in milliseconds or null if no limit using config and
   *         default as lookup
   */
  Integer getDurationMilliseconds() {
    Integer seconds = getDuration();
    if (seconds == null || seconds == Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    int milliseconds = (seconds * 1000);
    if (milliseconds < 0) {
      milliseconds = 0;
    }
    return milliseconds;
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the duration in seconds (or null or Integer.MAX for no limit)
   */
  Integer getDuration(String primary) {
    return getInteger(primary, ConfigOption.DURATION);
  }

  /**
   * @return the total number of operations to run using config and default as
   *         lookup
   */
  Integer getOpCount() {
    return getOpCount(null);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the total number of operations to run
   */
  Integer getOpCount(String primary) {
    return getInteger(primary, ConfigOption.OPS);
  }

  /**
   * @return the total number of files per directory using config and default as
   *         lookup
   */
  Integer getDirSize() {
    return getDirSize(null);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the total number of files per directory
   */
  Integer getDirSize(String primary) {
    return getInteger(primary, ConfigOption.DIR_SIZE);
  }

  /**
   * @param primary
   *          the primary string to attempt to convert into a integer
   * @param opt
   *          the option to use as secondary + default if no primary given
   * @return a parsed integer
   */
  private Integer getInteger(String primary, ConfigOption<Integer> opt) {
    String value = primary;
    if (value == null) {
      value = config.get(opt.getCfgOption());
    }
    if (value == null) {
      return opt.getDefault();
    }
    return Integer.parseInt(value);
  }

  /**
   * @return the total number of files allowed using configuration and default
   *         for lookup
   */
  Integer getTotalFiles() {
    return getTotalFiles(null);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the total number of files allowed
   */
  Integer getTotalFiles(String primary) {
    return getInteger(primary, ConfigOption.FILES);
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the random seed start point or null if none
   */
  Long getRandomSeed(String primary) {
    String seed = primary;
    if (seed == null) {
      seed = config.get(ConfigOption.RANDOM_SEED.getCfgOption());
    }
    if (seed == null) {
      return null;
    }
    return Long.parseLong(seed);
  }

  /**
   * @return the random seed start point or null if none using config and then
   *         default as lookup
   */
  Long getRandomSeed() {
    return getRandomSeed(null);
  }

  /**
   * @return the result file location or null if none using config and then
   *         default as lookup
   */
  String getResultFile() {
    return getResultFile(null);
  }

  /**
   * Gets the grid queue name to run on using config and default only
   * 
   * @return String
   */
  String getQueueName() {
    return getQueueName(null);
  }

  /**
   * Gets the grid queue name to run on using the primary string or config or
   * default
   * 
   * @param primary
   * 
   * @return String
   */
  String getQueueName(String primary) {
    String q = primary;
    if (q == null) {
      q = config.get(ConfigOption.QUEUE_NAME.getCfgOption());
    }
    if (q == null) {
      q = ConfigOption.QUEUE_NAME.getDefault();
    }
    return q;
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the result file location
   */
  String getResultFile(String primary) {
    String fn = primary;
    if (fn == null) {
      fn = config.get(ConfigOption.RESULT_FILE.getCfgOption());
    }
    if (fn == null) {
      fn = ConfigOption.RESULT_FILE.getDefault();
    }
    return fn;
  }

  /**
   * @param primary
   *          primary the initial string to be used for the value of this
   *          configuration option (if not provided then config and then the
   *          default are used)
   * @return the integer range allowed for the block size
   */
  Range<Long> getBlockSize(String primary) {
    return getMinMaxBytes(ConfigOption.BLOCK_SIZE, primary);
  }

  /**
   * @return the integer range allowed for the block size using config and
   *         default for lookup
   */
  Range<Long> getBlockSize() {
    return getBlockSize(null);
  }

  /**
   * @param cfgopt
   *          the configuration option to use for config and default lookup
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the parsed short range from primary, config, default
   */
  private Range<Short> getMinMaxShort(ConfigOption<Short> cfgopt, String primary) {
    String sval = primary;
    if (sval == null) {
      sval = config.get(cfgopt.getCfgOption());
    }
    Range<Short> range = null;
    if (sval != null) {
      String pieces[] = Helper.getTrimmedStrings(sval);
      if (pieces.length == 2) {
        String min = pieces[0];
        String max = pieces[1];
        short minVal = Short.parseShort(min);
        short maxVal = Short.parseShort(max);
        if (minVal > maxVal) {
          short tmp = minVal;
          minVal = maxVal;
          maxVal = tmp;
        }
        range = new Range<Short>(minVal, maxVal);
      }
    }
    if (range == null) {
      Short def = cfgopt.getDefault();
      if (def != null) {
        range = new Range<Short>(def, def);
      }
    }
    return range;
  }

  /**
   * @param cfgopt
   *          the configuration option to use for config and default lookup
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the parsed long range from primary, config, default
   */
  private Range<Long> getMinMaxLong(ConfigOption<Long> cfgopt, String primary) {
    String sval = primary;
    if (sval == null) {
      sval = config.get(cfgopt.getCfgOption());
    }
    Range<Long> range = null;
    if (sval != null) {
      String pieces[] = Helper.getTrimmedStrings(sval);
      if (pieces.length == 2) {
        String min = pieces[0];
        String max = pieces[1];
        long minVal = Long.parseLong(min);
        long maxVal = Long.parseLong(max);
        if (minVal > maxVal) {
          long tmp = minVal;
          minVal = maxVal;
          maxVal = tmp;
        }
        range = new Range<Long>(minVal, maxVal);
      }
    }
    if (range == null) {
      Long def = cfgopt.getDefault();
      if (def != null) {
        range = new Range<Long>(def, def);
      }
    }
    return range;
  }

  /**
   * @param cfgopt
   *          the configuration option to use for config and default lookup
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the parsed integer byte range from primary, config, default
   */
  private Range<Long> getMinMaxBytes(ConfigOption<Long> cfgopt, String primary) {
    String sval = primary;
    if (sval == null) {
      sval = config.get(cfgopt.getCfgOption());
    }
    Range<Long> range = null;
    if (sval != null) {
      String pieces[] = Helper.getTrimmedStrings(sval);
      if (pieces.length == 2) {
        String min = pieces[0];
        String max = pieces[1];
        long tMin = StringUtils.TraditionalBinaryPrefix.string2long(min);
        long tMax = StringUtils.TraditionalBinaryPrefix.string2long(max);
        if (tMin > tMax) {
          long tmp = tMin;
          tMin = tMax;
          tMax = tmp;
        }
        range = new Range<Long>(tMin, tMax);
      }
    }
    if (range == null) {
      Long def = cfgopt.getDefault();
      if (def != null) {
        range = new Range<Long>(def, def);
      }
    }
    return range;
  }

  /**
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the replication range
   */
  Range<Short> getReplication(String primary) {
    return getMinMaxShort(ConfigOption.REPLICATION_AM, primary);
  }

  /**
   * @return the replication range using config and default for lookup
   */
  Range<Short> getReplication() {
    return getReplication(null);
  }

  /**
   * @return the map of operations to perform using config (percent may be null
   *         if unspecified)
   */
  Map<OperationType, OperationData> getOperations() {
    Map<OperationType, OperationData> operations = new HashMap<OperationType, OperationData>();
    for (OperationType type : OperationType.values()) {
      String opname = type.lowerName();
      String keyname = String.format(Constants.OP, opname);
      String kval = config.get(keyname);
      if (kval == null) {
        continue;
      }
      operations.put(type, new OperationData(kval));
    }
    return operations;
  }

  /**
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the append byte size range (or null if none)
   */
  Range<Long> getAppendSize(String primary) {
    return getMinMaxBytes(ConfigOption.APPEND_SIZE, primary);
  }

  /**
   * @return the append byte size range (or null if none) using config and
   *         default for lookup
   */
  Range<Long> getAppendSize() {
    return getAppendSize(null);
  }

  /**
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the truncate byte size range (or null if none)
   */
  Range<Long> getTruncateSize(String primary) {
    return getMinMaxBytes(ConfigOption.TRUNCATE_SIZE, primary);
  }

  /**
   * @return the truncate byte size range (or null if none) using config and
   *         default for lookup
   */
  Range<Long> getTruncateSize() {
    return getTruncateSize(null);
  }

  /**
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the sleep range (or null if none)
   */
  Range<Long> getSleepRange(String primary) {
    return getMinMaxLong(ConfigOption.SLEEP_TIME, primary);
  }

  /**
   * @return the sleep range (or null if none) using config and default for
   *         lookup
   */
  Range<Long> getSleepRange() {
    return getSleepRange(null);
  }

  /**
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the write byte size range (or null if none)
   */
  Range<Long> getWriteSize(String primary) {
    return getMinMaxBytes(ConfigOption.WRITE_SIZE, primary);
  }

  /**
   * @return the write byte size range (or null if none) using config and
   *         default for lookup
   */
  Range<Long> getWriteSize() {
    return getWriteSize(null);
  }

  /**
   * Returns whether the write range should use the block size range
   * 
   * @return true|false
   */
  boolean shouldWriteUseBlockSize() {
    Range<Long> writeRange = getWriteSize();
    if (writeRange == null
        || (writeRange.getLower() == writeRange.getUpper() && (writeRange
            .getUpper() == Long.MAX_VALUE))) {
      return true;
    }
    return false;
  }

  /**
   * Returns whether the append range should use the block size range
   * 
   * @return true|false
   */
  boolean shouldAppendUseBlockSize() {
    Range<Long> appendRange = getAppendSize();
    if (appendRange == null
        || (appendRange.getLower() == appendRange.getUpper() && (appendRange
            .getUpper() == Long.MAX_VALUE))) {
      return true;
    }
    return false;
  }

  /**
   * Returns whether the truncate range should use the block size range
   *
   * @return true|false
   */
  boolean shouldTruncateUseBlockSize() {
    Range<Long> truncateRange = getTruncateSize();
    if (truncateRange == null
        || (truncateRange.getLower() == truncateRange.getUpper()
            && (truncateRange.getUpper() == Long.MAX_VALUE))) {
      return true;
    }
    return false;
  }

  /**
   * Returns whether the read range should use the entire file
   * 
   * @return true|false
   */
  boolean shouldReadFullFile() {
    Range<Long> readRange = getReadSize();
    if (readRange == null
        || (readRange.getLower() == readRange.getUpper() && (readRange
            .getUpper() == Long.MAX_VALUE))) {
      return true;
    }
    return false;
  }

  /**
   * @param primary
   *          the initial string to be used for the value of this configuration
   *          option (if not provided then config and then the default are used)
   * @return the read byte size range (or null if none)
   */
  Range<Long> getReadSize(String primary) {
    return getMinMaxBytes(ConfigOption.READ_SIZE, primary);
  }
  
  /**
   * Gets the bytes per checksum (if it exists or null if not)
   * 
   * @return Long 
   */
  Long getByteCheckSum() {
    String val = config.get(Constants.BYTES_PER_CHECKSUM);
    if(val == null) {
      return null;
    }
    return Long.parseLong(val);
  }

  /**
   * @return the read byte size range (or null if none) using config and default
   *         for lookup
   */
  Range<Long> getReadSize() {
    return getReadSize(null);
  }

  /**
   * Dumps out the given options for the given config extractor
   * 
   * @param cfg
   *          the config to write to the log
   */
  static void dumpOptions(ConfigExtractor cfg) {
    if (cfg == null) {
      return;
    }
    LOG.info("Base directory = " + cfg.getBaseDirectory());
    LOG.info("Data directory = " + cfg.getDataPath());
    LOG.info("Output directory = " + cfg.getOutputPath());
    LOG.info("Result file = " + cfg.getResultFile());
    LOG.info("Grid queue = " + cfg.getQueueName());
    LOG.info("Should exit on first error = " + cfg.shouldExitOnFirstError());
    {
      String duration = "Duration = ";
      if (cfg.getDurationMilliseconds() == Integer.MAX_VALUE) {
        duration += "unlimited";
      } else {
        duration += cfg.getDurationMilliseconds() + " milliseconds";
      }
      LOG.info(duration);
    }
    LOG.info("Map amount = " + cfg.getMapAmount());
    LOG.info("Reducer amount = " + cfg.getReducerAmount());
    LOG.info("Operation amount = " + cfg.getOpCount());
    LOG.info("Total file limit = " + cfg.getTotalFiles());
    LOG.info("Total dir file limit = " + cfg.getDirSize());
    {
      String read = "Read size = ";
      if (cfg.shouldReadFullFile()) {
        read += "entire file";
      } else {
        read += cfg.getReadSize() + " bytes";
      }
      LOG.info(read);
    }
    {
      String write = "Write size = ";
      if (cfg.shouldWriteUseBlockSize()) {
        write += "blocksize";
      } else {
        write += cfg.getWriteSize() + " bytes";
      }
      LOG.info(write);
    }
    {
      String append = "Append size = ";
      if (cfg.shouldAppendUseBlockSize()) {
        append += "blocksize";
      } else {
        append += cfg.getAppendSize() + " bytes";
      }
      LOG.info(append);
    }
    {
      String bsize = "Block size = ";
      bsize += cfg.getBlockSize() + " bytes";
      LOG.info(bsize);
    }
    if (cfg.getRandomSeed() != null) {
      LOG.info("Random seed = " + cfg.getRandomSeed());
    }
    if (cfg.getSleepRange() != null) {
      LOG.info("Sleep range = " + cfg.getSleepRange() + " milliseconds");
    }
    LOG.info("Replication amount = " + cfg.getReplication());
    LOG.info("Operations are:");
    NumberFormat percFormatter = Formatter.getPercentFormatter();
    Map<OperationType, OperationData> operations = cfg.getOperations();
    for (OperationType type : operations.keySet()) {
      String name = type.name();
      LOG.info(name);
      OperationData opInfo = operations.get(type);
      LOG.info(" " + opInfo.getDistribution().name());
      if (opInfo.getPercent() != null) {
        LOG.info(" " + percFormatter.format(opInfo.getPercent()));
      } else {
        LOG.info(" ???");
      }
    }
  }

}
