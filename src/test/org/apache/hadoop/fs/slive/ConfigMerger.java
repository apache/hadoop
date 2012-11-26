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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.ArgumentParser.ParsedOutput;
import org.apache.hadoop.fs.slive.Constants.Distribution;
import org.apache.hadoop.fs.slive.Constants.OperationType;
import org.apache.hadoop.util.StringUtils;

/**
 * Class which merges options given from a config file and the command line and
 * performs some basic verification of the data retrieved and sets the verified
 * values back into the configuration object for return
 */
class ConfigMerger {
  /**
   * Exception that represents config problems...
   */
  static class ConfigException extends IOException {

    private static final long serialVersionUID = 2047129184917444550L;

    ConfigException(String msg) {
      super(msg);
    }

    ConfigException(String msg, Throwable e) {
      super(msg, e);
    }
  }

  /**
   * Merges the given command line parsed output with the given configuration
   * object and returns the new configuration object with the correct options
   * overwritten
   * 
   * @param opts
   *          the parsed command line option output
   * @param base
   *          the base configuration to merge with
   * @return merged configuration object
   * @throws ConfigException
   *           when configuration errors or verification occur
   */
  Configuration getMerged(ParsedOutput opts, Configuration base)
      throws ConfigException {
    return handleOptions(opts, base);
  }

  /**
   * Gets the base set of operations to use
   * 
   * @return Map
   */
  private Map<OperationType, OperationData> getBaseOperations() {
    Map<OperationType, OperationData> base = new HashMap<OperationType, OperationData>();
    // add in all the operations
    // since they will all be applied unless changed
    OperationType[] types = OperationType.values();
    for (OperationType type : types) {
      base.put(type, new OperationData(Distribution.UNIFORM, null));
    }
    return base;
  }

  /**
   * Handles the specific task of merging operations from the command line or
   * extractor object into the base configuration provided
   * 
   * @param opts
   *          the parsed command line option output
   * @param base
   *          the base configuration to merge with
   * @param extractor
   *          the access object to fetch operations from if none from the
   *          command line
   * @return merged configuration object
   * @throws ConfigException
   *           when verification fails
   */
  private Configuration handleOperations(ParsedOutput opts, Configuration base,
      ConfigExtractor extractor) throws ConfigException {
    // get the base set to start off with
    Map<OperationType, OperationData> operations = getBaseOperations();
    // merge with what is coming from config
    Map<OperationType, OperationData> cfgOperations = extractor.getOperations();
    for (OperationType opType : cfgOperations.keySet()) {
      operations.put(opType, cfgOperations.get(opType));
    }
    // see if any coming in from the command line
    for (OperationType opType : OperationType.values()) {
      String opName = opType.lowerName();
      String opVal = opts.getValue(opName);
      if (opVal != null) {
        operations.put(opType, new OperationData(opVal));
      }
    }
    // remove those with <= zero percent
    {
      Map<OperationType, OperationData> cleanedOps = new HashMap<OperationType, OperationData>();
      for (OperationType opType : operations.keySet()) {
        OperationData data = operations.get(opType);
        if (data.getPercent() == null || data.getPercent() > 0.0d) {
          cleanedOps.put(opType, data);
        }
      }
      operations = cleanedOps;
    }
    if (operations.isEmpty()) {
      throw new ConfigException("No operations provided!");
    }
    // verify and adjust
    double currPct = 0;
    int needFill = 0;
    for (OperationType type : operations.keySet()) {
      OperationData op = operations.get(type);
      if (op.getPercent() != null) {
        currPct += op.getPercent();
      } else {
        needFill++;
      }
    }
    if (currPct > 1) {
      throw new ConfigException(
          "Unable to have accumlative percent greater than 100%");
    }
    if (needFill > 0 && currPct < 1) {
      double leftOver = 1.0 - currPct;
      Map<OperationType, OperationData> mpcp = new HashMap<OperationType, OperationData>();
      for (OperationType type : operations.keySet()) {
        OperationData op = operations.get(type);
        if (op.getPercent() == null) {
          op = new OperationData(op.getDistribution(), (leftOver / needFill));
        }
        mpcp.put(type, op);
      }
      operations = mpcp;
    } else if (needFill == 0 && currPct < 1) {
      // redistribute
      double leftOver = 1.0 - currPct;
      Map<OperationType, OperationData> mpcp = new HashMap<OperationType, OperationData>();
      double each = leftOver / operations.keySet().size();
      for (OperationType t : operations.keySet()) {
        OperationData op = operations.get(t);
        op = new OperationData(op.getDistribution(), (op.getPercent() + each));
        mpcp.put(t, op);
      }
      operations = mpcp;
    } else if (needFill > 0 && currPct >= 1) {
      throw new ConfigException(needFill
          + " unfilled operations but no percentage left to fill with");
    }
    // save into base
    for (OperationType opType : operations.keySet()) {
      String opName = opType.lowerName();
      OperationData opData = operations.get(opType);
      String distr = opData.getDistribution().lowerName();
      String ratio = new Double(opData.getPercent() * 100.0d).toString();
      base.set(String.format(Constants.OP, opName), opData.toString());
      base.set(String.format(Constants.OP_DISTR, opName), distr);
      base.set(String.format(Constants.OP_PERCENT, opName), ratio);
    }
    return base;
  }

  /**
   * Handles merging all options and verifying from the given command line
   * output and the given base configuration and returns the merged
   * configuration
   * 
   * @param opts
   *          the parsed command line option output
   * @param base
   *          the base configuration to merge with
   * @return the merged configuration
   * @throws ConfigException
   */
  private Configuration handleOptions(ParsedOutput opts, Configuration base)
      throws ConfigException {
    // ensure variables are overwritten and verified
    ConfigExtractor extractor = new ConfigExtractor(base);
    // overwrite the map amount and check to ensure > 0
    {
      Integer mapAmount = null;
      try {
        mapAmount = extractor.getMapAmount(opts.getValue(ConfigOption.MAPS
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging map amount", e);
      }
      if (mapAmount != null) {
        if (mapAmount <= 0) {
          throw new ConfigException(
              "Map amount can not be less than or equal to zero");
        }
        base.set(ConfigOption.MAPS.getCfgOption(), mapAmount.toString());
      }
    }
    // overwrite the reducer amount and check to ensure > 0
    {
      Integer reduceAmount = null;
      try {
        reduceAmount = extractor.getMapAmount(opts.getValue(ConfigOption.REDUCES
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging reducer amount", e);
      }
      if (reduceAmount != null) {
        if (reduceAmount <= 0) {
          throw new ConfigException(
              "Reducer amount can not be less than or equal to zero");
        }
        base.set(ConfigOption.REDUCES.getCfgOption(), reduceAmount.toString());
      }
    }
    // overwrite the duration amount and ensure > 0
    {
      Integer duration = null;
      try {
        duration = extractor.getDuration(opts.getValue(ConfigOption.DURATION
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging duration", e);
      }
      if (duration != null) {
        if (duration <= 0) {
          throw new ConfigException(
              "Duration can not be less than or equal to zero");
        }
        base.set(ConfigOption.DURATION.getCfgOption(), duration.toString());
      }
    }
    // overwrite the operation amount and ensure > 0
    {
      Integer operationAmount = null;
      try {
        operationAmount = extractor.getOpCount(opts.getValue(ConfigOption.OPS
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging operation amount", e);
      }
      if (operationAmount != null) {
        if (operationAmount <= 0) {
          throw new ConfigException(
              "Operation amount can not be less than or equal to zero");
        }
        base.set(ConfigOption.OPS.getCfgOption(), operationAmount.toString());
      }
    }
    // overwrite the exit on error setting
    {
      try {
        boolean exitOnError = extractor.shouldExitOnFirstError(opts
            .getValue(ConfigOption.EXIT_ON_ERROR.getOpt()));
        base.setBoolean(ConfigOption.EXIT_ON_ERROR.getCfgOption(), exitOnError);
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging exit on error value", e);
      }
    }
    // verify and set file limit and ensure > 0
    {
      Integer fileAm = null;
      try {
        fileAm = extractor.getTotalFiles(opts.getValue(ConfigOption.FILES
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging total file limit amount", e);
      }
      if (fileAm != null) {
        if (fileAm <= 0) {
          throw new ConfigException(
              "File amount can not be less than or equal to zero");
        }
        base.set(ConfigOption.FILES.getCfgOption(), fileAm.toString());
      }
    }
    // set the grid queue to run on
    {
      try {
        String qname = extractor.getQueueName(opts
            .getValue(ConfigOption.QUEUE_NAME.getOpt()));
        if (qname != null) {
          base.set(ConfigOption.QUEUE_NAME.getCfgOption(), qname);
        }
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging queue name", e);
      }
    }
    // verify and set the directory limit and ensure > 0
    {
      Integer directoryLimit = null;
      try {
        directoryLimit = extractor.getDirSize(opts
            .getValue(ConfigOption.DIR_SIZE.getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging directory file limit", e);
      }
      if (directoryLimit != null) {
        if (directoryLimit <= 0) {
          throw new ConfigException(
              "Directory file limit can not be less than or equal to zero");
        }
        base.set(ConfigOption.DIR_SIZE.getCfgOption(), directoryLimit
            .toString());
      }
    }
    // set the base directory
    {
      Path basedir = null;
      try {
        basedir = extractor.getBaseDirectory(opts
            .getValue(ConfigOption.BASE_DIR.getOpt()));
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging base directory",
            e);
      }
      if (basedir != null) {
        // always ensure in slive dir
        basedir = new Path(basedir, Constants.BASE_DIR);
        base.set(ConfigOption.BASE_DIR.getCfgOption(), basedir.toString());
      }
    }
    // set the result file
    {
      String fn = null;
      try {
        fn = extractor.getResultFile(opts.getValue(ConfigOption.RESULT_FILE
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging result file", e);
      }
      if (fn != null) {
        base.set(ConfigOption.RESULT_FILE.getCfgOption(), fn);
      }
    }
    {
      String fn = null;
      try {
        fn = extractor.getResultFile(opts.getValue(ConfigOption.RESULT_FILE
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging result file", e);
      }
      if (fn != null) {
        base.set(ConfigOption.RESULT_FILE.getCfgOption(), fn);
      }
    }
    // set the operations
    {
      try {
        base = handleOperations(opts, base, extractor);
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging operations", e);
      }
    }
    // set the replication amount range
    {
      Range<Short> replicationAm = null;
      try {
        replicationAm = extractor.getReplication(opts
            .getValue(ConfigOption.REPLICATION_AM.getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging replication amount range", e);
      }
      if (replicationAm != null) {
        int minRepl = base.getInt(Constants.MIN_REPLICATION, 1);
        if (replicationAm.getLower() < minRepl) {
          throw new ConfigException(
              "Replication amount minimum is less than property configured minimum "
                  + minRepl);
        }
        if (replicationAm.getLower() > replicationAm.getUpper()) {
          throw new ConfigException(
              "Replication amount minimum is greater than its maximum");
        }
        if (replicationAm.getLower() <= 0) {
          throw new ConfigException(
              "Replication amount minimum must be greater than zero");
        }
        base.set(ConfigOption.REPLICATION_AM.getCfgOption(), replicationAm
            .toString());
      }
    }
    // set the sleep range
    {
      Range<Long> sleepRange = null;
      try {
        sleepRange = extractor.getSleepRange(opts
            .getValue(ConfigOption.SLEEP_TIME.getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging sleep size range", e);
      }
      if (sleepRange != null) {
        if (sleepRange.getLower() > sleepRange.getUpper()) {
          throw new ConfigException(
              "Sleep range minimum is greater than its maximum");
        }
        if (sleepRange.getLower() <= 0) {
          throw new ConfigException(
              "Sleep range minimum must be greater than zero");
        }
        base.set(ConfigOption.SLEEP_TIME.getCfgOption(), sleepRange.toString());
      }
    }
    // set the packet size if given
    {
      String pSize = opts.getValue(ConfigOption.PACKET_SIZE.getOpt());
      if (pSize == null) {
        pSize = ConfigOption.PACKET_SIZE.getDefault();
      }
      if (pSize != null) {
        try {
          Long packetSize = StringUtils.TraditionalBinaryPrefix
              .string2long(pSize);
          base.set(ConfigOption.PACKET_SIZE.getCfgOption(), packetSize
              .toString());
        } catch (Exception e) {
          throw new ConfigException(
              "Error extracting & merging write packet size", e);
        }
      }
    }
    // set the block size range
    {
      Range<Long> blockSize = null;
      try {
        blockSize = extractor.getBlockSize(opts
            .getValue(ConfigOption.BLOCK_SIZE.getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging block size range", e);
      }
      if (blockSize != null) {
        if (blockSize.getLower() > blockSize.getUpper()) {
          throw new ConfigException(
              "Block size minimum is greater than its maximum");
        }
        if (blockSize.getLower() <= 0) {
          throw new ConfigException(
              "Block size minimum must be greater than zero");
        }
        // ensure block size is a multiple of BYTES_PER_CHECKSUM
        // if a value is set in the configuration
        Long bytesPerChecksum = extractor.getByteCheckSum();
        if (bytesPerChecksum != null) {
          if ((blockSize.getLower() % bytesPerChecksum) != 0) {
            throw new ConfigException(
                "Blocksize lower bound must be a multiple of "
                    + bytesPerChecksum);
          }
          if ((blockSize.getUpper() % bytesPerChecksum) != 0) {
            throw new ConfigException(
                "Blocksize upper bound must be a multiple of "
                    + bytesPerChecksum);
          }
        }
        base.set(ConfigOption.BLOCK_SIZE.getCfgOption(), blockSize.toString());
      }
    }
    // set the read size range
    {
      Range<Long> readSize = null;
      try {
        readSize = extractor.getReadSize(opts.getValue(ConfigOption.READ_SIZE
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException("Error extracting & merging read size range",
            e);
      }
      if (readSize != null) {
        if (readSize.getLower() > readSize.getUpper()) {
          throw new ConfigException(
              "Read size minimum is greater than its maximum");
        }
        if (readSize.getLower() < 0) {
          throw new ConfigException(
              "Read size minimum must be greater than or equal to zero");
        }
        base.set(ConfigOption.READ_SIZE.getCfgOption(), readSize.toString());
      }
    }
    // set the write size range
    {
      Range<Long> writeSize = null;
      try {
        writeSize = extractor.getWriteSize(opts
            .getValue(ConfigOption.WRITE_SIZE.getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging write size range", e);
      }
      if (writeSize != null) {
        if (writeSize.getLower() > writeSize.getUpper()) {
          throw new ConfigException(
              "Write size minimum is greater than its maximum");
        }
        if (writeSize.getLower() < 0) {
          throw new ConfigException(
              "Write size minimum must be greater than or equal to zero");
        }
        base.set(ConfigOption.WRITE_SIZE.getCfgOption(), writeSize.toString());
      }
    }
    // set the append size range
    {
      Range<Long> appendSize = null;
      try {
        appendSize = extractor.getAppendSize(opts
            .getValue(ConfigOption.APPEND_SIZE.getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging append size range", e);
      }
      if (appendSize != null) {
        if (appendSize.getLower() > appendSize.getUpper()) {
          throw new ConfigException(
              "Append size minimum is greater than its maximum");
        }
        if (appendSize.getLower() < 0) {
          throw new ConfigException(
              "Append size minimum must be greater than or equal to zero");
        }
        base
            .set(ConfigOption.APPEND_SIZE.getCfgOption(), appendSize.toString());
      }
    }
    // set the seed
    {
      Long seed = null;
      try {
        seed = extractor.getRandomSeed(opts.getValue(ConfigOption.RANDOM_SEED
            .getOpt()));
      } catch (Exception e) {
        throw new ConfigException(
            "Error extracting & merging random number seed", e);
      }
      if (seed != null) {
        base.set(ConfigOption.RANDOM_SEED.getCfgOption(), seed.toString());
      }
    }
    return base;
  }
}
