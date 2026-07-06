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
package org.apache.hadoop.tools.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.tools.dynamometer.workloadgenerator.WorkloadDriver;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper.REPLAYCOUNTERS;
import org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper.ReplayCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper.INDIVIDUAL_COMMANDS_COUNTER_GROUP;
import static org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper.INDIVIDUAL_COMMANDS_COUNT_SUFFIX;
import static org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper.INDIVIDUAL_COMMANDS_INVALID_SUFFIX;
import static org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper.INDIVIDUAL_COMMANDS_LATENCY_SUFFIX;

/**
 * This class replays each audit log entry at a specified timestamp in the
 * future. Each of these threads maintains a {@link DelayQueue} into which items
 * are inserted by the {@link AuditReplayMapper}. Once an item is ready, this
 * thread will fetch the command from the queue and attempt to replay it.
 */
public class AuditReplayThread extends Thread {

  private static final Logger LOG =
      LoggerFactory.getLogger(AuditReplayThread.class);

  private DelayQueue<AuditReplayCommand> commandQueue;
  private ConcurrentMap<String, FileSystem> fsCache;
  private URI namenodeUri;
  private UserGroupInformation loginUser;
  private Configuration mapperConf;
  // If any exception is encountered it will be stored here
  private Exception exception;
  private long startTimestampMs;
  private boolean createBlocks;

  // Counters are not thread-safe so we store a local mapping in our thread
  // and merge them all together at the end.
  private Map<REPLAYCOUNTERS, Counter> replayCountersMap = new HashMap<>();
  private Map<String, Counter> individualCommandsMap = new HashMap<>();
  private Map<UserCommandKey, CountTimeWritable> commandLatencyMap
      = new HashMap<>();

  AuditReplayThread(Mapper.Context mapperContext,
      DelayQueue<AuditReplayCommand> queue,
      ConcurrentMap<String, FileSystem> fsCache) throws IOException {
    commandQueue = queue;
    this.fsCache = fsCache;
    loginUser = UserGroupInformation.getLoginUser();
    mapperConf = mapperContext.getConfiguration();
    namenodeUri = URI.create(mapperConf.get(WorkloadDriver.NN_URI));
    startTimestampMs = mapperConf.getLong(WorkloadDriver.START_TIMESTAMP_MS,
        -1);
    createBlocks = mapperConf.getBoolean(AuditReplayMapper.CREATE_BLOCKS_KEY,
        AuditReplayMapper.CREATE_BLOCKS_DEFAULT);
    LOG.info("Start timestamp: " + startTimestampMs);
    for (REPLAYCOUNTERS rc : REPLAYCOUNTERS.values()) {
      replayCountersMap.put(rc, new GenericCounter());
    }
    for (ReplayCommand replayCommand : ReplayCommand.values()) {
      individualCommandsMap.put(
          replayCommand + INDIVIDUAL_COMMANDS_COUNT_SUFFIX,
          new GenericCounter());
      individualCommandsMap.put(
          replayCommand + INDIVIDUAL_COMMANDS_LATENCY_SUFFIX,
          new GenericCounter());
      individualCommandsMap.put(
          replayCommand + INDIVIDUAL_COMMANDS_INVALID_SUFFIX,
          new GenericCounter());
    }
  }

  /**
   * Merge all of this thread's counter values into the counters contained
   * within the passed context.
   *
   * @param context The context holding the counters to increment.
   */
  void drainCounters(Mapper.Context context) {
    for (Map.Entry<REPLAYCOUNTERS, Counter> ent : replayCountersMap
        .entrySet()) {
      context.getCounter(ent.getKey()).increment(ent.getValue().getValue());
    }
    for (Map.Entry<String, Counter> ent : individualCommandsMap.entrySet()) {
      context.getCounter(INDIVIDUAL_COMMANDS_COUNTER_GROUP, ent.getKey())
          .increment(ent.getValue().getValue());
    }
  }

  void drainCommandLatencies(Mapper.Context context)
      throws InterruptedException, IOException {
    for (Map.Entry<UserCommandKey, CountTimeWritable> ent
        : commandLatencyMap.entrySet()) {
      context.write(ent.getKey(), ent.getValue());
    }
  }

  /**
   * Add a command to this thread's processing queue.
   *
   * @param cmd Command to add.
   */
  void addToQueue(AuditReplayCommand cmd) {
    commandQueue.put(cmd);
  }

  /**
   * Get the Exception that caused this thread to stop running, if any, else
   * null. Should not be called until this thread has already completed (i.e.,
   * after {@link #join()} has been called).
   *
   * @return The exception which was thrown, if any.
   */
  Exception getException() {
    return exception;
  }

  @Override
  public void run() {
    long currentEpoch = System.currentTimeMillis();
    long delay = startTimestampMs - currentEpoch;
    try {
      if (delay > 0) {
        LOG.info("Sleeping for " + delay + " ms");
        Thread.sleep(delay);
      } else {
        LOG.warn("Starting late by " + (-1 * delay) + " ms");
      }

      AuditReplayCommand cmd = commandQueue.take();
      while (!cmd.isPoison()) {
        replayCountersMap.get(REPLAYCOUNTERS.TOTALCOMMANDS).increment(1);
        delay = cmd.getDelay(TimeUnit.MILLISECONDS);
        if (delay < -5) { // allow some tolerance here
          replayCountersMap.get(REPLAYCOUNTERS.LATECOMMANDS).increment(1);
          replayCountersMap.get(REPLAYCOUNTERS.LATECOMMANDSTOTALTIME)
              .increment(-1 * delay);
        }
        if (!replayLog(cmd)) {
          replayCountersMap.get(REPLAYCOUNTERS.TOTALINVALIDCOMMANDS)
              .increment(1);
        }
        cmd = commandQueue.take();
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted; exiting from thread.", e);
    } catch (Exception e) {
      exception = e;
      LOG.error("ReplayThread encountered exception; exiting.", e);
    }
  }

  /**
   * Attempt to replay the provided command. Updates counters accordingly.
   *
   * @param command The command to replay
   * @return True iff the command was successfully replayed (i.e., no exceptions
   *         were thrown).
   */
  private boolean replayLog(final AuditReplayCommand command) {
    final String src = command.getSrc();
    final String dst = command.getDest();
    FileSystem proxyFs = fsCache.get(command.getSimpleUgi());
    if (proxyFs == null) {
      UserGroupInformation ugi = UserGroupInformation
          .createProxyUser(command.getSimpleUgi(), loginUser);
      proxyFs = ugi.doAs((PrivilegedAction<FileSystem>) () -> {
        try {
          FileSystem fs = new DistributedFileSystem();
          fs.initialize(namenodeUri, mapperConf);
          return fs;
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      });
      fsCache.put(command.getSimpleUgi(), proxyFs);
    }
    final FileSystem fs = proxyFs;
    ReplayCommand replayCommand;
    try {
      replayCommand = ReplayCommand
          .valueOf(command.getCommand().split(" ")[0].toUpperCase());
    } catch (IllegalArgumentException iae) {
      LOG.warn("Unsupported/invalid command: " + command);
      replayCountersMap.get(REPLAYCOUNTERS.TOTALUNSUPPORTEDCOMMANDS)
          .increment(1);
      return false;
    }
    try {
      long startTime = System.currentTimeMillis();
      switch (replayCommand) {
      case CREATE:
        FSDataOutputStream fsDos = fs.create(new Path(src));
        if (createBlocks) {
          fsDos.writeByte(0);
        }
        fsDos.close();
        break;
      case GETFILEINFO:
        fs.getFileStatus(new Path(src));
        break;
      case CONTENTSUMMARY:
        fs.getContentSummary(new Path(src));
        break;
      case MKDIRS:
        fs.mkdirs(new Path(src));
        break;
      case RENAME:
        fs.rename(new Path(src), new Path(dst));
        break;
      case LISTSTATUS:
        ((DistributedFileSystem) fs).getClient().listPaths(src,
            HdfsFileStatus.EMPTY_NAME);
        break;
      case APPEND:
        fs.append(new Path(src));
        return true;
      case DELETE:
        fs.delete(new Path(src), true);
        break;
      case OPEN:
        fs.open(new Path(src)).close();
        break;
      case SETPERMISSION:
        fs.setPermission(new Path(src), FsPermission.getDefault());
        break;
      case SETOWNER:
        fs.setOwner(new Path(src),
            UserGroupInformation.getCurrentUser().getShortUserName(),
            UserGroupInformation.getCurrentUser().getPrimaryGroupName());
        break;
      case SETTIMES:
        fs.setTimes(new Path(src), System.currentTimeMillis(),
            System.currentTimeMillis());
        break;
      case SETREPLICATION:
        fs.setReplication(new Path(src), (short) 1);
        break;
      case CONCAT:
        // dst is like [path1, path2] - strip brackets and split on comma
        String bareDist = dst.length() < 2 ? ""
            : dst.substring(1, dst.length() - 1).trim();
        List<Path> dsts = new ArrayList<>();
        for (String s : Splitter.on(",").omitEmptyStrings().trimResults()
            .split(bareDist)) {
          dsts.add(new Path(s));
        }
        fs.concat(new Path(src), dsts.toArray(new Path[] {}));
        break;
      default:
        throw new RuntimeException("Unexpected command: " + replayCommand);
      }
      long latency = System.currentTimeMillis() - startTime;

      UserCommandKey userCommandKey = new UserCommandKey(command.getSimpleUgi(),
          replayCommand.toString(), replayCommand.getType().toString());
      commandLatencyMap.putIfAbsent(userCommandKey, new CountTimeWritable());
      CountTimeWritable latencyWritable = commandLatencyMap.get(userCommandKey);
      latencyWritable.setCount(latencyWritable.getCount() + 1);
      latencyWritable.setTime(latencyWritable.getTime() + latency);

      switch (replayCommand.getType()) {
      case WRITE:
        replayCountersMap.get(REPLAYCOUNTERS.TOTALWRITECOMMANDLATENCY)
            .increment(latency);
        replayCountersMap.get(REPLAYCOUNTERS.TOTALWRITECOMMANDS).increment(1);
        break;
      case READ:
        replayCountersMap.get(REPLAYCOUNTERS.TOTALREADCOMMANDLATENCY)
            .increment(latency);
        replayCountersMap.get(REPLAYCOUNTERS.TOTALREADCOMMANDS).increment(1);
        break;
      default:
        throw new RuntimeException("Unexpected command type: "
            + replayCommand.getType());
      }
      individualCommandsMap
          .get(replayCommand + INDIVIDUAL_COMMANDS_LATENCY_SUFFIX)
          .increment(latency);
      individualCommandsMap
          .get(replayCommand + INDIVIDUAL_COMMANDS_COUNT_SUFFIX).increment(1);
      return true;
    } catch (IOException e) {
      LOG.debug("IOException: " + e.getLocalizedMessage());
      individualCommandsMap
          .get(replayCommand + INDIVIDUAL_COMMANDS_INVALID_SUFFIX).increment(1);
      return false;
    }
  }
}
