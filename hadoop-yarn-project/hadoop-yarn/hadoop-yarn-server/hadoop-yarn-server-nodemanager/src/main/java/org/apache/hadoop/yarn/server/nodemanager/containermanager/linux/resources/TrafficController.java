/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper around the 'tc' tool. Provides access to a very specific subset of
 * the functionality provided by the tc tool.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable class TrafficController {
  private static final Logger LOG =
       LoggerFactory.getLogger(TrafficController.class);
  private static final int ROOT_QDISC_HANDLE = 42;
  private static final int ZERO_CLASS_ID = 0;
  private static final int ROOT_CLASS_ID = 1;
  /** Traffic shaping class used for all unclassified traffic */
  private static final int DEFAULT_CLASS_ID = 2;
  /** Traffic shaping class used for all YARN traffic */
  private static final int YARN_ROOT_CLASS_ID = 3;
  /** Classes 0-3 are used already. We need to ensure that container classes
   * do not collide with these classids.
   */
  private static final int MIN_CONTAINER_CLASS_ID = 4;
  /** This is the number of distinct (container) traffic shaping classes
   * that are supported */
  private static final int MAX_CONTAINER_CLASSES = 1024;

  private static final String MBIT_SUFFIX = "mbit";
  private static final String TMP_FILE_PREFIX = "tc.";
  private static final String TMP_FILE_SUFFIX = ".cmds";

  /** Root queuing discipline attached to the root of the interface */
  private static final String FORMAT_QDISC_ADD_TO_ROOT_WITH_DEFAULT =
      "qdisc add dev %s root handle %d: htb default %s";
  /** Specifies a cgroup/classid based filter - based on the classid associated
   * with the outbound packet, the corresponding traffic shaping rule is used
   * . Please see tc documentation for additional details.
   */
  private static final String FORMAT_FILTER_CGROUP_ADD_TO_PARENT =
      "filter add dev %s parent %d: protocol ip prio 10 handle 1: cgroup";
  /** Standard format for adding a traffic shaping class to a parent, with
   * the specified bandwidth limits
   */
  private static final String FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES =
      "class add dev %s parent %d:%d classid %d:%d htb rate %s ceil %s";
  /** Standard format to delete a traffic shaping class */
  private static final String FORMAT_DELETE_CLASS =
      "class del dev %s classid %d:%d";
  /** Format of the classid that is to be used with the net_cls cgroup. Needs
   * to be of the form 0xAAAABBBB */
  private static final String FORMAT_NET_CLS_CLASS_ID = "0x%04d%04d";
  /** Commands to read the qdsic(s)/filter(s)/class(es) associated with an
   * interface
   */
  private static final String FORMAT_READ_STATE =
      "qdisc show dev %1$s%n" +
          "filter show dev %1$s%n" +
          "class show dev %1$s";
  private static final String FORMAT_READ_CLASSES = "class show dev %s";
  /** Delete a qdisc and all its children - classes/filters etc */
  private static final String FORMAT_WIPE_STATE =
      "qdisc del dev %s parent root";

  private final Configuration conf;
  //Used to store the set of classids in use for container classes
  private final BitSet classIdSet;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;

  private String tmpDirPath;
  private String device;
  private int rootBandwidthMbit;
  private int yarnBandwidthMbit;
  private int defaultClassBandwidthMbit;

  TrafficController(Configuration conf, PrivilegedOperationExecutor exec) {
    this.conf = conf;
    this.classIdSet = new BitSet(MAX_CONTAINER_CLASSES);
    this.privilegedOperationExecutor = exec;
  }

  /**
   * Bootstrap tc configuration
   */
  public void bootstrap(String device, int rootBandwidthMbit, int
      yarnBandwidthMbit)
      throws ResourceHandlerException {
    if (device == null) {
      throw new ResourceHandlerException("device cannot be null!");
    }

    String tmpDirBase = conf.get("hadoop.tmp.dir");
    if (tmpDirBase == null) {
      throw new ResourceHandlerException("hadoop.tmp.dir not set!");
    }
    tmpDirPath = tmpDirBase + "/nm-tc-rules";

    File tmpDir = new File(tmpDirPath);
    if (!(tmpDir.exists() || tmpDir.mkdirs())) {
      LOG.warn("Unable to create directory: " + tmpDirPath);
      throw new ResourceHandlerException("Unable to create directory: " +
          tmpDirPath);
    }

    this.device = device;
    this.rootBandwidthMbit = rootBandwidthMbit;
    this.yarnBandwidthMbit = yarnBandwidthMbit;
    defaultClassBandwidthMbit = (rootBandwidthMbit - yarnBandwidthMbit) <= 0
        ? rootBandwidthMbit : (rootBandwidthMbit - yarnBandwidthMbit);

    boolean recoveryEnabled = conf.getBoolean(YarnConfiguration
        .NM_RECOVERY_ENABLED, YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    String state = null;

    if (!recoveryEnabled) {
      LOG.info("NM recovery is not enabled. We'll wipe tc state before proceeding.");
    } else {
      //NM recovery enabled - run a state check
      state = readState();
      if (checkIfAlreadyBootstrapped(state)) {
        LOG.info("TC configuration is already in place. Not wiping state.");

        //We already have the list of existing container classes, if any
        //that were created after bootstrapping
        reacquireContainerClasses(state);
        return;
      } else {
        LOG.info("TC configuration is incomplete. Wiping tc state before proceeding");
      }
    }

    wipeState(); //start over in case preview bootstrap was incomplete
    initializeState();
  }

  private void initializeState() throws ResourceHandlerException {
    LOG.info("Initializing tc state.");

    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_MODIFY_STATE)
        .addRootQDisc()
        .addCGroupFilter()
        .addClassToRootQDisc(rootBandwidthMbit)
        .addDefaultClass(defaultClassBandwidthMbit, rootBandwidthMbit)
            //yarn bandwidth is capped with rate = ceil
        .addYARNRootClass(yarnBandwidthMbit, yarnBandwidthMbit);
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      privilegedOperationExecutor.executePrivilegedOperation(op, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to bootstrap outbound bandwidth configuration");

      throw new ResourceHandlerException(
          "Failed to bootstrap outbound bandwidth configuration", e);
    }
  }

  /**
   * Function to check if the interface in use has already been fully
   * bootstrapped with the required tc configuration
   *
   * @return boolean indicating the result of the check
   */
  private boolean checkIfAlreadyBootstrapped(String state)
      throws ResourceHandlerException {
    List<String> regexes = new ArrayList<>();

    //root qdisc
    regexes.add(String.format("^qdisc htb %d: root(.)*$",
        ROOT_QDISC_HANDLE));
    //cgroup filter
    regexes.add(String.format("^filter parent %d: protocol ip " +
        "(.)*cgroup(.)*$", ROOT_QDISC_HANDLE));
    //root, default and yarn classes
    regexes.add(String.format("^class htb %d:%d root(.)*$",
        ROOT_QDISC_HANDLE, ROOT_CLASS_ID));
    regexes.add(String.format("^class htb %d:%d parent %d:%d(.)*$",
        ROOT_QDISC_HANDLE, DEFAULT_CLASS_ID, ROOT_QDISC_HANDLE, ROOT_CLASS_ID));
    regexes.add(String.format("^class htb %d:%d parent %d:%d(.)*$",
        ROOT_QDISC_HANDLE, YARN_ROOT_CLASS_ID, ROOT_QDISC_HANDLE,
        ROOT_CLASS_ID));

    for (String regex : regexes) {
      Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

      if (pattern.matcher(state).find()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Matched regex: " + regex);
        }
      } else {
        String logLine = new StringBuffer("Failed to match regex: ")
              .append(regex).append(" Current state: ").append(state).toString();
        LOG.warn(logLine);
        return false;
      }
    }

    LOG.info("Bootstrap check succeeded");

    return true;
  }

  private String readState() throws ResourceHandlerException {
    //Sample state output:
    //    qdisc htb 42: root refcnt 2 r2q 10 default 2 direct_packets_stat 0
    //    filter parent 42: protocol ip pref 10 cgroup handle 0x1
    //
    //    filter parent 42: protocol ip pref 10 cgroup handle 0x1
    //
    //    class htb 42:1 root rate 10000Kbit ceil 10000Kbit burst 1600b cburst 1600b
    //    class htb 42:2 parent 42:1 prio 0 rate 3000Kbit ceil 10000Kbit burst 1599b cburst 1600b
    //    class htb 42:3 parent 42:1 prio 0 rate 7000Kbit ceil 7000Kbit burst 1598b cburst 1598b

    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_READ_STATE)
        .readState();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      String output =
          privilegedOperationExecutor.executePrivilegedOperation(op, true);

      if (LOG.isDebugEnabled()) {
        LOG.debug("TC state: %n" + output);
      }

      return output;
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to bootstrap outbound bandwidth rules");
      throw new ResourceHandlerException(
          "Failed to bootstrap outbound bandwidth rules", e);
    }
  }

  private void wipeState() throws ResourceHandlerException {
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_MODIFY_STATE)
        .wipeState();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      LOG.info("Wiping tc state.");
      privilegedOperationExecutor.executePrivilegedOperation(op, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to wipe tc state. This could happen if the interface" +
          " is already in its default state. Ignoring.");
      //Ignoring this exception. This could happen if the interface is already
      //in its default state. For this reason we don't throw a
      //ResourceHandlerException here.
    }
  }

  /**
   * Parses the current state looks for classids already in use
   */
  private void reacquireContainerClasses(String state) {
    //At this point we already have already successfully passed
    //checkIfAlreadyBootstrapped() - so we know that at least the
    //root classes are in place.
    String tcClassesStr = state.substring(state.indexOf("class"));
    //one class per line - the results of the split will need to trimmed
    String[] tcClasses = Pattern.compile("$", Pattern.MULTILINE)
        .split(tcClassesStr);
    Pattern tcClassPattern = Pattern.compile(String.format(
        "class htb %d:(\\d+) .*", ROOT_QDISC_HANDLE));

    synchronized (classIdSet) {
      for (String tcClassSplit : tcClasses) {
        String tcClass = tcClassSplit.trim();

        if (!tcClass.isEmpty()) {
          Matcher classMatcher = tcClassPattern.matcher(tcClass);
          if (classMatcher.matches()) {
            int classId = Integer.parseInt(classMatcher.group(1));
            if (classId >= MIN_CONTAINER_CLASS_ID) {
              classIdSet.set(classId - MIN_CONTAINER_CLASS_ID);
              LOG.info("Reacquired container classid: " + classId);
            }
          } else {
            LOG.warn("Unable to match classid in string:" + tcClass);
          }
        }
      }
    }
  }

  public Map<Integer, Integer> readStats() throws ResourceHandlerException {
    BatchBuilder builder = new BatchBuilder(PrivilegedOperation.
        OperationType.TC_READ_STATS)
        .readClasses();
    PrivilegedOperation op = builder.commitBatchToTempFile();

    try {
      String output =
          privilegedOperationExecutor.executePrivilegedOperation(op, true);

      if (LOG.isDebugEnabled()) {
        LOG.debug("TC stats output:" + output);
      }

      Map<Integer, Integer> classIdBytesStats = parseStatsString(output);

      if (LOG.isDebugEnabled()) {
        LOG.debug("classId -> bytes sent %n" + classIdBytesStats);
      }

      return classIdBytesStats;
    } catch (PrivilegedOperationException e) {
      LOG.warn("Failed to get tc stats");
      throw new ResourceHandlerException("Failed to get tc stats", e);
    }
  }

  private Map<Integer, Integer> parseStatsString(String stats) {
    //Example class stats segment (multiple present in tc output)
    //  class htb 42:4 parent 42:3 prio 0 rate 1000Kbit ceil 7000Kbit burst1600b cburst 1598b
    //   Sent 77921300 bytes 52617 pkt (dropped 0, overlimits 0 requeues 0)
    //   rate 6973Kbit 589pps backlog 0b 39p requeues 0
    //   lended: 3753 borrowed: 22514 giants: 0
    //   tokens: -122164 ctokens: -52488

    String[] lines = Pattern.compile("$", Pattern.MULTILINE)
        .split(stats);
    Pattern tcClassPattern = Pattern.compile(String.format(
        "class htb %d:(\\d+) .*", ROOT_QDISC_HANDLE));
    Pattern bytesPattern = Pattern.compile("Sent (\\d+) bytes.*");

    int currentClassId = -1;
    Map<Integer, Integer> containerClassIdStats = new HashMap<>();

    for (String lineSplit : lines) {
      String line = lineSplit.trim();

      if (!line.isEmpty()) {
        //Check if we encountered a stats segment for a container class
        Matcher classMatcher = tcClassPattern.matcher(line);
        if (classMatcher.matches()) {
          int classId = Integer.parseInt(classMatcher.group(1));
          if (classId >= MIN_CONTAINER_CLASS_ID) {
            currentClassId = classId;
            continue;
          }
        }

        //Check if we encountered a stats line
        Matcher bytesMatcher = bytesPattern.matcher(line);
        if (bytesMatcher.matches()) {
          //we found at least one class segment
          if (currentClassId != -1) {
            int bytes = Integer.parseInt(bytesMatcher.group(1));
            containerClassIdStats.put(currentClassId, bytes);
          } else {
            LOG.warn("Matched a 'bytes sent' line outside of a class stats " +
                  "segment : " + line);
          }
          continue;
        }

        //skip other kinds of non-empty lines - since we aren't interested in
        //them.
      }
    }

    return containerClassIdStats;
  }

  /**
   * Returns a formatted string for attaching a qdisc to the root of the
   * device/interface. Additional qdisc
   * parameters can be supplied - for example, the default 'class' to use for
   * incoming packets
   */
  private String getStringForAddRootQDisc() {
    return String.format(FORMAT_QDISC_ADD_TO_ROOT_WITH_DEFAULT, device,
        ROOT_QDISC_HANDLE, DEFAULT_CLASS_ID);
  }

  /**
   * Returns a formatted string for a filter that matches packets based on the
   * presence of net_cls classids
   */
  private String getStringForaAddCGroupFilter() {
    return String.format(FORMAT_FILTER_CGROUP_ADD_TO_PARENT, device,
        ROOT_QDISC_HANDLE);
  }

  /**
   * Get the next available classid. This has to be released post container
   * complete
   */
  public int getNextClassId() throws ResourceHandlerException {
    synchronized (classIdSet) {
      int index = classIdSet.nextClearBit(0);
      if (index >= MAX_CONTAINER_CLASSES) {
        throw new ResourceHandlerException("Reached max container classes: "
            + MAX_CONTAINER_CLASSES);
      }
      classIdSet.set(index);
      return (index + MIN_CONTAINER_CLASS_ID);
    }
  }

  public void releaseClassId(int classId) throws ResourceHandlerException {
    synchronized (classIdSet) {
      int index = classId - MIN_CONTAINER_CLASS_ID;
      if (index < 0 || index >= MAX_CONTAINER_CLASSES) {
        throw new ResourceHandlerException("Invalid incoming classId: "
            + classId);
      }
      classIdSet.clear(index);
    }
  }

  /**
   * Returns a formatted string representing the given classId including a
   * handle
   */
  public String getStringForNetClsClassId(int classId) {
    return String.format(FORMAT_NET_CLS_CLASS_ID, ROOT_QDISC_HANDLE, classId);
  }

  /**
   * A value read out of net_cls.classid file is in decimal form. We need to
   * convert to 32-bit/8 digit hex, extract the lower 16-bit/four digits
   * as an int
   */
  public int getClassIdFromFileContents(String input) {
    //convert from decimal back to fixed size hex form
    //e.g 4325381 -> 00420005
    String classIdStr = String.format("%08x", Integer.parseInt(input));

    if (LOG.isDebugEnabled()) {
      LOG.debug("ClassId hex string : " + classIdStr);
    }

    //extract and return 4 digits
    //e.g 00420005 -> 0005
    return Integer.parseInt(classIdStr.substring(4));
  }

  /**
   * Adds a tc class to qdisc at root
   */
  private String getStringForAddClassToRootQDisc(int rateMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    //example : "class add dev eth0 parent 42:0 classid 42:1 htb rate 1000mbit
    // ceil 1000mbit"
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, ZERO_CLASS_ID, ROOT_QDISC_HANDLE, ROOT_CLASS_ID,
        rateMbitStr, rateMbitStr);
  }

  private String getStringForAddDefaultClass(int rateMbit, int ceilMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    String ceilMbitStr = ceilMbit + MBIT_SUFFIX;
    //example : "class add dev eth0 parent 42:1 classid 42:2 htb rate 300mbit
    // ceil 1000mbit"
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, ROOT_CLASS_ID, ROOT_QDISC_HANDLE, DEFAULT_CLASS_ID,
        rateMbitStr, ceilMbitStr);
  }

  private String getStringForAddYARNRootClass(int rateMbit, int ceilMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    String ceilMbitStr = ceilMbit + MBIT_SUFFIX;
    //example : "class add dev eth0 parent 42:1 classid 42:3 htb rate 700mbit
    // ceil 1000mbit"
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, ROOT_CLASS_ID, ROOT_QDISC_HANDLE, YARN_ROOT_CLASS_ID,
        rateMbitStr, ceilMbitStr);
  }

  private String getStringForAddContainerClass(int classId, int rateMbit, int
      ceilMbit) {
    String rateMbitStr = rateMbit + MBIT_SUFFIX;
    String ceilMbitStr = ceilMbit + MBIT_SUFFIX;
    //example : "class add dev eth0 parent 42:99 classid 42:99 htb rate 50mbit
    // ceil 700mbit"
    return String.format(FORMAT_CLASS_ADD_TO_PARENT_WITH_RATES, device,
        ROOT_QDISC_HANDLE, YARN_ROOT_CLASS_ID, ROOT_QDISC_HANDLE, classId,
        rateMbitStr, ceilMbitStr);
  }

  private String getStringForDeleteContainerClass(int classId) {
    //example "class del dev eth0 classid 42:7"
    return String.format(FORMAT_DELETE_CLASS, device, ROOT_QDISC_HANDLE,
        classId);
  }

  private String getStringForReadState() {
    return String.format(FORMAT_READ_STATE, device);
  }

  private String getStringForReadClasses() {
    return String.format(FORMAT_READ_CLASSES, device);
  }

  private String getStringForWipeState() {
    return String.format(FORMAT_WIPE_STATE, device);
  }

  public class BatchBuilder {
    final PrivilegedOperation operation;
    final List<String> commands;

    public BatchBuilder(PrivilegedOperation.OperationType opType)
        throws ResourceHandlerException {
      switch (opType) {
      case TC_MODIFY_STATE:
      case TC_READ_STATE:
      case TC_READ_STATS:
        operation = new PrivilegedOperation(opType);
        commands = new ArrayList<>();
        break;
      default:
        throw new ResourceHandlerException("Not a tc operation type : " +
            opType);
      }
    }

    private BatchBuilder addRootQDisc() {
      commands.add(getStringForAddRootQDisc());
      return this;
    }

    private BatchBuilder addCGroupFilter() {
      commands.add(getStringForaAddCGroupFilter());
      return this;
    }

    private BatchBuilder addClassToRootQDisc(int rateMbit) {
      commands.add(getStringForAddClassToRootQDisc(rateMbit));
      return this;
    }

    private BatchBuilder addDefaultClass(int rateMbit, int ceilMbit) {
      commands.add(getStringForAddDefaultClass(rateMbit, ceilMbit));
      return this;
    }

    private BatchBuilder addYARNRootClass(int rateMbit, int ceilMbit) {
      commands.add(getStringForAddYARNRootClass(rateMbit, ceilMbit));
      return this;
    }

    public BatchBuilder addContainerClass(int classId, int rateMbit, boolean
        strictMode) {
      int ceilMbit;

      if (strictMode) {
        ceilMbit = rateMbit;
      } else {
        ceilMbit = yarnBandwidthMbit;
      }

      commands.add(getStringForAddContainerClass(classId, rateMbit, ceilMbit));
      return this;
    }

    public BatchBuilder deleteContainerClass(int classId) {
      commands.add(getStringForDeleteContainerClass(classId));
      return this;
    }

    private BatchBuilder readState() {
      commands.add(getStringForReadState());
      return this;
    }

    //We'll read all classes, but use a different tc operation type
    //when reading stats for all these classes. Stats are fetched using a
    //different tc cli option (-s).

    private BatchBuilder readClasses() {
      //We'll read all classes, but use a different tc operation type
      //for reading stats for all these classes. Stats are fetched using a
      //different tc cli option (-s).
      commands.add(getStringForReadClasses());
      return this;
    }

    private BatchBuilder wipeState() {
      commands.add(getStringForWipeState());
      return this;
    }

    public PrivilegedOperation commitBatchToTempFile()
        throws ResourceHandlerException {
      try {
        File tcCmds = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_SUFFIX, new
            File(tmpDirPath));

        try (
            Writer writer = new OutputStreamWriter(new FileOutputStream(tcCmds),
                "UTF-8");
            PrintWriter printWriter = new PrintWriter(writer)) {
          for (String command : commands) {
            printWriter.println(command);
          }
        }

        operation.appendArgs(tcCmds.getAbsolutePath());

        return operation;
      } catch (IOException e) {
        LOG.warn("Failed to create or write to temporary file in dir: " +
            tmpDirPath);
        throw new ResourceHandlerException(
            "Failed to create or write to temporary file in dir: "
                + tmpDirPath);
      }
    }
  } //end BatchBuilder
}
