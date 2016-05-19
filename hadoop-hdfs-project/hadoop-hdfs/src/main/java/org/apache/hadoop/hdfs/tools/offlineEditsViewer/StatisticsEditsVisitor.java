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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import com.google.common.base.Charsets;

/**
 * StatisticsEditsVisitor implements text version of EditsVisitor
 * that aggregates counts of op codes processed
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StatisticsEditsVisitor implements OfflineEditsVisitor {
  final private PrintWriter out;

  private int version = -1;
  private final Map<FSEditLogOpCodes, Long> opCodeCount =
    new HashMap<FSEditLogOpCodes, Long>();

  /**
   * Create a processor that writes to the file named and may or may not
   * also output to the screen, as specified.
   *
   * @param filename Name of file to write output to
   * @param tokenizer Input tokenizer
   * @param printToScreen Mirror output to screen?
   */
  public StatisticsEditsVisitor(OutputStream out) throws IOException {
    this.out = new PrintWriter(new OutputStreamWriter(out, Charsets.UTF_8));
  }

  /** Start the visitor */
  @Override
  public void start(int version) throws IOException {
    this.version = version;
  }
  
  /** Close the visitor */
  @Override
  public void close(Throwable error) throws IOException {
    out.print(getStatisticsString());
    if (error != null) {
      out.print("EXITING ON ERROR: " + error.toString() + "\n");
    }
    out.close();
  }

  @Override
  public void visitOp(FSEditLogOp op) throws IOException {
    incrementOpCodeCount(op.opCode);
  }

  /**
   * Increment the op code counter
   *
   * @param opCode opCode for which to increment count
   */
  private void incrementOpCodeCount(FSEditLogOpCodes opCode) {
    if(!opCodeCount.containsKey(opCode)) {
      opCodeCount.put(opCode, 0L);
    }
    Long newValue = opCodeCount.get(opCode) + 1;
    opCodeCount.put(opCode, newValue);
  }

  /**
   * Get statistics
   *
   * @return statistics, map of counts per opCode
   */
  public Map<FSEditLogOpCodes, Long> getStatistics() {
    return opCodeCount;
  }

  /**
   * Get the statistics in string format, suitable for printing
   *
   * @return statistics in in string format, suitable for printing
   */
  public String getStatisticsString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(
        "    %-30.30s      : %d%n",
        "VERSION", version));
    for(FSEditLogOpCodes opCode : FSEditLogOpCodes.values()) {
      Long count = opCodeCount.get(opCode);
      sb.append(String.format(
          "    %-30.30s (%3d): %d%n",
          opCode.toString(),
          opCode.getOpCode(),
          count == null ? Long.valueOf(0L) : count));
    }
    return sb.toString();
  }
}