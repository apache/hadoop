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

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

/**
 * StatisticsEditsVisitor implements text version of EditsVisitor
 * that aggregates counts of op codes processed
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StatisticsEditsVisitor extends EditsVisitor {
  private boolean printToScreen = false;
  private boolean okToWrite = false;
  final private FileWriter fw;

  public final Map<FSEditLogOpCodes, Long> opCodeCount =
    new HashMap<FSEditLogOpCodes, Long>();

  /**
   * Create a processor that writes to the file named.
   *
   * @param filename Name of file to write output to
   */
  public StatisticsEditsVisitor(String filename, Tokenizer tokenizer)
    throws IOException {

    this(filename, tokenizer, false);
  }

  /**
   * Create a processor that writes to the file named and may or may not
   * also output to the screen, as specified.
   *
   * @param filename Name of file to write output to
   * @param tokenizer Input tokenizer
   * @param printToScreen Mirror output to screen?
   */
  public StatisticsEditsVisitor(String filename,
    Tokenizer tokenizer,
    boolean printToScreen) throws IOException {

    super(tokenizer);
    this.printToScreen = printToScreen;
    fw = new FileWriter(filename);
    okToWrite = true;
  }

  /**
   * Start the visitor (initialization)
   */
  @Override
  void start() throws IOException {
    // nothing to do
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineEditsViewer.EditsVisitor#finish()
   */
  @Override
  void finish() throws IOException {
    write(getStatisticsString());
    close();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.tools.offlineEditsViewer.EditsVisitor#finishAbnormally()
   */
  @Override
  void finishAbnormally() throws IOException {
    close();
  }

  /**
   * Close output stream and prevent further writing
   */
  private void close() throws IOException {
    fw.close();
    okToWrite = false;
  }

  /**
   * Visit a enclosing element (element that has other elements in it)
   */
  @Override
  void visitEnclosingElement(Tokenizer.Token value) throws IOException {
    // nothing to do
  }

  /**
   * End of eclosing element
   */
  @Override
  void leaveEnclosingElement() throws IOException {
    // nothing to do
  }  

  /**
   * Visit a Token, calculate statistics
   *
   * @param value a Token to visit
   */
  @Override
  Tokenizer.Token visit(Tokenizer.Token value) throws IOException {
    // count the opCodes
    if(value.getEditsElement() == EditsElement.OPCODE) {
      if(value instanceof Tokenizer.ByteToken) {
        incrementOpCodeCount(
          FSEditLogOpCodes.fromByte(((Tokenizer.ByteToken)value).value));
      } else {
        throw new IOException("Token for EditsElement.OPCODE should be " +
          "of type Tokenizer.ByteToken, not " + value.getClass());
      }
    }
    return value;
  }

  /**
   * Write parameter to output file (and possibly screen).
   *
   * @param toWrite Text to write to file
   */
  protected void write(String toWrite) throws IOException  {
    if(!okToWrite)
      throw new IOException("file not open for writing.");

    if(printToScreen)
      System.out.print(toWrite);

    try {
      fw.write(toWrite);
    } catch (IOException e) {
      okToWrite = false;
      throw e;
    }
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
    StringBuffer sb = new StringBuffer();
    for(FSEditLogOpCodes opCode : FSEditLogOpCodes.values()) {
      sb.append(String.format(
        "    %-30.30s (%3d): %d%n",
        opCode,
        opCode.getOpCode(),
        opCodeCount.get(opCode)));
    }
    return sb.toString();
  }
}
