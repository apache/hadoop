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

import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * BinaryEditsVisitor implements a binary EditsVisitor
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BinaryEditsVisitor extends EditsVisitor {
  final private DataOutputStream out;

  /**
   * Create a processor that writes to a given file and
   * reads using a given Tokenizer
   *
   * @param filename Name of file to write output to
   * @param tokenizer Input tokenizer
   */
  public BinaryEditsVisitor(String filename, Tokenizer tokenizer)
    throws IOException {

    this(filename, tokenizer, false);
  }

  /**
   * Create a processor that writes to a given file and reads using
   * a given Tokenizer, may also print to screen
   *
   * @param filename Name of file to write output to
   * @param tokenizer Input tokenizer
   * @param printToScreen Mirror output to screen? (ignored for binary)
   */
  public BinaryEditsVisitor(String filename,
    Tokenizer tokenizer,
    boolean printToScreen) throws IOException {

    super(tokenizer);
    out = new DataOutputStream(new FileOutputStream(filename));
  }

  /**
   * Start the visitor (initialization)
   */
  @Override
  void start() throws IOException {
    // nothing to do for binary format
  }

  /**
   * Finish the visitor
   */
  @Override
  void finish() throws IOException {
    close();
  }

  /**
   * Finish the visitor and indicate an error
   */
  @Override
  void finishAbnormally() throws IOException {
    System.err.println("Error processing EditLog file.  Exiting.");
    close();
  }

  /**
   * Close output stream and prevent further writing
   */
  private void close() throws IOException {
    out.close();
  }

  /**
   * Visit a enclosing element (element that has other elements in it)
   */
  @Override
  void visitEnclosingElement(Tokenizer.Token value) throws IOException {
    // nothing to do for binary format
  }

  /**
   * End of eclosing element
   */
  @Override
  void leaveEnclosingElement() throws IOException {
    // nothing to do for binary format
  }  

  /**
   * Visit a Token
   */
  @Override
  Tokenizer.Token visit(Tokenizer.Token value) throws IOException {
    value.toBinary(out);
    return value;
  }
}
