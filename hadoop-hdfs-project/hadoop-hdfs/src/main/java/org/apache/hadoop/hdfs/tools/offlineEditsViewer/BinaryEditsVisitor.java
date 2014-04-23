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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;

/**
 * BinaryEditsVisitor implements a binary EditsVisitor
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BinaryEditsVisitor implements OfflineEditsVisitor {
  final private EditLogFileOutputStream elfos;

  /**
   * Create a processor that writes to a given file
   * @param outputName Name of file to write output to
   */
  public BinaryEditsVisitor(String outputName) throws IOException {
    this.elfos = new EditLogFileOutputStream(new Configuration(),
      new File(outputName), 0);
    elfos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

  /**
   * Start the visitor (initialization)
   */
  @Override
  public void start(int version) throws IOException {
  }

  /**
   * Finish the visitor
   */
  @Override
  public void close(Throwable error) throws IOException {
    elfos.setReadyToFlush();
    elfos.flushAndSync(true);
    elfos.close();
  }

  @Override
  public void visitOp(FSEditLogOp op) throws IOException {
    elfos.write(op);
  }
}
