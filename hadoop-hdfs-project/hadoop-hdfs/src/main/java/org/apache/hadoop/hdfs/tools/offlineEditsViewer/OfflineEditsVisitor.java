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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

/**
 * An implementation of OfflineEditsVisitor can traverse the structure of an
 * Hadoop edits log and respond to each of the structures within the file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
abstract public interface OfflineEditsVisitor {
  /**
   * Begin visiting the edits log structure.  Opportunity to perform
   * any initialization necessary for the implementing visitor.
   * 
   * @param version     Edit log version
   */
  abstract void start(int version) throws IOException;

  /**
   * Finish visiting the edits log structure.  Opportunity to perform any
   * clean up necessary for the implementing visitor.
   * 
   * @param error        If the visitor was closed because of an 
   *                     unrecoverable error in the input stream, this 
   *                     is the exception.
   */
  abstract void close(Throwable error) throws IOException;

  /**
   * Begin visiting an element that encloses another element, such as
   * the beginning of the list of blocks that comprise a file.
   *
   * @param value Token being visited
   */
  abstract void visitOp(FSEditLogOp op)
     throws IOException;
}
