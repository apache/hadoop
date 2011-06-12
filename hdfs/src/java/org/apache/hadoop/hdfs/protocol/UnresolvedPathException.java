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

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.Path;

/** 
 * Thrown when a symbolic link is encountered in a path.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class UnresolvedPathException extends UnresolvedLinkException {
  private static final long serialVersionUID = 1L;
  private String originalPath;  // The original path containing the link
  private String linkTarget;    // The target of the link 
  private String remainingPath; // The path part following the link
  

  /**
   * Used by RemoteException to instantiate an UnresolvedPathException.
   */
  public UnresolvedPathException(String msg) {
    super(msg);
  }
  
  public UnresolvedPathException(String originalPath, String remainingPath, 
      String linkTarget) {
    this.originalPath  = originalPath;
    this.remainingPath = remainingPath;
    this.linkTarget    = linkTarget;
  }

  public Path getUnresolvedPath() throws IOException {
    return new Path(originalPath);
  }
  
  public Path getResolvedPath() throws IOException {
    if (remainingPath == null || "".equals(remainingPath)) {
      return new Path(linkTarget);
    }
    return new Path(linkTarget, remainingPath);
  }

  @Override
  public String getMessage() {
    String msg = super.getMessage();
    if (msg != null) {
      return msg;
    }
    String myMsg = "Unresolved path " + originalPath;
    try {
      return getResolvedPath().toString();
    } catch (IOException e) {
      // Ignore
    }
    return myMsg;
  }
}
