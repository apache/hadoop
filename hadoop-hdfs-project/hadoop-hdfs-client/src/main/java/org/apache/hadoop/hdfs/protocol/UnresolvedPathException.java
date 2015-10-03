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
  private String path;        // The path containing the link
  private String preceding;   // The path part preceding the link
  private String remainder;   // The path part following the link
  private String linkTarget;  // The link's target

  /**
   * Used by RemoteException to instantiate an UnresolvedPathException.
   */
  public UnresolvedPathException(String msg) {
    super(msg);
  }

  public UnresolvedPathException(String path, String preceding,
      String remainder, String linkTarget) {
    this.path = path;
    this.preceding = preceding;
    this.remainder = remainder;
    this.linkTarget = linkTarget;
  }

  /**
   * Return a path with the link resolved with the target.
   */
  public Path getResolvedPath() {
    // If the path is absolute we cam throw out the preceding part and
    // just append the remainder to the target, otherwise append each
    // piece to resolve the link in path.
    boolean noRemainder = (remainder == null || "".equals(remainder));
    Path target = new Path(linkTarget);
    if (target.isUriPathAbsolute()) {
      return noRemainder ? target : new Path(target, remainder);
    } else {
      return noRemainder
        ? new Path(preceding, target)
        : new Path(new Path(preceding, linkTarget), remainder);
    }
  }

  @Override
  public String getMessage() {
    String msg = super.getMessage();
    if (msg != null) {
      return msg;
    }
    return getResolvedPath().toString();
  }
}
