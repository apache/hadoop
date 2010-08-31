/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Exception thrown by HTable methods when an attempt to do something (like
 * commit changes) fails after a bunch of retries.
 */
public class RetriesExhaustedException extends IOException {
  private static final long serialVersionUID = 1876775844L;

  public RetriesExhaustedException(final String msg) {
    super(msg);
  }

  public RetriesExhaustedException(final String msg, final IOException e) {
    super(msg, e);
  }

  /**
   * Create a new RetriesExhaustedException from the list of prior failures.
   * @param serverName name of HRegionServer
   * @param regionName name of region
   * @param row The row we were pursuing when we ran out of retries
   * @param numTries The number of tries we made
   * @param exceptions List of exceptions that failed before giving up
   */
  public RetriesExhaustedException(String serverName, final byte [] regionName,
      final byte []  row, int numTries, List<Throwable> exceptions) {
    super(getMessage(serverName, regionName, row, numTries, exceptions));
  }

  private static String getMessage(String serverName, final byte [] regionName,
      final byte [] row,
      int numTries, List<Throwable> exceptions) {
    StringBuilder buffer = new StringBuilder("Trying to contact region server ");
    buffer.append(serverName);
    buffer.append(" for region ");
    buffer.append(regionName == null? "": Bytes.toStringBinary(regionName));
    buffer.append(", row '");
    buffer.append(row == null? "": Bytes.toStringBinary(row));
    buffer.append("', but failed after ");
    buffer.append(numTries + 1);
    buffer.append(" attempts.\nExceptions:\n");
    for (Throwable t : exceptions) {
      buffer.append(t.toString());
      buffer.append("\n");
    }
    return buffer.toString();
  }
}