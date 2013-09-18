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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Rolling logs consist of a current log and a set of previous logs.
 *
 * The implementation should support a single appender and multiple readers.
 */
public interface RollingLogs {
  /**
   * To iterate the lines of the logs.
   */
  public interface LineIterator extends Iterator<String>, Closeable {
    /** Is the iterator iterating the previous? */
    public boolean isPrevious();

    /**
     * Is the last read entry from previous? This should be called after
     * reading.
     */
    public boolean isLastReadFromPrevious();
  }

  /**
   * To append text to the logs.
   */
  public interface Appender extends Appendable, Closeable {
  }

  /**
   * Create an iterator to iterate the lines in the logs.
   * 
   * @param skipPrevious Should it skip reading the previous log? 
   * @return a new iterator.
   */
  public LineIterator iterator(boolean skipPrevious) throws IOException;

  /**
   * @return the only appender to append text to the logs.
   *   The same object is returned if it is invoked multiple times.
   */
  public Appender appender();

  /**
   * Roll current to previous.
   *
   * @return true if the rolling succeeded.
   *   When it returns false, it is not equivalent to an error. 
   *   It means that the rolling cannot be performed at the moment,
   *   e.g. the logs are being read.
   */
  public boolean roll() throws IOException;
}