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

package org.apache.hadoop.hdfs.server.datanode.extdataset;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RollingLogs;

public class ExternalRollingLogs implements RollingLogs {

  private class ExternalLineIterator implements LineIterator {
    @Override
    public boolean isPrevious() {
      return false;
    }

    @Override
    public boolean isLastReadFromPrevious() {
      return false;
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public String next() {
      return null;
    }

    @Override
    public void remove() {
    }

    @Override
    public void close() throws IOException {
    }
  }

  private class ExternalAppender implements Appender {
    @Override
    public Appendable append(CharSequence cs) throws IOException {
      return null;
    }

    @Override
    public Appendable append(CharSequence cs, int i, int i1)
	throws IOException {
      return null;
    }

    @Override
    public Appendable append(char c) throws IOException {
      return null;
    }

    @Override
    public void close() throws IOException {
    }
  }

  @Override
  public LineIterator iterator(boolean skipPrevious) throws IOException {
    return new ExternalLineIterator();
  }

  @Override
  public Appender appender() {
    return new ExternalAppender();
  }

  @Override
  public boolean roll() throws IOException {
    return false;
  }
}
