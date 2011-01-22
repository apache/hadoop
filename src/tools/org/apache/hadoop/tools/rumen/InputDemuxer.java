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
package org.apache.hadoop.tools.rumen;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * {@link InputDemuxer} dem-ultiplexes the input files into individual input
 * streams.
 */
public interface InputDemuxer extends Closeable {
  /**
   * Bind the {@link InputDemuxer} to a particular file.
   * 
   * @param path
   *          The path to the find it should bind to.
   * @param conf
   *          Configuration
   * @throws IOException
   * 
   *           Returns true when the binding succeeds. If the file can be read
   *           but is in the wrong format, returns false. IOException is
   *           reserved for read errors.
   */
  public void bindTo(Path path, Configuration conf) throws IOException;

  /**
   * Get the next <name, input> pair. The name should preserve the original job
   * history file or job conf file name. The input object should be closed
   * before calling getNext() again. The old input object would be invalid after
   * calling getNext() again.
   * 
   * @return the next <name, input> pair.
   */
  public Pair<String, InputStream> getNext() throws IOException;
}
