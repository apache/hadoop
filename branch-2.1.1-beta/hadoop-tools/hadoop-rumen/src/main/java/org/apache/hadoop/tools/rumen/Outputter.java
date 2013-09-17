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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Interface to output a sequence of objects of type T.
 */
public interface Outputter<T> extends Closeable {
  /**
   * Initialize the {@link Outputter} to a specific path.
   * @param path The {@link Path} to the output file.
   * @param conf Configuration
   * @throws IOException
   */
  public void init(Path path, Configuration conf) throws IOException;
  
  /**
   * Output an object.
   * @param object The objecte.
   * @throws IOException
   */
  public void output(T object) throws IOException;

}