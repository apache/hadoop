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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

/**
 * A Handler accept input, and give output can be used to transfer command and data
 */
@InterfaceAudience.Private
public interface INativeHandler extends NativeDataTarget, NativeDataSource {

  public String name();

  public long getNativeHandler();

  /**
   * init the native handler
   */
  public void init(Configuration conf) throws IOException;

  /**
   * close the native handler
   */
  public void close() throws IOException;

  /**
   * call command to downstream
   */
  public ReadWriteBuffer call(Command command, ReadWriteBuffer parameter) throws IOException;

  void setCommandDispatcher(CommandDispatcher handler);

}
