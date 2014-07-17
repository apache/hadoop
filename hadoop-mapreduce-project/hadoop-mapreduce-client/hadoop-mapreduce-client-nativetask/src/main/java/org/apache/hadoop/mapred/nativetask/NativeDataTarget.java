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

import org.apache.hadoop.mapred.nativetask.buffer.OutputBuffer;

/**
 * NativeDataTarge sends data to downstream
 */
public interface NativeDataTarget {

  /**
   * send a signal to indicate that the data has been stored in output buffer
   * 
   * @throws IOException
   */
  public void sendData() throws IOException;

  /**
   * Send a signal that there is no more data
   * 
   * @throws IOException
   */
  public void finishSendData() throws IOException;

  /**
   * get the output buffer.
   * 
   * @return
   */
  public OutputBuffer getOutputBuffer();

}
