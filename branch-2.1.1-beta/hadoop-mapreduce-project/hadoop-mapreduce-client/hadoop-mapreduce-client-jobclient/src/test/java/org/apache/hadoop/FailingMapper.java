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

package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Fails the Mapper. First attempt throws exception. Rest do System.exit.
 *
 */
public class FailingMapper extends Mapper<Text, Text, Text, Text> {
  public void map(Text key, Text value,
      Context context) throws IOException,InterruptedException {

    // Just create a non-daemon thread which hangs forever. MR AM should not be
    // hung by this.
    new Thread() {
      @Override
      public void run() {
        synchronized (this) {
          try {
            wait();
          } catch (InterruptedException e) {
            //
          }
        }
      }
    }.start();

    if (context.getTaskAttemptID().getId() == 0) {
      System.out.println("Attempt:" + context.getTaskAttemptID() + 
        " Failing mapper throwing exception");
      throw new IOException("Attempt:" + context.getTaskAttemptID() + 
          " Failing mapper throwing exception");
    } else {
      System.out.println("Attempt:" + context.getTaskAttemptID() + 
      " Exiting");
      System.exit(-1);
    }
  }
}
