/**
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;

public class DirectMemoryUtils {
  /**
   * @return the setting of -XX:MaxDirectMemorySize as a long. Returns 0 if
   *         -XX:MaxDirectMemorySize is not set.
   */

  public static long getDirectMemorySize() {
    RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
    List<String> arguments = RuntimemxBean.getInputArguments();
    long multiplier = 1; //for the byte case.
    for (String s : arguments) {
      if (s.contains("-XX:MaxDirectMemorySize=")) {
        String memSize = s.toLowerCase()
            .replace("-xx:maxdirectmemorysize=", "").trim();

        if (memSize.contains("k")) {
          multiplier = 1024;
        }

        else if (memSize.contains("m")) {
          multiplier = 1048576;
        }

        else if (memSize.contains("g")) {
          multiplier = 1073741824;
        }
        memSize = memSize.replaceAll("[^\\d]", "");

        long retValue = Long.parseLong(memSize);
        return retValue * multiplier;
      }

    }
    return 0;
  }

  /**
   * DirectByteBuffers are garbage collected by using a phantom reference and a
   * reference queue. Every once a while, the JVM checks the reference queue and
   * cleans the DirectByteBuffers. However, as this doesn't happen
   * immediately after discarding all references to a DirectByteBuffer, it's
   * easy to OutOfMemoryError yourself using DirectByteBuffers. This function
   * explicitly calls the Cleaner method of a DirectByteBuffer.
   * 
   * @param toBeDestroyed
   *          The DirectByteBuffer that will be "cleaned". Utilizes reflection.
   *          
   */
  public static void destroyDirectByteBuffer(ByteBuffer toBeDestroyed)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException, SecurityException, NoSuchMethodException {

    Preconditions.checkArgument(toBeDestroyed.isDirect(),
        "toBeDestroyed isn't direct!");

    Method cleanerMethod = toBeDestroyed.getClass().getMethod("cleaner");
    cleanerMethod.setAccessible(true);
    Object cleaner = cleanerMethod.invoke(toBeDestroyed);
    Method cleanMethod = cleaner.getClass().getMethod("clean");
    cleanMethod.setAccessible(true);
    cleanMethod.invoke(cleaner);

  }
}
