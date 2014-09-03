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

package org.apache.hadoop.mapred.nativetask.util;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

@InterfaceAudience.Private
public class OutputUtil {

  private static Log LOG = LogFactory.getLog(OutputUtil.class);
  public static final String NATIVE_TASK_OUTPUT_MANAGER = "nativetask.output.manager";

  public static NativeTaskOutput createNativeTaskOutput(Configuration conf, String id) {
    Class<?> clazz = conf.getClass(OutputUtil.NATIVE_TASK_OUTPUT_MANAGER,
        NativeTaskOutputFiles.class);
    LOG.info(OutputUtil.NATIVE_TASK_OUTPUT_MANAGER + " = " + clazz.getName());
    try {
      Constructor<?> ctor = clazz.getConstructor(Configuration.class, String.class);
      ctor.setAccessible(true);
      NativeTaskOutput instance = (NativeTaskOutput) ctor.newInstance(conf, id);
      return instance;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
