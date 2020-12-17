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
package org.apache.hadoop.util;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The factory class to create instance of {@link DiskValidator}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DiskValidatorFactory {
  @VisibleForTesting
  static final ConcurrentHashMap<Class<? extends DiskValidator>, DiskValidator>
      INSTANCES = new ConcurrentHashMap<>();

  private DiskValidatorFactory() {
  }

  /**
   * Returns a {@link DiskValidator} instance corresponding to the passed clazz.
   * @param clazz a class extends {@link DiskValidator}
   */
  public static DiskValidator
      getInstance(Class<? extends DiskValidator> clazz) {
    DiskValidator diskValidator;
    if (INSTANCES.containsKey(clazz)) {
      diskValidator = INSTANCES.get(clazz);
    } else {
      diskValidator = ReflectionUtils.newInstance(clazz, null);
      // check the return of putIfAbsent() to see if any other thread have put
      // the instance with the same key into INSTANCES
      DiskValidator diskValidatorRet =
          INSTANCES.putIfAbsent(clazz, diskValidator);
      if (diskValidatorRet != null) {
        diskValidator = diskValidatorRet;
      }
    }

    return diskValidator;
  }

  /**
   * Returns {@link DiskValidator} instance corresponding to its name.
   * The diskValidator parameter can be "basic" for {@link BasicDiskValidator}
   * or "read-write" for {@link ReadWriteDiskValidator}.
   * @param diskValidator canonical class name, for example, "basic"
   * @throws DiskErrorException if the class cannot be located
   */
  @SuppressWarnings("unchecked")
  public static DiskValidator getInstance(String diskValidator)
      throws DiskErrorException {
    @SuppressWarnings("rawtypes")
    Class clazz;

    if (diskValidator.equalsIgnoreCase(BasicDiskValidator.NAME)) {
      clazz = BasicDiskValidator.class;
    } else if (diskValidator.equalsIgnoreCase(ReadWriteDiskValidator.NAME)) {
      clazz = ReadWriteDiskValidator.class;
    } else {
      try {
        clazz = Class.forName(diskValidator);
      } catch (ClassNotFoundException cnfe) {
        throw new DiskErrorException(diskValidator
            + " DiskValidator class not found.", cnfe);
      }
    }

    return getInstance(clazz);
  }
}
