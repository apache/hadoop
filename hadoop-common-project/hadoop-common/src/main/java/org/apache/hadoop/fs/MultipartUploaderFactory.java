/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * {@link ServiceLoader}-driven uploader API for storage services supporting
 * multipart uploads.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MultipartUploaderFactory {
  public static final Logger LOG =
      LoggerFactory.getLogger(MultipartUploaderFactory.class);

  /**
   * Multipart Uploaders listed as services.
   */
  private static ServiceLoader<MultipartUploaderFactory> serviceLoader =
      ServiceLoader.load(MultipartUploaderFactory.class,
          MultipartUploaderFactory.class.getClassLoader());

  // Iterate through the serviceLoader to avoid lazy loading.
  // Lazy loading would require synchronization in concurrent use cases.
  static {
    Iterator<MultipartUploaderFactory> iterServices = serviceLoader.iterator();
    while (iterServices.hasNext()) {
      iterServices.next();
    }
  }

  /**
   * Get the multipart loader for a specific filesystem.
   * @param fs filesystem
   * @param conf configuration
   * @return an uploader, or null if one was found.
   * @throws IOException failure during the creation process.
   */
  public static MultipartUploader get(FileSystem fs, Configuration conf)
      throws IOException {
    MultipartUploader mpu = null;
    for (MultipartUploaderFactory factory : serviceLoader) {
      mpu = factory.createMultipartUploader(fs, conf);
      if (mpu != null) {
        break;
      }
    }
    return mpu;
  }

  protected abstract MultipartUploader createMultipartUploader(FileSystem fs,
      Configuration conf) throws IOException;
}
