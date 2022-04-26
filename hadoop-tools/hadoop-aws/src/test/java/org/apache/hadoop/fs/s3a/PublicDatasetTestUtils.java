/*
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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.S3ATestConstants.KEY_BUCKET_WITH_MANY_OBJECTS;

/**
 * Provides S3A filesystem URIs for public data sets for specific use cases.
 *
 * This allows for the contract between S3A tests and the existence of data sets
 * to be explicit and also standardizes access and configuration of
 * replacements.
 *
 * Bucket specific configuration such as endpoint or requester pays should be
 * configured within "hadoop-tools/hadoop-aws/src/test/resources/core-site.xml".
 *
 * Warning: methods may mutate the configuration instance passed in.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PublicDatasetTestUtils {

  /**
   * Default bucket for an S3A file system with many objects: {@value}.
   */
  private static final String DEFAULT_BUCKET_WITH_MANY_OBJECTS
      = "s3a://common-crawl/";

  /**
   * Provide a URI for a directory containing many objects.
   *
   * Unless otherwise configured,
   * this will be {@value DEFAULT_BUCKET_WITH_MANY_OBJECTS}.
   *
   * @param conf Hadoop configuration
   * @return S3A FS URI
   */
  public static String getBucketPrefixWithManyObjects(Configuration conf) {
    return fetchFromConfig(conf,
        KEY_BUCKET_WITH_MANY_OBJECTS, DEFAULT_BUCKET_WITH_MANY_OBJECTS);
  }

  private static String fetchFromConfig(Configuration conf, String key, String defaultValue) {
    String value = conf.getTrimmed(key, defaultValue);

    S3ATestUtils.assume("Empty test property: " + key, !value.isEmpty());

    return value;
  }

}
