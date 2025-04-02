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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.apache.hadoop.util.Lists;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class TestUtility {
  private static final String ENV_TOS_BUCKET = "TOS_BUCKET";
  private static final String ENV_TEST_SCHEME = "TEST_SCHEME";
  private static final Random RND = new Random(System.currentTimeMillis());

  private TestUtility() {
  }

  public static byte[] rand(int size) {
    byte[] buffer = new byte[size];
    RND.nextBytes(buffer);
    return buffer;
  }

  public static int randInt(int bound) {
    return RND.nextInt(bound);
  }

  public static String randomWithChinese() {
    return RandomStringUtils.random(10, 0x4e00, 0x9fa5, false, false);
  }

  public static String createUniquePath(String scheme) {
    String bucket = bucket();
    if (bucket != null) {
      return String.format("%s://%s/%s-%s/", scheme, bucket, scheme, UUIDUtils.random());
    } else {
      throw new IllegalStateException("OS test bucket is not available");
    }
  }

  public static String defaultFs() {
    return String.format("%s://%s/", scheme(), bucket());
  }

  public static String scheme() {
    return ParseUtils.envAsString(ENV_TEST_SCHEME, "tos");
  }

  public static String bucket() {
    String bucket = ParseUtils.envAsString(ENV_TOS_BUCKET);
    if (bucket != null) {
      return bucket;
    }

    // Parse from endpoint if it is formatted like http[s]://<bucket>.<region>.xxx.com
    String endpoint = ParseUtils.envAsString(TOS.ENV_TOS_ENDPOINT);
    if (endpoint != null) {
      for (String scheme : Lists.newArrayList("http://", "https://")) {
        if (endpoint.startsWith(scheme)) {
          endpoint = endpoint.substring(scheme.length());
        }
      }

      String[] elements = endpoint.split("\\.");
      if (elements.length == 4) {
        return elements[0];
      }
    }
    throw new RuntimeException(
        "Cannot decide the bucket name for object storage with scheme 'tos'");
  }

  public static String region() {
    return TOSClientContextUtils.parseRegion(ParseUtils.envAsString(TOS.ENV_TOS_ENDPOINT));
  }

  public static String endpoint() {
    return ParseUtils.envAsString(TOS.ENV_TOS_ENDPOINT);
  }

  @SuppressWarnings("unchecked")
  public static void setSystemEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.put(key, value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }

  @SuppressWarnings("unchecked")
  public static void removeSystemEnv(String key) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.remove(key);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Failed to remove environment variable: %s", key), e);
    }
  }

  public static FileContext createTestFileContext(Configuration conf) throws IOException {
    URI testURI = URI.create(defaultFs());
    return FileContext.getFileContext(testURI, conf);
  }

  private static ObjectStorage generalBucketObjectStorage() {
    Configuration conf = new Configuration();

    String endpoint = ParseUtils.envAsString(TOS.ENV_TOS_ENDPOINT, "");
    if (!StringUtils.isEmpty(endpoint)) {
      conf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(scheme()), endpoint);
    }

    return ObjectStorageFactory.createWithPrefix(
        String.format("%s-%s/", scheme(), UUIDUtils.random()), scheme(), bucket(), conf);
  }

  public static List<ObjectStorage> createTestObjectStorage(String fileStoreRoot) {
    List<ObjectStorage> storages = new ArrayList<>();

    // 1. FileStore
    Configuration fileStoreConf = new Configuration();
    fileStoreConf.set(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key("filestore"), fileStoreRoot);
    storages.add(ObjectStorageFactory.create("filestore", TestUtility.bucket(), fileStoreConf));

    // 2. General Bucket
    storages.add(generalBucketObjectStorage());

    return storages;
  }
}
