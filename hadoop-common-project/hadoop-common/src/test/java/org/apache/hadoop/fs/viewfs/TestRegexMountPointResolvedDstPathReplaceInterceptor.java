/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.fs.viewfs.RegexMountPointInterceptorType.REPLACE_RESOLVED_DST_PATH;

/**
 * Test RegexMountPointResolvedDstPathReplaceInterceptor.
 */
public class TestRegexMountPointResolvedDstPathReplaceInterceptor {

  public String createSerializedString(String regex, String replaceString) {
    return REPLACE_RESOLVED_DST_PATH.getConfigName()
        + RegexMountPoint.INTERCEPTOR_INTERNAL_SEP + regex
        + RegexMountPoint.INTERCEPTOR_INTERNAL_SEP + replaceString;
  }

  @Test
  public void testDeserializeFromStringNormalCase() throws IOException {
    String srcRegex = "-";
    String replaceString = "_";
    String serializedString = createSerializedString(srcRegex, replaceString);
    RegexMountPointResolvedDstPathReplaceInterceptor interceptor =
        RegexMountPointResolvedDstPathReplaceInterceptor
            .deserializeFromString(serializedString);
    Assert.assertEquals(srcRegex, interceptor.getSrcRegexString());
    Assert.assertEquals(replaceString, interceptor.getReplaceString());
    Assert.assertNull(interceptor.getSrcRegexPattern());
    interceptor.initialize();
    Assert.assertEquals(srcRegex,
        interceptor.getSrcRegexPattern().toString());
  }

  @Test
  public void testDeserializeFromStringBadCase() throws IOException {
    String srcRegex = "-";
    String replaceString = "_";
    String serializedString = createSerializedString(srcRegex, replaceString);
    serializedString = serializedString + ":ddd";
    RegexMountPointResolvedDstPathReplaceInterceptor interceptor =
        RegexMountPointResolvedDstPathReplaceInterceptor
            .deserializeFromString(serializedString);
    Assert.assertNull(interceptor);
  }

  @Test
  public void testSerialization() {
    String srcRegex = "word1";
    String replaceString = "word2";
    String serializedString = createSerializedString(srcRegex, replaceString);
    RegexMountPointResolvedDstPathReplaceInterceptor interceptor =
        new RegexMountPointResolvedDstPathReplaceInterceptor(srcRegex,
            replaceString);
    Assert.assertEquals(interceptor.serializeToString(), serializedString);
  }

  @Test
  public void testInterceptSource() {
    String srcRegex = "word1";
    String replaceString = "word2";
    RegexMountPointResolvedDstPathReplaceInterceptor interceptor =
        new RegexMountPointResolvedDstPathReplaceInterceptor(srcRegex,
            replaceString);
    String sourcePath = "/a/b/l3/dd";
    Assert.assertEquals(sourcePath, interceptor.interceptSource(sourcePath));
  }

  @Test
  public void testInterceptResolve() throws IOException {
    String pathAfterResolution = "/user-hadoop";

    String srcRegex = "hadoop";
    String replaceString = "hdfs";
    RegexMountPointResolvedDstPathReplaceInterceptor interceptor =
        new RegexMountPointResolvedDstPathReplaceInterceptor(srcRegex,
            replaceString);
    interceptor.initialize();
    Assert.assertEquals("/user-hdfs",
        interceptor.interceptResolvedDestPathStr(pathAfterResolution));
  }
}
