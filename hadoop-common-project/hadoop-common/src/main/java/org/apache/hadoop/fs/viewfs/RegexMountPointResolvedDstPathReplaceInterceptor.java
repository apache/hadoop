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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.viewfs.RegexMountPointInterceptorType.REPLACE_RESOLVED_DST_PATH;

/**
 * Implementation of RegexMountPointResolvedDstPathReplaceInterceptor.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RegexMountPointResolvedDstPathReplaceInterceptor
    implements RegexMountPointInterceptor {

  private String srcRegexString;
  private String replaceString;
  private Pattern srcRegexPattern;

  RegexMountPointResolvedDstPathReplaceInterceptor(String srcRegex,
      String replaceString) {
    this.srcRegexString = srcRegex;
    this.replaceString = replaceString;
    this.srcRegexPattern = null;
  }

  public String getSrcRegexString() {
    return srcRegexString;
  }

  public String getReplaceString() {
    return replaceString;
  }

  public Pattern getSrcRegexPattern() {
    return srcRegexPattern;
  }

  @Override
  public void initialize() throws IOException {
    try {
      srcRegexPattern = Pattern.compile(srcRegexString);
    } catch (PatternSyntaxException ex) {
      throw new IOException(
          "Initialize interceptor failed, srcRegx:" + srcRegexString, ex);
    }
  }

  /**
   * Source won't be changed in the interceptor.
   *
   * @return source param string passed in.
   */
  @Override
  public String interceptSource(String source) {
    return source;
  }

  /**
   * Intercept resolved path, e.g.
   * Mount point /^(\\w+)/, ${1}.hadoop.net
   * If incoming path is /user1/home/tmp/job1,
   * then the resolved path str will be user1.
   *
   * @return intercepted string
   */
  @Override
  public String interceptResolvedDestPathStr(
      String parsedDestPathStr) {
    Matcher matcher = srcRegexPattern.matcher(parsedDestPathStr);
    return matcher.replaceAll(replaceString);
  }

  /**
   * The interceptRemainingPath will just return the remainingPath passed in.
   *
   */
  @Override
  public Path interceptRemainingPath(Path remainingPath) {
    return remainingPath;
  }

  @Override
  public RegexMountPointInterceptorType getType() {
    return REPLACE_RESOLVED_DST_PATH;
  }

  @Override
  public String serializeToString() {
    return REPLACE_RESOLVED_DST_PATH.getConfigName()
        + RegexMountPoint.INTERCEPTOR_INTERNAL_SEP + srcRegexString
        + RegexMountPoint.INTERCEPTOR_INTERNAL_SEP + replaceString;
  }

  /**
   * Create interceptor from config string. The string should be in
   * replaceresolvedpath:wordToReplace:replaceString
   * Note that we'll assume there's no ':' in the regex for the moment.
   *
   * @return Interceptor instance or null on bad config.
   */
  public static RegexMountPointResolvedDstPathReplaceInterceptor
      deserializeFromString(String serializedString) {
    String[] strings = serializedString
        .split(Character.toString(RegexMountPoint.INTERCEPTOR_INTERNAL_SEP));
    // We'll assume there's no ':' in the regex for the moment.
    if (strings.length != 3) {
      return null;
    }
    //The format should be like replaceresolvedpath:wordToReplace:replaceString
    return new RegexMountPointResolvedDstPathReplaceInterceptor(strings[1],
        strings[2]);
  }
}
