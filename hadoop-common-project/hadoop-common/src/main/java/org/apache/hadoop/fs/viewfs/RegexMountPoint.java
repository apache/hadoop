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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.viewfs.InodeTree.SlashPath;

/**
 * Regex mount point is build to implement regex based mount point.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RegexMountPoint<T> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RegexMountPoint.class.getName());

  private InodeTree inodeTree;
  private String srcPathRegex;
  private Pattern srcPattern;
  private String dstPath;
  private String interceptorSettingsString;
  private List<RegexMountPointInterceptor> interceptorList;

  public static final String SETTING_SRCREGEX_SEP = "#.";
  public static final char INTERCEPTOR_SEP = ';';
  public static final char INTERCEPTOR_INTERNAL_SEP = ':';
  // ${var},$var
  public static final Pattern VAR_PATTERN_IN_DEST =
      Pattern.compile("\\$((\\{\\w+\\})|(\\w+))");

  // Same var might have different representations.
  // e.g.
  // key => $key or key = > ${key}
  private Map<String, Set<String>> varInDestPathMap;

  public Map<String, Set<String>> getVarInDestPathMap() {
    return varInDestPathMap;
  }

  RegexMountPoint(InodeTree inodeTree, String sourcePathRegex,
      String destPath, String settingsStr) {
    this.inodeTree = inodeTree;
    this.srcPathRegex = sourcePathRegex;
    this.dstPath = destPath;
    this.interceptorSettingsString = settingsStr;
    this.interceptorList = new ArrayList<>();
  }

  /**
   * Initialize regex mount point.
   *
   * @throws IOException
   */
  public void initialize() throws IOException {
    try {
      srcPattern = Pattern.compile(srcPathRegex);
    } catch (PatternSyntaxException ex) {
      throw new IOException(
          "Failed to initialized mount point due to bad src path regex:"
              + srcPathRegex + ", dstPath:" + dstPath, ex);
    }
    varInDestPathMap = getVarListInString(dstPath);
    initializeInterceptors();
  }

  private void initializeInterceptors() throws IOException {
    if (interceptorSettingsString == null
        || interceptorSettingsString.isEmpty()) {
      return;
    }
    String[] interceptorStrArray =
        StringUtils.split(interceptorSettingsString, INTERCEPTOR_SEP);
    for (String interceptorStr : interceptorStrArray) {
      RegexMountPointInterceptor interceptor =
          RegexMountPointInterceptorFactory.create(interceptorStr);
      if (interceptor == null) {
        throw new IOException(
            "Illegal settings String " + interceptorSettingsString);
      }
      interceptor.initialize();
      interceptorList.add(interceptor);
    }
  }

  /**
   * Get $var1 and $var2 style variables in string.
   *
   * @param input - the string to be process.
   * @return
   */
  public static Map<String, Set<String>> getVarListInString(String input) {
    Map<String, Set<String>> varMap = new HashMap<>();
    Matcher matcher = VAR_PATTERN_IN_DEST.matcher(input);
    while (matcher.find()) {
      // $var or ${var}
      String varName = matcher.group(0);
      // var or {var}
      String strippedVarName = matcher.group(1);
      if (strippedVarName.startsWith("{")) {
        // {varName} = > varName
        strippedVarName =
            strippedVarName.substring(1, strippedVarName.length() - 1);
      }
      varMap.putIfAbsent(strippedVarName, new HashSet<>());
      varMap.get(strippedVarName).add(varName);
    }
    return varMap;
  }

  public String getSrcPathRegex() {
    return srcPathRegex;
  }

  public Pattern getSrcPattern() {
    return srcPattern;
  }

  public String getDstPath() {
    return dstPath;
  }

  public static Pattern getVarPatternInDest() {
    return VAR_PATTERN_IN_DEST;
  }

  /**
   * Get resolved path from regex mount points.
   *  E.g. link: ^/user/(?<username>\\w+) => s3://$user.apache.com/_${user}
   *  srcPath: is /user/hadoop/dir1
   *  resolveLastComponent: true
   *  then return value is s3://hadoop.apache.com/_hadoop
   * @param srcPath - the src path to resolve
   * @param resolveLastComponent - whether resolve the path after last `/`
   * @return mapped path of the mount point.
   */
  public InodeTree.ResolveResult<T> resolve(final String srcPath,
      final boolean resolveLastComponent) {
    String pathStrToResolve = getPathToResolve(srcPath, resolveLastComponent);
    for (RegexMountPointInterceptor interceptor : interceptorList) {
      pathStrToResolve = interceptor.interceptSource(pathStrToResolve);
    }
    LOGGER.debug("Path to resolve:" + pathStrToResolve + ", srcPattern:"
        + getSrcPathRegex());
    Matcher srcMatcher = getSrcPattern().matcher(pathStrToResolve);
    String parsedDestPath = getDstPath();
    int mappedCount = 0;
    String resolvedPathStr = "";
    while (srcMatcher.find()) {
      resolvedPathStr = pathStrToResolve.substring(0, srcMatcher.end());
      Map<String, Set<String>> varMap = getVarInDestPathMap();
      for (Map.Entry<String, Set<String>> entry : varMap.entrySet()) {
        String regexGroupNameOrIndexStr = entry.getKey();
        Set<String> groupRepresentationStrSetInDest = entry.getValue();
        parsedDestPath = replaceRegexCaptureGroupInPath(
            parsedDestPath, srcMatcher,
            regexGroupNameOrIndexStr, groupRepresentationStrSetInDest);
      }
      ++mappedCount;
    }
    if (0 == mappedCount) {
      return null;
    }
    Path remainingPath = getRemainingPathStr(srcPath, resolvedPathStr);
    for (RegexMountPointInterceptor interceptor : interceptorList) {
      parsedDestPath = interceptor.interceptResolvedDestPathStr(parsedDestPath);
      remainingPath =
          interceptor.interceptRemainingPath(remainingPath);
    }
    InodeTree.ResolveResult resolveResult = inodeTree
        .buildResolveResultForRegexMountPoint(InodeTree.ResultKind.EXTERNAL_DIR,
            resolvedPathStr, parsedDestPath, remainingPath);
    return resolveResult;
  }

  private Path getRemainingPathStr(
      String srcPath,
      String resolvedPathStr) {
    String remainingPathStr = srcPath.substring(resolvedPathStr.length());
    if (!remainingPathStr.startsWith("/")) {
      remainingPathStr = "/" + remainingPathStr;
    }
    return new Path(remainingPathStr);
  }

  private String getPathToResolve(
      String srcPath, boolean resolveLastComponent) {
    if (resolveLastComponent) {
      return srcPath;
    }
    int lastSlashIndex = srcPath.lastIndexOf(SlashPath.toString());
    if (lastSlashIndex == -1) {
      return null;
    }
    return srcPath.substring(0, lastSlashIndex);
  }

  /**
   * Use capture group named regexGroupNameOrIndexStr in mather to replace
   * parsedDestPath.
   * E.g. link: ^/user/(?<username>\\w+) => s3://$user.apache.com/_${user}
   * srcMatcher is from /user/hadoop.
   * Then the params will be like following.
   * parsedDestPath: s3://$user.apache.com/_${user},
   * regexGroupNameOrIndexStr: user
   * groupRepresentationStrSetInDest: {user:$user; user:${user}}
   * return value will be s3://hadoop.apache.com/_hadoop
   * @param parsedDestPath
   * @param srcMatcher
   * @param regexGroupNameOrIndexStr
   * @param groupRepresentationStrSetInDest
   * @return return parsedDestPath while ${var},$var replaced or
   * parsedDestPath nothing found.
   */
  private String replaceRegexCaptureGroupInPath(
      String parsedDestPath,
      Matcher srcMatcher,
      String regexGroupNameOrIndexStr,
      Set<String> groupRepresentationStrSetInDest) {
    String groupValue = getRegexGroupValueFromMather(
        srcMatcher, regexGroupNameOrIndexStr);
    if (groupValue == null) {
      return parsedDestPath;
    }
    for (String varName : groupRepresentationStrSetInDest) {
      parsedDestPath = parsedDestPath.replace(varName, groupValue);
      LOGGER.debug("parsedDestPath value is:" + parsedDestPath);
    }
    return parsedDestPath;
  }

  /**
   * Get matched capture group value from regex matched string. E.g.
   * Regex: ^/user/(?<username>\\w+), regexGroupNameOrIndexStr: userName
   * then /user/hadoop should return hadoop while call
   * getRegexGroupValueFromMather(matcher, usersName)
   * or getRegexGroupValueFromMather(matcher, 1)
   *
   * @param srcMatcher - the matcher to be use
   * @param regexGroupNameOrIndexStr - the regex group name or index
   * @return - Null if no matched group named regexGroupNameOrIndexStr found.
   */
  private String getRegexGroupValueFromMather(
      Matcher srcMatcher, String regexGroupNameOrIndexStr) {
    if (regexGroupNameOrIndexStr.matches("\\d+")) {
      // group index
      int groupIndex = Integer.parseUnsignedInt(regexGroupNameOrIndexStr);
      if (groupIndex >= 0 && groupIndex <= srcMatcher.groupCount()) {
        return srcMatcher.group(groupIndex);
      }
    } else {
      // named group in regex
      return srcMatcher.group(regexGroupNameOrIndexStr);
    }
    return null;
  }

}
