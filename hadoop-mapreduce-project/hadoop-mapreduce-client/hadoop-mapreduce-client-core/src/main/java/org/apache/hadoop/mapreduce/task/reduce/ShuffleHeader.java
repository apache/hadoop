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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;

/**
 * Shuffle Header information that is sent by the TaskTracker and
 * deciphered by the Fetcher thread of Reduce task
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ShuffleHeader implements Writable {
  private static final Log LOG = LogFactory.getLog(ShuffleHeader.class);

  /** Header info of the shuffle http request/response */
  public static final String HTTP_HEADER_NAME = "name";
  public static final String DEFAULT_HTTP_HEADER_NAME = "mapreduce";
  public static final String HTTP_HEADER_VERSION = "version";
  public static final String HTTP_HEADER_TARGET_VERSION = "target_version";
  public static final String DEFAULT_HTTP_HEADER_VERSION = "1.0.0";
  /** Header version instances*/
  public static final HeaderVersion DEFAULT_HEADER_VERSION_INSTANCE = new HeaderVersion(
          DEFAULT_HTTP_HEADER_VERSION);
  public static final HeaderVersion HEADER_VERSION_INSTANCE_V1_1 = new HeaderVersion("1.1.0");
  // the smaller index is the newer version, if client&server not matched, should set back to nex index
  public static final List<HeaderVersion> HEADER_VERSION_LIST = Lists
          .newArrayList(
                  HEADER_VERSION_INSTANCE_V1_1,
                  DEFAULT_HEADER_VERSION_INSTANCE
          );
  public static final HeaderVersionProtocol DEFAULT_VERSION_PROTOCOL = new HeaderVersionProtocol(
          DEFAULT_HEADER_VERSION_INSTANCE, DEFAULT_HEADER_VERSION_INSTANCE);

  /**
   * The longest possible length of task attempt id that we will accept.
   */
  private static final int MAX_ID_LENGTH = 1000;

  String mapId;
  long uncompressedLength;
  long compressedLength;
  int forReduce;
  /**
   * use to decide property to write or read.
   */
  HeaderVersion headerVersion;

  /**
   * for shuffle client
   */
  public ShuffleHeader() {
    this.headerVersion = DEFAULT_HEADER_VERSION_INSTANCE;
  }

  /**
   * for shuffle client
   */
  public ShuffleHeader(HeaderVersion headerVersion) {
    this.headerVersion = headerVersion;
  }

  public ShuffleHeader(String mapId, long compressedLength,
          long uncompressedLength, int forReduce,HeaderVersion headerVersion) {
    this.mapId = mapId;
    this.compressedLength = compressedLength;
    this.uncompressedLength = uncompressedLength;
    this.forReduce = forReduce;
    this.headerVersion = headerVersion;
  }

  public ShuffleHeader(String mapId, long compressedLength,
          long uncompressedLength, int forReduce) {
    this(mapId, compressedLength, uncompressedLength, forReduce, DEFAULT_HEADER_VERSION_INSTANCE);
  }

  public void readFields(DataInput in) throws IOException {
    mapId = WritableUtils.readStringSafely(in, MAX_ID_LENGTH);
    compressedLength = WritableUtils.readVLong(in);
    uncompressedLength = WritableUtils.readVLong(in);
    forReduce = WritableUtils.readVInt(in);
    readByVersion(in);
  }

  private void readByVersion(DataInput in) throws IOException {
    if (headerVersion == null || headerVersion.compareTo(DEFAULT_HEADER_VERSION_INSTANCE) == 0) {
      return;
    }
    // if current version larger then target version,
    // we should read the properties owned by the version in order.
    if (headerVersion.compareTo(HEADER_VERSION_INSTANCE_V1_1) >= 0) {
      // todo here read version properties
    }
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, mapId);
    WritableUtils.writeVLong(out, compressedLength);
    WritableUtils.writeVLong(out, uncompressedLength);
    WritableUtils.writeVInt(out, forReduce);
    writeByVersion(out);
  }

  /**
   * unified hard code header version for new properties.
   * @throws IOException
   */
  private void writeByVersion(DataOutput out) throws IOException {
    if (headerVersion == null || headerVersion.compareTo(DEFAULT_HEADER_VERSION_INSTANCE) == 0) {
      return;
    }
    // if current version larger then target version,
    // we should serialize the properties owned by the version in order.
    if (headerVersion.compareTo(HEADER_VERSION_INSTANCE_V1_1) >= 0) {
      // todo here write version properties
    }
  }

  public void setHeaderVersion(
          String headerVersion) { this.headerVersion = new HeaderVersion(headerVersion); }

  /**
   * get current newest header version, which client or server side can support.
   * @return
   */
  public static HeaderVersion getNewestVersion() {
    return HEADER_VERSION_LIST.get(0);
  }

  /**
   * for shuffle server
   * @param currentHeaderVersion
   * @param targetHeaderVersion
   * @return
   */
  public static HeaderVersionProtocol getHeaderVersionProtocol(String currentHeaderVersion,
          String targetHeaderVersion) {
    HeaderVersion current = currentHeaderVersion == null ? DEFAULT_HEADER_VERSION_INSTANCE
            : new HeaderVersion(currentHeaderVersion);
    // if client request header not contains target_version,
    // means in upgrade phase, use current header version in compatible
    HeaderVersion target = targetHeaderVersion == null ? current
            : new HeaderVersion(targetHeaderVersion);

    return new HeaderVersionProtocol(current, target);
  }

  /**
   * for shuffle client
   * @param currentHeaderVersion
   * @return
   */
  public static HeaderVersionProtocol getHeaderVersionProtocol(String currentHeaderVersion) {
    HeaderVersion current = currentHeaderVersion == null ? DEFAULT_HEADER_VERSION_INSTANCE
            : new HeaderVersion(currentHeaderVersion);

    return new HeaderVersionProtocol(current);
  }

  public static class HeaderVersionProtocol {

    // for compatible
    private HeaderVersion defaultVersion;
    private HeaderVersion targetVersion;
    // final chosen version
    private HeaderVersion compatibleVersion;

    /**
     * for shuffle client
     * @param defaultVersion
     */
    public HeaderVersionProtocol(
            HeaderVersion defaultVersion) {
      this.defaultVersion = defaultVersion;
      this.targetVersion = defaultVersion;
      this.compatibleVersion = defaultVersion;
    }

    /**
     * for shuffle server
     * @param defaultVersion
     * @param targetVersion
     */
    public HeaderVersionProtocol(
            HeaderVersion defaultVersion,
            HeaderVersion targetVersion) {
      this.defaultVersion = defaultVersion;
      this.targetVersion = targetVersion;
      setUpCompatibleVersion();
    }

    private void setUpCompatibleVersion() {
      HeaderVersion matchedDefaultVersion = null;
      for (HeaderVersion version : HEADER_VERSION_LIST) {
        if (version.compareTo(targetVersion) <= 0) {
          compatibleVersion = version;
          // find first one which less or equal than target
          // if found, should break.
          break;
        }
        if (version.compareTo(defaultVersion) == 0) {
          matchedDefaultVersion = version;
        }
      }

      // if can not find compatible version, set to client default version
      if (compatibleVersion == null) {
        compatibleVersion = matchedDefaultVersion;
      }
    }

    public boolean isHeaderCompatible(String headerName) {
      if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(headerName)) {
        LOG.error(
            String.format(
                "Shuffle isHeaderCompatible: false, request header name: %s", headerName));
        return false;
      }
      String versionLogMsg = getVersionMsg();
      if (compatibleVersion == null || !isMatchedVersion()) {
        LOG.error(String.format("Shuffle version is not compatible, %s", versionLogMsg));
        return false;
      }

      // compare with newest version
      if (compatibleVersion.compareTo(getNewestVersion()) != 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Shuffle version should fall back to compatible version: %s, %s",
                  compatibleVersion, versionLogMsg));
        }
      }
      return true;
    }

    private boolean isMatchedVersion() {
      for (HeaderVersion version : HEADER_VERSION_LIST) {
        if (version.compareTo(compatibleVersion) == 0) {
          return true;
        }
      }
      return false;
    }

    private String getVersionMsg() {
      StringBuilder supportedVersion = new StringBuilder();
      for (HeaderVersion headerVersion : HEADER_VERSION_LIST) {
        supportedVersion.append(headerVersion.getVersionStr()).append(",");
      }
      return String.format("get protocol: %s, supported versions: %s",
              this.toString(), supportedVersion.toString());
    }

    @Override
    public String toString() {
      return "HeaderVersionProtocol{" +
              "defaultVersion=" + defaultVersion +
              ", targetVersion=" + targetVersion +
              ", compatibleVersion=" + compatibleVersion +
              '}';
    }

    public HeaderVersion getDefaultVersion() { return defaultVersion; }

    public void setDefaultVersion(
            HeaderVersion defaultVersion) { this.defaultVersion = defaultVersion; }

    public HeaderVersion getTargetVersion() { return targetVersion; }

    public void setTargetVersion(
            HeaderVersion targetVersion) { this.targetVersion = targetVersion; }

    public HeaderVersion getCompatibleVersion() { return compatibleVersion; }

    public void setCompatibleVersion(
            HeaderVersion compatibleVersion) { this.compatibleVersion = compatibleVersion; }
  }

  /**
   * parse header version & compare between versions
   * eg 1.0.0
   */
  public static class HeaderVersion implements Comparable<HeaderVersion> {

    private final String versionStr;
    private final int majorVersion;
    private final int minorVersion;
    private final int revision;

    public HeaderVersion(String version) {
      versionStr = version;
      String[] versions = StringUtils.split(version, ".");
      majorVersion = Integer.parseInt(versions[0]);
      minorVersion = Integer.parseInt(versions[1]);
      revision = Integer.parseInt(versions[2]);
    }

    public String getVersionStr() { return versionStr; }

    @Override
    public int compareTo(HeaderVersion o) {
      if (o.majorVersion > majorVersion) {
        return -1;
      } else if (o.majorVersion < majorVersion) {
        return 1;
      }

      if (o.minorVersion > minorVersion) {
        return -1;
      } else if (o.minorVersion < minorVersion) {
        return 1;
      }

      if (o.revision > revision) {
        return -1;
      } else if (o.revision < revision) {
        return 1;
      }

      return 0;
    }

    @Override
    public String toString() {
      return "HeaderVersion{" +
              "versionStr='" + versionStr + '\'' +
              '}';
    }
  }
}