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

package org.apache.hadoop.mapred;

import io.netty.channel.group.ChannelGroup;

import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.util.Shell;

import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_MAX_SHUFFLE_CONNECTIONS;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_BUFFER_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_MANAGE_OS_CACHE;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_READAHEAD_BYTES;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED;
import static org.apache.hadoop.mapred.ShuffleHandler.DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.MAX_SHUFFLE_CONNECTIONS;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_BUFFER_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_MANAGE_OS_CACHE;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_MAX_SESSION_OPEN_FILES;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_READAHEAD_BYTES;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_TRANSFERTO_ALLOWED;
import static org.apache.hadoop.mapred.ShuffleHandler.SUFFLE_SSL_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.mapred.ShuffleHandler.WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class ShuffleChannelHandlerContext {

  public final Configuration conf;
  public final JobTokenSecretManager secretManager;
  public final Map<String, String> userRsrc;
  public final LoadingCache<ShuffleHandler.AttemptPathIdentifier,
      ShuffleHandler.AttemptPathInfo> pathCache;
  public final IndexCache indexCache;
  public final ShuffleHandler.ShuffleMetrics metrics;
  public final ChannelGroup allChannels;


  public final boolean connectionKeepAliveEnabled;
  public final int sslFileBufferSize;
  public final int connectionKeepAliveTimeOut;
  public final int mapOutputMetaInfoCacheSize;

  public final AtomicInteger activeConnections = new AtomicInteger();

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile.
   */
  public final boolean manageOsCache;
  public final int readaheadLength;
  public final int maxShuffleConnections;
  public final int shuffleBufferSize;
  public final boolean shuffleTransferToAllowed;
  public final int maxSessionOpenFiles;
  public final ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  public int port = -1;

  public ShuffleChannelHandlerContext(Configuration conf,
                                      Map<String, String> userRsrc,
                                      JobTokenSecretManager secretManager,
                                      LoadingCache<ShuffleHandler.AttemptPathIdentifier,
                                          ShuffleHandler.AttemptPathInfo> patCache,
                                      IndexCache indexCache,
                                      ShuffleHandler.ShuffleMetrics metrics,
                                      ChannelGroup allChannels) {
    this.conf = conf;
    this.userRsrc = userRsrc;
    this.secretManager = secretManager;
    this.pathCache = patCache;
    this.indexCache = indexCache;
    this.metrics = metrics;
    this.allChannels = allChannels;

    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
        DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);
    connectionKeepAliveEnabled =
        conf.getBoolean(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED,
            DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED);
    connectionKeepAliveTimeOut =
        Math.max(1, conf.getInt(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
            DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT));
    mapOutputMetaInfoCacheSize =
        Math.max(1, conf.getInt(SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE,
            DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE));

    manageOsCache = conf.getBoolean(SHUFFLE_MANAGE_OS_CACHE,
        DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

    readaheadLength = conf.getInt(SHUFFLE_READAHEAD_BYTES,
        DEFAULT_SHUFFLE_READAHEAD_BYTES);

    maxShuffleConnections = conf.getInt(MAX_SHUFFLE_CONNECTIONS,
        DEFAULT_MAX_SHUFFLE_CONNECTIONS);

    shuffleBufferSize = conf.getInt(SHUFFLE_BUFFER_SIZE,
        DEFAULT_SHUFFLE_BUFFER_SIZE);

    shuffleTransferToAllowed = conf.getBoolean(SHUFFLE_TRANSFERTO_ALLOWED,
        (Shell.WINDOWS)?WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED:
            DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED);

    maxSessionOpenFiles = conf.getInt(SHUFFLE_MAX_SESSION_OPEN_FILES,
        DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES);
  }

  void setPort(int port) {
    this.port = port;
  }
}
