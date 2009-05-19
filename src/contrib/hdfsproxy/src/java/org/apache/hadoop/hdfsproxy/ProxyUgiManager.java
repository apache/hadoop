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

package org.apache.hadoop.hdfsproxy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Shell;

/** An ugi manager that maintains a temporary ugi cache */
public class ProxyUgiManager {
  private static final Map<String, CachedUgi> ugiCache = new HashMap<String, CachedUgi>();
  private static long ugiLifetime;
  /** username can only comprise of 0-9a-zA-Z and underscore, i.e. \w */
  private static final Pattern USERNAME_PATTERN = Pattern.compile("^\\w+$");
  static final int CLEANUP_THRESHOLD = 1000;

  static {
    Configuration conf = new Configuration(false);
    conf.addResource("hdfsproxy-default.xml");
    ugiLifetime = conf.getLong("hdfsproxy.ugi.cache.ugi.lifetime", 15) * 60 * 1000L;
  }

  /**
   * retrieve an ugi for a user. try the cache first, if not found, get it by
   * running a shell command
   */
  public static synchronized UnixUserGroupInformation getUgiForUser(
      String userName) {
    long now = System.currentTimeMillis();
    long cutoffTime = now - ugiLifetime;
    CachedUgi cachedUgi = ugiCache.get(userName);
    if (cachedUgi != null && cachedUgi.getInitTime() > cutoffTime)
      return cachedUgi.getUgi();
    UnixUserGroupInformation ugi = null;
    try {
      ugi = getUgi(userName);
    } catch (IOException e) {
      return null;
    }
    if (ugiCache.size() > CLEANUP_THRESHOLD) { // remove expired ugi's first
      for (Iterator<Map.Entry<String, CachedUgi>> it = ugiCache.entrySet()
          .iterator(); it.hasNext();) {
        Map.Entry<String, CachedUgi> e = it.next();
        if (e.getValue().getInitTime() < cutoffTime) {
          it.remove();
        }
      }
    }
    ugiCache.put(ugi.getUserName(), new CachedUgi(ugi, now));
    return ugi;
  }

  /** clear the ugi cache */
  public static synchronized void clearCache() {
    ugiCache.clear();
  }

  /** set ugi lifetime, only for junit testing purposes */
  static synchronized void setUgiLifetime(long lifetime) {
    ugiLifetime = lifetime;
  }

  /** save an ugi to cache, only for junit testing purposes */
  static synchronized void saveToCache(UnixUserGroupInformation ugi) {
    ugiCache.put(ugi.getUserName(), new CachedUgi(ugi, System
        .currentTimeMillis()));
  }

  /** get cache size, only for junit testing purposes */
  static synchronized int getCacheSize() {
    return ugiCache.size();
  }

  /**
   * Get the ugi for a user by running shell command "id -Gn"
   * 
   * @param userName name of the user
   * @return ugi of the user
   * @throws IOException if encounter any error while running the command
   */
  private static UnixUserGroupInformation getUgi(String userName)
      throws IOException {
    if (userName == null || !USERNAME_PATTERN.matcher(userName).matches())
      throw new IOException("Invalid username=" + userName);
    String[] cmd = new String[] { "bash", "-c", "id -Gn '" + userName + "'"};
    String[] groups = Shell.execCommand(cmd).split("\\s+");
    return new UnixUserGroupInformation(userName, groups);
  }

  /** cached ugi object with its associated init time */
  private static class CachedUgi {
    final UnixUserGroupInformation ugi;
    final long initTime;

    CachedUgi(UnixUserGroupInformation ugi, long initTime) {
      this.ugi = ugi;
      this.initTime = initTime;
    }

    UnixUserGroupInformation getUgi() {
      return ugi;
    }

    long getInitTime() {
      return initTime;
    }

    /** {@inheritDoc} */
    public int hashCode() {
      return ugi.hashCode();
    }

    static boolean isEqual(Object a, Object b) {
      return a == b || (a != null && a.equals(b));
    }

    /** {@inheritDoc} */
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj != null && obj instanceof CachedUgi) {
        CachedUgi that = (CachedUgi) obj;
        return isEqual(this.ugi, that.ugi) && this.initTime == that.initTime;
      }
      return false;
    }

  }
}
