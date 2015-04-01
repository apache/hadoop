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
package org.apache.hadoop.nfs;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.util.LightWeightCache;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * This class provides functionality for loading and checking the mapping 
 * between client hosts and their access privileges.
 */
public class NfsExports {
  
  private static NfsExports exports = null;
  
  public static synchronized NfsExports getInstance(Configuration conf) {
    if (exports == null) {
      String matchHosts = conf.get(
          CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_KEY,
          CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_KEY_DEFAULT);
      int cacheSize = conf.getInt(Nfs3Constant.NFS_EXPORTS_CACHE_SIZE_KEY,
          Nfs3Constant.NFS_EXPORTS_CACHE_SIZE_DEFAULT);
      long expirationPeriodNano = conf.getLong(
          Nfs3Constant.NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_KEY,
          Nfs3Constant.NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_DEFAULT) * 1000 * 1000;
      try {
        exports = new NfsExports(cacheSize, expirationPeriodNano, matchHosts);
      } catch (IllegalArgumentException e) {
        LOG.error("Invalid NFS Exports provided: ", e);
        return exports;
      }
    }
    return exports;
  }
  
  public static final Log LOG = LogFactory.getLog(NfsExports.class);
  
  // only support IPv4 now
  private static final String IP_ADDRESS = 
      "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
  private static final String SLASH_FORMAT_SHORT = IP_ADDRESS + "/(\\d{1,3})";
  private static final String SLASH_FORMAT_LONG = IP_ADDRESS + "/" + IP_ADDRESS;
  
  private static final Pattern CIDR_FORMAT_SHORT = 
      Pattern.compile(SLASH_FORMAT_SHORT);
  
  private static final Pattern CIDR_FORMAT_LONG = 
      Pattern.compile(SLASH_FORMAT_LONG);

  // Hostnames are composed of series of 'labels' concatenated with dots.
  // Labels can be between 1-63 characters long, and can only take
  // letters, digits & hyphens. They cannot start and end with hyphens. For
  // more details, refer RFC-1123 & http://en.wikipedia.org/wiki/Hostname
  private static final String LABEL_FORMAT =
      "[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?";
  private static final Pattern HOSTNAME_FORMAT =
      Pattern.compile("^(" + LABEL_FORMAT + "\\.)*" + LABEL_FORMAT + "$");

  static class AccessCacheEntry implements LightWeightCache.Entry{
    private final String hostAddr;
    private AccessPrivilege access;
    private final long expirationTime; 
    
    private LightWeightGSet.LinkedElement next;
    
    AccessCacheEntry(String hostAddr, AccessPrivilege access,
        long expirationTime) {
      Preconditions.checkArgument(hostAddr != null);
      this.hostAddr = hostAddr;
      this.access = access;
      this.expirationTime = expirationTime;
    }
    
    @Override
    public int hashCode() {
      return hostAddr.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof AccessCacheEntry) {
        AccessCacheEntry entry = (AccessCacheEntry) obj;
        return this.hostAddr.equals(entry.hostAddr);
      }
      return false;
    }
    
    @Override
    public void setNext(LinkedElement next) {
      this.next = next;
    }

    @Override
    public LinkedElement getNext() {
      return this.next;
    }

    @Override
    public void setExpirationTime(long timeNano) {
      // we set expiration time in the constructor, and the expiration time 
      // does not change
    }

    @Override
    public long getExpirationTime() {
      return this.expirationTime;
    }
  }

  private final List<Match> mMatches;
  
  private final LightWeightCache<AccessCacheEntry, AccessCacheEntry> accessCache;
  private final long cacheExpirationPeriod;

  /**
   * Constructor.
   * @param cacheSize The size of the access privilege cache.
   * @param expirationPeriodNano The period 
   * @param matchingHosts A string specifying one or multiple matchers. 
   */
  NfsExports(int cacheSize, long expirationPeriodNano, String matchHosts) {
    this.cacheExpirationPeriod = expirationPeriodNano;
    accessCache = new LightWeightCache<AccessCacheEntry, AccessCacheEntry>(
        cacheSize, cacheSize, expirationPeriodNano, 0);        
    String[] matchStrings = matchHosts.split(
        CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_SEPARATOR);
    mMatches = new ArrayList<Match>(matchStrings.length);
    for(String mStr : matchStrings) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing match string '" + mStr + "'");
      }
      mStr = mStr.trim();
      if(!mStr.isEmpty()) {
        mMatches.add(getMatch(mStr));
      }
    }
  }
  
  /**
   * Return the configured group list
   */
  public String[] getHostGroupList() {
    int listSize = mMatches.size();
    String[] hostGroups = new String[listSize];

    for (int i = 0; i < mMatches.size(); i++) {
      hostGroups[i] = mMatches.get(i).getHostGroup();
    }
    return hostGroups;
  }
  
  public AccessPrivilege getAccessPrivilege(InetAddress addr) {
    return getAccessPrivilege(addr.getHostAddress(),
        addr.getCanonicalHostName());
  }
  
  AccessPrivilege getAccessPrivilege(String address, String hostname) {
    long now = System.nanoTime();
    AccessCacheEntry newEntry = new AccessCacheEntry(address,
        AccessPrivilege.NONE, now + this.cacheExpirationPeriod);
    // check if there is a cache entry for the given address
    AccessCacheEntry cachedEntry = accessCache.get(newEntry);
    if (cachedEntry != null && now < cachedEntry.expirationTime) {
      // get a non-expired cache entry, use it
      return cachedEntry.access;
    } else {
      for(Match match : mMatches) {
        if(match.isIncluded(address, hostname)) {
          if (match.accessPrivilege == AccessPrivilege.READ_ONLY) {
            newEntry.access = AccessPrivilege.READ_ONLY;
            break;
          } else if (match.accessPrivilege == AccessPrivilege.READ_WRITE) {
            newEntry.access = AccessPrivilege.READ_WRITE;
          }
        }
      }
      accessCache.put(newEntry);
      return newEntry.access;
    }
  }

  private static abstract class Match {
    private final AccessPrivilege accessPrivilege;

    private Match(AccessPrivilege accessPrivilege) {
      this.accessPrivilege = accessPrivilege;
    }

    public abstract boolean isIncluded(String address, String hostname);
    public abstract String getHostGroup();
  }
  
  /**
   * Matcher covering all client hosts (specified by "*")
   */
  private static class AnonymousMatch extends Match {
    private AnonymousMatch(AccessPrivilege accessPrivilege) {
      super(accessPrivilege);
    }
  
    @Override
    public boolean isIncluded(String address, String hostname) {
      return true;
    }

    @Override
    public String getHostGroup() {
      return "*";
    }
  }
  
  /**
   * Matcher using CIDR for client host matching
   */
  private static class CIDRMatch extends Match {
    private final SubnetInfo subnetInfo;
    
    private CIDRMatch(AccessPrivilege accessPrivilege, SubnetInfo subnetInfo) {
      super(accessPrivilege);
      this.subnetInfo = subnetInfo;
    }
    
    @Override
    public boolean isIncluded(String address, String hostname) {
      if(subnetInfo.isInRange(address)) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("CIDRNMatcher low = " + subnetInfo.getLowAddress() +
              ", high = " + subnetInfo.getHighAddress() +
              ", allowing client '" + address + "', '" + hostname + "'");
        }
        return true;
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("CIDRNMatcher low = " + subnetInfo.getLowAddress() +
            ", high = " + subnetInfo.getHighAddress() +
            ", denying client '" + address + "', '" + hostname + "'");
      }
      return false;
    }

    @Override
    public String getHostGroup() {
      return subnetInfo.getAddress() + "/" + subnetInfo.getNetmask();
    }
  }
  
  /**
   * Matcher requiring exact string match for client host
   */
  private static class ExactMatch extends Match {
    private final String ipOrHost;
    
    private ExactMatch(AccessPrivilege accessPrivilege, String ipOrHost) {
      super(accessPrivilege);
      this.ipOrHost = ipOrHost;
    }
    
    @Override
    public boolean isIncluded(String address, String hostname) {
      if(ipOrHost.equalsIgnoreCase(address) ||
          ipOrHost.equalsIgnoreCase(hostname)) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("ExactMatcher '" + ipOrHost + "', allowing client " +
              "'" + address + "', '" + hostname + "'");
        }
        return true;
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("ExactMatcher '" + ipOrHost + "', denying client " +
            "'" + address + "', '" + hostname + "'");
      }
      return false;
    }

    @Override
    public String getHostGroup() {
      return ipOrHost;
    }
  }

  /**
   * Matcher where client hosts are specified by regular expression
   */
  private static class RegexMatch extends Match {
    private final Pattern pattern;

    private RegexMatch(AccessPrivilege accessPrivilege, String wildcard) {
      super(accessPrivilege);
      this.pattern = Pattern.compile(wildcard, Pattern.CASE_INSENSITIVE);
    }

    @Override
    public boolean isIncluded(String address, String hostname) {
      if (pattern.matcher(address).matches()
          || pattern.matcher(hostname).matches()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("RegexMatcher '" + pattern.pattern()
              + "', allowing client '" + address + "', '" + hostname + "'");
        }
        return true;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("RegexMatcher '" + pattern.pattern()
            + "', denying client '" + address + "', '" + hostname + "'");
      }
      return false;
    }

    @Override
    public String getHostGroup() {
      return pattern.toString();
    }
  }

  /**
   * Loading a matcher from a string. The default access privilege is read-only.
   * The string contains 1 or 2 parts, separated by whitespace characters, where
   * the first part specifies the client hosts, and the second part (if 
   * existent) specifies the access privilege of the client hosts. I.e.,
   * 
   * "client-hosts [access-privilege]"
   */
  private static Match getMatch(String line) {
    String[] parts = line.split("\\s+");
    final String host;
    AccessPrivilege privilege = AccessPrivilege.READ_ONLY;
    switch (parts.length) {
    case 1:
      host = StringUtils.toLowerCase(parts[0]).trim();
      break;
    case 2:
      host = StringUtils.toLowerCase(parts[0]).trim();
      String option = parts[1].trim();
      if ("rw".equalsIgnoreCase(option)) {
        privilege = AccessPrivilege.READ_WRITE;
      }
      break;
    default:
      throw new IllegalArgumentException("Incorrectly formatted line '" + line
          + "'");
    }
    if (host.equals("*")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using match all for '" + host + "' and " + privilege);
      }
      return new AnonymousMatch(privilege);
    } else if (CIDR_FORMAT_SHORT.matcher(host).matches()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using CIDR match for '" + host + "' and " + privilege);
      }
      return new CIDRMatch(privilege, new SubnetUtils(host).getInfo());
    } else if (CIDR_FORMAT_LONG.matcher(host).matches()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using CIDR match for '" + host + "' and " + privilege);
      }
      String[] pair = host.split("/");
      return new CIDRMatch(privilege,
          new SubnetUtils(pair[0], pair[1]).getInfo());
    } else if (host.contains("*") || host.contains("?") || host.contains("[")
        || host.contains("]") || host.contains("(") || host.contains(")")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using Regex match for '" + host + "' and " + privilege);
      }
      return new RegexMatch(privilege, host);
    } else if (HOSTNAME_FORMAT.matcher(host).matches()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using exact match for '" + host + "' and " + privilege);
      }
      return new ExactMatch(privilege, host);
    } else {
      throw new IllegalArgumentException("Invalid hostname provided '" + host
          + "'");
    }
  }
}
