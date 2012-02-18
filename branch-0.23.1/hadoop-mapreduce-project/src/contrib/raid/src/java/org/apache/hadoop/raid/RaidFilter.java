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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.raid.protocol.PolicyInfo;

public class RaidFilter {
  static class Statistics {
    long numRaided = 0;
    long numTooNew = 0;
    long sizeTooNew = 0;
    long numTooSmall = 0;
    long sizeTooSmall = 0;

    public void aggregate(Statistics other) {
      this.numRaided += other.numRaided;
      this.numTooNew += other.numTooNew;
      this.sizeTooNew += other.sizeTooNew;
      this.numTooSmall += other.numTooSmall;
      this.sizeTooSmall += other.sizeTooSmall;
    }

    public String toString() {
      return "numRaided = " + numRaided +
             ", numTooNew = " + numTooNew +
             ", sizeTooNew = " + sizeTooNew +
             ", numTooSmall = " + numTooSmall +
             ", sizeTooSmall = " + sizeTooSmall;
    }
  }

  static class TimeBasedFilter extends Configured
    implements DirectoryTraversal.FileFilter {
    int targetRepl;
    Path raidDestPrefix;
    long modTimePeriod;
    long startTime;
    Statistics stats = new Statistics();
    String currentSrcPath = null;
    long[] modTimePeriods = new long[0];
    String[] otherSrcPaths = new String[0];

    TimeBasedFilter(Configuration conf, Path destPrefix, int targetRepl,
      long startTime, long modTimePeriod) {
      super(conf);
      this.raidDestPrefix = destPrefix;
      this.targetRepl = targetRepl;
      this.startTime = startTime;
      this.modTimePeriod = modTimePeriod;
    }

    TimeBasedFilter(Configuration conf,
      Path destPrefix, PolicyInfo info,
      List<PolicyInfo> allPolicies, long startTime, Statistics stats) {
      super(conf);
      this.raidDestPrefix = destPrefix;
      this.targetRepl = Integer.parseInt(info.getProperty("targetReplication"));
      this.modTimePeriod = Long.parseLong(info.getProperty("modTimePeriod"));
      this.startTime = startTime;
      this.stats = stats;
      this.currentSrcPath = info.getSrcPath().toUri().getPath();
      initializeOtherPaths(allPolicies);
    }

    private void initializeOtherPaths(List<PolicyInfo> allPolicies) {
      ArrayList<PolicyInfo> tmp = new ArrayList<PolicyInfo>(allPolicies);
      // Remove all policies where srcPath <= currentSrcPath or
      // matchingPrefixLength is < length(currentSrcPath)
      // The policies remaining are the only ones that could better
      // select a file chosen by the current policy.
      for (Iterator<PolicyInfo> it = tmp.iterator(); it.hasNext(); ) {
        String src = it.next().getSrcPath().toUri().getPath();
        if (src.compareTo(currentSrcPath) <= 0) {
          it.remove();
          continue;
        }
        int matchLen = matchingPrefixLength(src, currentSrcPath);
        if (matchLen < currentSrcPath.length()) {
          it.remove();
        }
      }
      // Sort in reverse lexicographic order.
      Collections.sort(tmp, new Comparator() {
        public int compare(Object o1, Object o2) {
          return 0 -
            ((PolicyInfo)o1).getSrcPath().toUri().getPath().compareTo(
              ((PolicyInfo)o1).getSrcPath().toUri().getPath());
        }
      });
      otherSrcPaths = new String[tmp.size()];
      modTimePeriods = new long[otherSrcPaths.length];
      for (int i = 0; i < otherSrcPaths.length; i++) {
        otherSrcPaths[i] = tmp.get(i).getSrcPath().toUri().getPath();
        modTimePeriods[i] = Long.parseLong(
          tmp.get(i).getProperty("modTimePeriod"));
      }
    }

    public boolean check(FileStatus f) throws IOException {
      if (!canChooseForCurrentPolicy(f)) {
        return false;
      }

      // If the source file has fewer than or equal to 2 blocks, then skip it.
      long blockSize = f.getBlockSize();
      if (2 * blockSize >= f.getLen()) {
        stats.numTooSmall++;
        stats.sizeTooSmall += f.getLen();
        return false;
      }

      boolean select = false;
      try {
        Object ppair = RaidNode.getParityFile(
            raidDestPrefix, f.getPath(), getConf());
        // Is there is a valid parity file?
        if (ppair != null) {
          // Is the source at the target replication?
          if (f.getReplication() != targetRepl) {
            // Select the file so that its replication can be set.
            select = true;
          } else {
            stats.numRaided++;
            // Nothing to do, don't select the file.
            select = false;
          }
        } else {
          // No parity file.
          if (f.getModificationTime() + modTimePeriod < startTime) {
            // If the file is not too new, choose it for raiding.
            select = true;
          } else {
            select = false;
            stats.numTooNew++;
            stats.sizeTooNew += f.getLen();
          }
        }
      } catch (java.io.FileNotFoundException e) {
        select = true; // destination file does not exist
      } catch (java.io.IOException e) {
        // If there is a problem with the har path, this will let us continue.
        DirectoryTraversal.LOG.error(
          "Error while selecting " + StringUtils.stringifyException(e));
      }
      return select;
    }

    /**
     * Checks if a file can be chosen for the current policy.
     */
    boolean canChooseForCurrentPolicy(FileStatus stat) {
      boolean choose = true;
      if (otherSrcPaths.length > 0) {
        String fileStr = stat.getPath().toUri().getPath();

        // For a given string, find the best matching srcPath.
        int matchWithCurrent = matchingPrefixLength(fileStr, currentSrcPath);
        for (int i = 0; i < otherSrcPaths.length; i++) {
          // If the file is too new, move to the next.
          if (stat.getModificationTime() > startTime - modTimePeriods[i]) {
            continue;
          }
          int matchLen = matchingPrefixLength(fileStr, otherSrcPaths[i]);
          if (matchLen > 0 &&
              fileStr.charAt(matchLen - 1) == Path.SEPARATOR_CHAR) {
            matchLen--;
          }
          if (matchLen > matchWithCurrent) {
            choose = false;
            break;
          }
        }
      }
      return choose;
    }

    int matchingPrefixLength(final String s1, final String s2) {
      int len = 0;
      for (int j = 0; j < s1.length() && j < s2.length(); j++) {
        if (s1.charAt(j) == s2.charAt(j)) {
          len++;
        } else {
          break;
        }
      }
      return len;
    }
  }

  static class PreferenceFilter extends Configured
    implements DirectoryTraversal.FileFilter {
    Path firstChoicePrefix;
    DirectoryTraversal.FileFilter secondChoiceFilter;

    PreferenceFilter(Configuration conf,
      Path firstChoicePrefix, Path secondChoicePrefix,
      int targetRepl, long startTime, long modTimePeriod) {
      super(conf);
      this.firstChoicePrefix = firstChoicePrefix;
      this.secondChoiceFilter = new TimeBasedFilter(conf,
        secondChoicePrefix, targetRepl, startTime, modTimePeriod);
    }

    PreferenceFilter(Configuration conf,
      Path firstChoicePrefix, Path secondChoicePrefix,
      PolicyInfo info, List<PolicyInfo> allPolicies, long startTime,
        Statistics stats) {
      super(conf);
      this.firstChoicePrefix = firstChoicePrefix;
      this.secondChoiceFilter = new TimeBasedFilter(
        conf, secondChoicePrefix, info, allPolicies, startTime, stats);
    }

    public boolean check(FileStatus f) throws IOException {
      Object firstChoicePPair =
        RaidNode.getParityFile(firstChoicePrefix, f.getPath(), getConf());
      if (firstChoicePPair == null) {
        // The decision is upto the the second choice filter.
        return secondChoiceFilter.check(f);
      } else {
        // There is already a parity file under the first choice path.
        // We dont want to choose this file.
        return false;
      }
    }
  }
}
