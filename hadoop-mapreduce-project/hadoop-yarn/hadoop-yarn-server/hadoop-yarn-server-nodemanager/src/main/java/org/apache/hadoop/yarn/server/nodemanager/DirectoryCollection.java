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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * Manages a list of local storage directories.
 */
class DirectoryCollection {
  private static final Log LOG = LogFactory.getLog(DirectoryCollection.class);

  // Good local storage directories
  private List<String> localDirs;
  private List<String> failedDirs;
  private int numFailures;

  public DirectoryCollection(String[] dirs) {
    localDirs = new ArrayList<String>();
    localDirs.addAll(Arrays.asList(dirs));
    failedDirs = new ArrayList<String>();
  }

  /**
   * @return the current valid directories 
   */
  synchronized List<String> getGoodDirs() {
    return localDirs;
  }

  /**
   * @return the failed directories
   */
  synchronized List<String> getFailedDirs() {
    return failedDirs;
  }

  /**
   * @return total the number of directory failures seen till now
   */
  synchronized int getNumFailures() {
    return numFailures;
  }

  /**
   * Check the health of current set of local directories, updating the list
   * of valid directories if necessary.
   * @return <em>true</em> if there is a new disk-failure identified in
   *         this checking. <em>false</em> otherwise.
   */
  synchronized boolean checkDirs() {
    int oldNumFailures = numFailures;
    ListIterator<String> it = localDirs.listIterator();
    while (it.hasNext()) {
      final String dir = it.next();
      try {
        DiskChecker.checkDir(new File(dir));
      } catch (DiskErrorException de) {
        LOG.warn("Directory " + dir + " error " +
            de.getMessage() + ", removing from the list of valid directories.");
        it.remove();
        failedDirs.add(dir);
        numFailures++;
      }
    }
    if (numFailures > oldNumFailures) {
      return true;
    }
    return false;
  }
}
