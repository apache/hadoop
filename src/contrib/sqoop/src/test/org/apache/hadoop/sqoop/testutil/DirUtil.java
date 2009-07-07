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

package org.apache.hadoop.sqoop.testutil;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Misc directory operations
 * 
 *
 */
public final class DirUtil {

  public static final Log LOG = LogFactory.getLog(DirUtil.class.getName());

  /**
   * recursively delete a dir and its children.
   * @param dir
   * @return true on succesful removal of a dir
   */
  public static boolean deleteDir(File dir) {
    if (dir.isDirectory()) {
      String [] children = dir.list();
      for (int i = 0; i < children.length; i++) {
        File f = new File(dir, children[i]);
        boolean success = deleteDir(f);
        if (!success) {
          LOG.warn("Could not delete " + f.getAbsolutePath());
          return false;
        }
      }
    }

    // The directory is now empty so delete it too.
    LOG.debug("Removing: " + dir);
    return dir.delete();
  }

}
