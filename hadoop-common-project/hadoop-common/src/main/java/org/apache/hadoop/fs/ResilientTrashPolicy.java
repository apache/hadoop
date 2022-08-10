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

package org.apache.hadoop.fs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.DurationInfo;

/**
 * A resilient rename policy which will declare success if
 * the source file does not exist, and swallow any raised
 * IOEs in the superclass's {@code moveToTrash()} method.
 */
public class ResilientTrashPolicy extends TrashPolicyDefault {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResilientTrashPolicy.class);

  public ResilientTrashPolicy() {
  }

  @Override
  public boolean moveToTrash(Path path) throws IOException {
    if (!isEnabled()) {
      return false;
    }
    if (!path.isAbsolute()) {
      // make path absolute
      path = new Path(fs.getWorkingDirectory(), path);
    }
    if (!fs.exists(path)) {
      // path doesn't actually exist.
      return true;
    }

    try (DurationInfo info = new DurationInfo(LOG, true, "moveToTrash(%s)", path)) {
      return super.moveToTrash(path);
    } catch (IOException e) {
      if (!fs.exists(path)) {
        // race condition with the trash setup; something else moved it.
        LOG.info("'{} was deleted before it could be moved to trash", path);
        LOG.debug("IOE raised on moveToTrash({})", path, e);
        // report success
        return true;
      } else {
        // source path still exists, so throw the exception.
        throw e;
      }
    }
  }
}
