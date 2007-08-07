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
package org.apache.hadoop.fs;

import java.text.*;
import java.io.*;
import java.net.URI;
import java.util.Date;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.*;

/** Provides a <i>trash</i> feature.  Files may be moved to a trash directory.
 * They're initially stored in a <i>current</i> sub-directory of the trash
 * directory.  Within that sub-directory their original path is preserved.
 * Periodically one may checkpoint the current trash and remove older
 * checkpoints.  (This design permits trash management without enumeration of
 * the full trash content, without date support in the filesystem, and without
 * clock synchronization.)
 */
public class Trash extends Configured {
  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.fs.Trash");

  private static final String CURRENT = "Current";
  private static final DateFormat CHECKPOINT = new SimpleDateFormat("yyMMddHHmm");
  private static final int MSECS_PER_MINUTE = 60*1000;

  private FileSystem fs;
  private Path root;
  private Path current;
  private long interval;

  /** Construct a trash can accessor.
   * @param conf a Configuration
   */
  public Trash(Configuration conf) throws IOException {
    super(conf);

    Path root = new Path(conf.get("fs.trash.root", "/tmp/Trash"));

    this.fs = root.getFileSystem(conf);

    if (!root.isAbsolute())
      root = new Path(fs.getWorkingDirectory(), root);

    this.root = root;
    this.current = new Path(root, CURRENT);
    this.interval = conf.getLong("fs.trash.interval", 60) * MSECS_PER_MINUTE;
  }

  /** Move a file or directory to the current trash directory.
   * @return false if the item is already in the trash or trash is disabled
   */ 
  public boolean moveToTrash(Path path) throws IOException {
    if (interval == 0)
      return false;

    if (!path.isAbsolute())                       // make path absolute
      path = new Path(fs.getWorkingDirectory(), path);

    if (!fs.exists(path))                         // check that path exists
      throw new FileNotFoundException(path.toString());

    URI rootUri = root.toUri();
    String dirPath = path.toUri().getPath();

    if (dirPath.startsWith(rootUri.getPath())) {  // already in trash
      return false;
    }

    Path trashPath =                              // create path in current
      new Path(rootUri.getScheme(), rootUri.getAuthority(),
               current.toUri().getPath()+dirPath);

    IOException cause = null;
    
    // try twice, in case checkpoint between the mkdirs() & rename()
    for (int i = 0; i < 2; i++) {
      Path trashDir = trashPath.getParent();
      if (!fs.mkdirs(trashDir)) {                 // make parent directory
        throw new IOException("Failed to create trash directory: "+trashDir);
      }
      try {
        //
        // if the target path in Trash already exists, then append with 
        // a number. Start from 1.
        //
        String orig = trashPath.toString();
        for (int j = 1; fs.exists(trashPath); j++) {
          trashPath = new Path(orig + "." + j);
        }
        if (fs.rename(path, trashPath))           // move to current trash
          return true;
      } catch (IOException e) {
        cause = e;
      }
    }
    throw (IOException)
      new IOException("Failed to move to trash: "+path).initCause(cause);
  }

  /** Create a trash checkpoint. */
  public void checkpoint() throws IOException {
    if (!fs.exists(current))                      // no trash, no checkpoint
      return;

    Path checkpoint;
    synchronized (CHECKPOINT) {
      checkpoint = new Path(root, CHECKPOINT.format(new Date()));
    }

    if (fs.rename(current, checkpoint)) {
      LOG.info("Created trash checkpoint: "+checkpoint);
    } else {
      throw new IOException("Failed to checkpoint trash: "+checkpoint);
    }
  }

  /** Delete old checkpoints. */
  public void expunge() throws IOException {
    Path[] dirs = fs.listPaths(root);             // scan trash sub-directories
    long now = System.currentTimeMillis();
    for (int i = 0; i < dirs.length; i++) {
      Path dir = dirs[i];
      String name = dir.getName();
      if (name.equals(CURRENT))                   // skip current
        continue;

      long time;
      try {
        synchronized (CHECKPOINT) {
          time = CHECKPOINT.parse(name).getTime();
        }
      } catch (ParseException e) {
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if ((now - interval) > time) {
        if (fs.delete(dir)) {
          LOG.info("Deleted trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't delete checkpoint: "+dir+" Ignoring.");
        }
      }
    }
  }

  //
  // get the current working directory
  //
  Path getCurrentTrashDir() {
    return current;
  }

  /** Return a {@link Runnable} that periodically empties the trash.
   * Only one checkpoint is kept at a time.
   */
  public Runnable getEmptier() {
    return new Emptier();
  }

  private class Emptier implements Runnable {

    public void run() {
      if (interval == 0)
        return;                                   // trash disabled

      long now = System.currentTimeMillis();
      long end = ceiling(now, interval);
      while (true) {
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (InterruptedException e) {
          return;                                 // exit on interrupt
        }
          
        now = System.currentTimeMillis();
        if (now >= end) {

          try {
            expunge();
          } catch (IOException e) {
            LOG.warn("Trash expunge caught: "+e+". Ignoring.");
          }

          try {
            checkpoint();
          } catch (IOException e) {
            LOG.warn("Trash checkpoint caught: "+e+". Ignoring.");
          }

          end = ceiling(now, interval);
        }
      }
    }

    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }
    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }

  }

  /** Run an emptier.*/
  public static void main(String[] args) throws Exception {
    new Trash(new Configuration()).getEmptier().run();
  }

}
