/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Manage regionserver shutdown hooks.
 * @see #install(Configuration, FileSystem, Stoppable, Thread)
 */
public class ShutdownHook {
  private static final Log LOG = LogFactory.getLog(ShutdownHook.class);
  private static final String CLIENT_FINALIZER_DATA_METHOD = "clientFinalizer";

  /**
   * Key for boolean configuration whose default is true.
   */
  public static final String RUN_SHUTDOWN_HOOK = "hbase.shutdown.hook";

  /**
   * Key for a long configuration on how much time to wait on the fs shutdown
   * hook. Default is 30 seconds.
   */
  public static final String FS_SHUTDOWN_HOOK_WAIT = "hbase.fs.shutdown.hook.wait";

  /**
   * A place for keeping track of all the filesystem shutdown hooks that need
   * to be executed after the last regionserver referring to a given filesystem
   * stops. We keep track of the # of regionserver references in values of the map.
   */
  private final static Map<Thread, Integer> fsShutdownHooks = new HashMap<Thread, Integer>();

  /**
   * Install a shutdown hook that calls stop on the passed Stoppable
   * and then thread joins against the passed <code>threadToJoin</code>.
   * When this thread completes, it then runs the hdfs thread (This install
   * removes the hdfs shutdown hook keeping a handle on it to run it after
   * <code>threadToJoin</code> has stopped).
   *
   * <p>To suppress all shutdown hook  handling -- both the running of the
   * regionserver hook and of the hdfs hook code -- set
   * {@link ShutdownHook#RUN_SHUTDOWN_HOOK} in {@link Configuration} to
   * <code>false</code>.
   * This configuration value is checked when the hook code runs.
   * @param conf
   * @param fs Instance of Filesystem used by the RegionServer
   * @param stop Installed shutdown hook will call stop against this passed
   * <code>Stoppable</code> instance.
   * @param threadToJoin After calling stop on <code>stop</code> will then
   * join this thread.
   */
  public static void install(final Configuration conf, final FileSystem fs,
      final Stoppable stop, final Thread threadToJoin) {
    Thread fsShutdownHook = suppressHdfsShutdownHook(fs);
    Thread t = new ShutdownHookThread(conf, stop, threadToJoin, fsShutdownHook);
    Runtime.getRuntime().addShutdownHook(t);
    LOG.info("Installed shutdown hook thread: " + t.getName());
  }

  /*
   * Thread run by shutdown hook.
   */
  private static class ShutdownHookThread extends Thread {
    private final Stoppable stop;
    private final Thread threadToJoin;
    private final Thread fsShutdownHook;
    private final Configuration conf;

    ShutdownHookThread(final Configuration conf, final Stoppable stop,
        final Thread threadToJoin, final Thread fsShutdownHook) {
      super("Shutdownhook:" + threadToJoin.getName());
      this.stop = stop;
      this.threadToJoin = threadToJoin;
      this.conf = conf;
      this.fsShutdownHook = fsShutdownHook;
    }

    @Override
    public void run() {
      boolean b = this.conf.getBoolean(RUN_SHUTDOWN_HOOK, true);
      LOG.info("Shutdown hook starting; " + RUN_SHUTDOWN_HOOK + "=" + b +
        "; fsShutdownHook=" + this.fsShutdownHook);
      if (b) {
        this.stop.stop("Shutdown hook");
        Threads.shutdown(this.threadToJoin);
        if (this.fsShutdownHook != null) {
          synchronized (fsShutdownHooks) {
            int refs = fsShutdownHooks.get(fsShutdownHook);
            if (refs == 1) {
              LOG.info("Starting fs shutdown hook thread.");
              this.fsShutdownHook.start();
              Threads.shutdown(this.fsShutdownHook,
              this.conf.getLong(FS_SHUTDOWN_HOOK_WAIT, 30000));
            }
            if (refs > 0) {
              fsShutdownHooks.put(fsShutdownHook, refs - 1);
            }
          }
        }
      }
      LOG.info("Shutdown hook finished.");
    }
  }

  /*
   * So, HDFS keeps a static map of all FS instances. In order to make sure
   * things are cleaned up on our way out, it also creates a shutdown hook
   * so that all filesystems can be closed when the process is terminated; it
   * calls FileSystem.closeAll. This inconveniently runs concurrently with our
   * own shutdown handler, and therefore causes all the filesystems to be closed
   * before the server can do all its necessary cleanup.
   *
   * <p>The dirty reflection in this method sneaks into the FileSystem class
   * and grabs the shutdown hook, removes it from the list of active shutdown
   * hooks, and returns the hook for the caller to run at its convenience.
   *
   * <p>This seems quite fragile and susceptible to breaking if Hadoop changes
   * anything about the way this cleanup is managed. Keep an eye on things.
   * @return The fs shutdown hook
   * @throws RuntimeException if we fail to find or grap the shutdown hook.
   */
  private static Thread suppressHdfsShutdownHook(final FileSystem fs) {
    try {
      // This introspection has been updated to work for hadoop 0.20, 0.21 and for
      // cloudera 0.20.  0.21 and cloudera 0.20 both have hadoop-4829.  With the
      // latter in place, things are a little messy in that there are now two
      // instances of the data member clientFinalizer; an uninstalled one in
      // FileSystem and one in the innner class named Cache that actually gets
      // registered as a shutdown hook.  If the latter is present, then we are
      // on 0.21 or cloudera patched 0.20.
      Thread hdfsClientFinalizer = null;
      // Look into the FileSystem#Cache class for clientFinalizer
      Class<?> [] classes = FileSystem.class.getDeclaredClasses();
      Class<?> cache = null;
      for (Class<?> c: classes) {
        if (c.getSimpleName().equals("Cache")) {
          cache = c;
          break;
        }
      }
      Field field = null;
      try {
        field = cache.getDeclaredField(CLIENT_FINALIZER_DATA_METHOD);
      } catch (NoSuchFieldException e) {
        // We can get here if the Cache class does not have a clientFinalizer
        // instance: i.e. we're running on straight 0.20 w/o hadoop-4829.
      }
      if (field != null) {
        field.setAccessible(true);
        Field cacheField = FileSystem.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
        Object cacheInstance = cacheField.get(fs);
        hdfsClientFinalizer = (Thread)field.get(cacheInstance);
      } else {
        // Then we didnt' find clientFinalizer in Cache.  Presume clean 0.20 hadoop.
        field = FileSystem.class.getDeclaredField(CLIENT_FINALIZER_DATA_METHOD);
        field.setAccessible(true);
        hdfsClientFinalizer = (Thread)field.get(null);
      }
      if (hdfsClientFinalizer == null) {
        throw new RuntimeException("Client finalizer is null, can't suppress!");
      }
      if (!fsShutdownHooks.containsKey(hdfsClientFinalizer) &&
          !Runtime.getRuntime().removeShutdownHook(hdfsClientFinalizer)) {
        throw new RuntimeException("Failed suppression of fs shutdown hook: " +
          hdfsClientFinalizer);
      }
      synchronized (fsShutdownHooks) {
        Integer refs = fsShutdownHooks.get(hdfsClientFinalizer);
        fsShutdownHooks.put(hdfsClientFinalizer, refs == null ? 1 : refs + 1);
      }
      return hdfsClientFinalizer;
    } catch (NoSuchFieldException nsfe) {
      LOG.fatal("Couldn't find field 'clientFinalizer' in FileSystem!", nsfe);
      throw new RuntimeException("Failed to suppress HDFS shutdown hook");
    } catch (IllegalAccessException iae) {
      LOG.fatal("Couldn't access field 'clientFinalizer' in FileSystem!", iae);
      throw new RuntimeException("Failed to suppress HDFS shutdown hook");
    }
  }

  // Thread that does nothing. Used in below main testing.
  static class DoNothingThread extends Thread {
    DoNothingThread() {
      super("donothing");
    }
    @Override
    public void run() {
      super.run();
    }
  }

  // Stoppable with nothing to stop.  Used below in main testing.
  static class DoNothingStoppable implements Stoppable {
    @Override
    public boolean isStopped() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void stop(String why) {
      // TODO Auto-generated method stub
    }
  }

  /**
   * Main to test basic functionality.  Run with clean hadoop 0.20 and hadoop
   * 0.21 and cloudera patched hadoop to make sure our shutdown hook handling
   * works for all compbinations.
   * Pass '-Dhbase.shutdown.hook=false' to test turning off the running of
   * shutdown hooks.
   * @param args
   * @throws IOException
   */
  public static void main(final String [] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String prop = System.getProperty(RUN_SHUTDOWN_HOOK);
    if (prop != null) {
      conf.setBoolean(RUN_SHUTDOWN_HOOK, Boolean.parseBoolean(prop));
    }
    // Instantiate a FileSystem. This will register the fs shutdown hook.
    FileSystem fs = FileSystem.get(conf);
    Thread donothing = new DoNothingThread();
    donothing.start();
    ShutdownHook.install(conf, fs, new DoNothingStoppable(), donothing);
  }
}
