/*
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SortedCopyOnWriteSet;
import org.apache.hadoop.hbase.util.VersionInfo;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Provides the common setup framework and runtime services for coprocessor
 * invocation from HBase services.
 * @param <E> the specific environment extension that a concrete implementation
 * provides
 */
public abstract class CoprocessorHost<E extends CoprocessorEnvironment> {
  public static final String REGION_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.region.classes";
  public static final String MASTER_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.master.classes";
  public static final String WAL_COPROCESSOR_CONF_KEY =
    "hbase.coprocessor.wal.classes";

  private static final Log LOG = LogFactory.getLog(CoprocessorHost.class);
  /** Ordered set of loaded coprocessors with lock */
  protected SortedSet<E> coprocessors =
      new SortedCopyOnWriteSet<E>(new EnvironmentPriorityComparator());
  protected Configuration conf;
  // unique file prefix to use for local copies of jars when classloading
  protected String pathPrefix;
  protected volatile int loadSequence;

  public CoprocessorHost() {
    pathPrefix = UUID.randomUUID().toString();
  }

  /**
   * Load system coprocessors. Read the class names from configuration.
   * Called by constructor.
   */
  protected void loadSystemCoprocessors(Configuration conf, String confKey) {
    Class<?> implClass = null;

    // load default coprocessors from configure file
    String defaultCPClasses = conf.get(confKey);
    if (defaultCPClasses == null || defaultCPClasses.length() == 0)
      return;
    StringTokenizer st = new StringTokenizer(defaultCPClasses, ",");
    int priority = Coprocessor.PRIORITY_SYSTEM;
    List<E> configured = new ArrayList<E>();
    while (st.hasMoreTokens()) {
      String className = st.nextToken();
      if (findCoprocessor(className) != null) {
        continue;
      }
      ClassLoader cl = this.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(cl);
      try {
        implClass = cl.loadClass(className);
        configured.add(loadInstance(implClass, Coprocessor.PRIORITY_SYSTEM, conf));
        LOG.info("System coprocessor " + className + " was loaded " +
            "successfully with priority (" + priority++ + ").");
      } catch (ClassNotFoundException e) {
        LOG.warn("Class " + className + " cannot be found. " +
            e.getMessage());
      } catch (IOException e) {
        LOG.warn("Load coprocessor " + className + " failed. " +
            e.getMessage());
      }
    }

    // add entire set to the collection for COW efficiency
    coprocessors.addAll(configured);
  }

  /**
   * Load a coprocessor implementation into the host
   * @param path path to implementation jar
   * @param className the main class name
   * @param priority chaining priority
   * @param conf configuration for coprocessor
   * @throws java.io.IOException Exception
   */
  @SuppressWarnings("deprecation")
  public E load(Path path, String className, int priority,
      Configuration conf) throws IOException {
    Class<?> implClass = null;

    // Have we already loaded the class, perhaps from an earlier region open
    // for the same table?
    try {
      implClass = getClass().getClassLoader().loadClass(className);
    } catch (ClassNotFoundException e) {
      LOG.info("Class " + className + " needs to be loaded from a file - " +
          path.toString() + ".");
      // go ahead to load from file system.
    }

    // If not, load
    if (implClass == null) {
      // copy the jar to the local filesystem
      if (!path.toString().endsWith(".jar")) {
        throw new IOException(path.toString() + ": not a jar file?");
      }
      FileSystem fs = path.getFileSystem(HBaseConfiguration.create());
      Path dst = new Path(System.getProperty("java.io.tmpdir") +
          java.io.File.separator +"." + pathPrefix +
          "." + className + "." + System.currentTimeMillis() + ".jar");
      fs.copyToLocalFile(path, dst);
      fs.deleteOnExit(dst);

      // TODO: code weaving goes here

      // TODO: wrap heap allocations and enforce maximum usage limits

      /* TODO: inject code into loop headers that monitors CPU use and
         aborts runaway user code */

      // load the jar and get the implementation main class
      String cp = System.getProperty("java.class.path");
      // NOTE: Path.toURL is deprecated (toURI instead) but the URLClassLoader
      // unsuprisingly wants URLs, not URIs; so we will use the deprecated
      // method which returns URLs for as long as it is available
      List<URL> paths = new ArrayList<URL>();
      paths.add(new File(dst.toString()).getCanonicalFile().toURL());
      StringTokenizer st = new StringTokenizer(cp, File.pathSeparator);
      while (st.hasMoreTokens()) {
        paths.add((new File(st.nextToken())).getCanonicalFile().toURL());
      }
      ClassLoader cl = new URLClassLoader(paths.toArray(new URL[]{}),
        this.getClass().getClassLoader());
      Thread.currentThread().setContextClassLoader(cl);
      try {
        implClass = cl.loadClass(className);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }

    return loadInstance(implClass, priority, conf);
  }

  /**
   * @param implClass Implementation class
   * @param priority priority
   * @param conf configuration
   * @throws java.io.IOException Exception
   */
  public void load(Class<?> implClass, int priority, Configuration conf)
      throws IOException {
    E env = loadInstance(implClass, priority, conf);
    coprocessors.add(env);
  }

  /**
   * @param implClass Implementation class
   * @param priority priority
   * @param conf configuration
   * @throws java.io.IOException Exception
   */
  public E loadInstance(Class<?> implClass, int priority, Configuration conf)
      throws IOException {
    // create the instance
    Coprocessor impl;
    Object o = null;
    try {
      o = implClass.newInstance();
      impl = (Coprocessor)o;
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
    // create the environment
    E env = createEnvironment(implClass, impl, priority, ++loadSequence, conf);
    if (env instanceof Environment) {
      ((Environment)env).startup();
    }
    return env;
  }

  /**
   * Called when a new Coprocessor class is loaded
   */
  public abstract E createEnvironment(Class<?> implClass, Coprocessor instance,
      int priority, int sequence, Configuration conf);

  public void shutdown(CoprocessorEnvironment e) {
    if (e instanceof Environment) {
      ((Environment)e).shutdown();
    } else {
      LOG.warn("Shutdown called on unknown environment: "+
          e.getClass().getName());
    }
  }

  /**
   * Find a coprocessor implementation by class name
   * @param className the class name
   * @return the coprocessor, or null if not found
   */
  public Coprocessor findCoprocessor(String className) {
    // initialize the coprocessors
    for (E env: coprocessors) {
      if (env.getInstance().getClass().getName().equals(className) ||
          env.getInstance().getClass().getSimpleName().equals(className)) {
        return env.getInstance();
      }
    }
    return null;
  }

  /**
   * Find a coprocessor environment by class name
   * @param className the class name
   * @return the coprocessor, or null if not found
   */
  public CoprocessorEnvironment findCoprocessorEnvironment(String className) {
    // initialize the coprocessors
    for (E env: coprocessors) {
      if (env.getInstance().getClass().getName().equals(className) ||
          env.getInstance().getClass().getSimpleName().equals(className)) {
        return env;
      }
    }
    return null;
  }

  /**
   * Environment priority comparator.
   * Coprocessors are chained in sorted order.
   */
  static class EnvironmentPriorityComparator
      implements Comparator<CoprocessorEnvironment> {
    public int compare(final CoprocessorEnvironment env1,
        final CoprocessorEnvironment env2) {
      if (env1.getPriority() < env2.getPriority()) {
        return -1;
      } else if (env1.getPriority() > env2.getPriority()) {
        return 1;
      }
      if (env1.getLoadSequence() < env2.getLoadSequence()) {
        return -1;
      } else if (env1.getLoadSequence() > env2.getLoadSequence()) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Encapsulation of the environment of each coprocessor
   */
  public static class Environment implements CoprocessorEnvironment {

    /**
     * A wrapper for HTable. Can be used to restrict privilege.
     *
     * Currently it just helps to track tables opened by a Coprocessor and
     * facilitate close of them if it is aborted.
     *
     * We also disallow row locking.
     *
     * There is nothing now that will stop a coprocessor from using HTable
     * objects directly instead of this API, but in the future we intend to
     * analyze coprocessor implementations as they are loaded and reject those
     * which attempt to use objects and methods outside the Environment
     * sandbox.
     */
    class HTableWrapper implements HTableInterface {

      private byte[] tableName;
      private HTable table;

      public HTableWrapper(byte[] tableName) throws IOException {
        this.tableName = tableName;
        this.table = new HTable(tableName);
        openTables.add(this);
      }

      void internalClose() throws IOException {
        table.close();
      }

      public Configuration getConfiguration() {
        return table.getConfiguration();
      }

      public void close() throws IOException {
        try {
          internalClose();
        } finally {
          openTables.remove(this);
        }
      }

      public Result getRowOrBefore(byte[] row, byte[] family)
          throws IOException {
        return table.getRowOrBefore(row, family);
      }

      public Result get(Get get) throws IOException {
        return table.get(get);
      }

      public boolean exists(Get get) throws IOException {
        return table.exists(get);
      }

      public void put(Put put) throws IOException {
        table.put(put);
      }

      public void put(List<Put> puts) throws IOException {
        table.put(puts);
      }

      public void delete(Delete delete) throws IOException {
        table.delete(delete);
      }

      public void delete(List<Delete> deletes) throws IOException {
        table.delete(deletes);
      }

      public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
          byte[] value, Put put) throws IOException {
        return table.checkAndPut(row, family, qualifier, value, put);
      }

      public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
          byte[] value, Delete delete) throws IOException {
        return table.checkAndDelete(row, family, qualifier, value, delete);
      }

      public long incrementColumnValue(byte[] row, byte[] family,
          byte[] qualifier, long amount) throws IOException {
        return table.incrementColumnValue(row, family, qualifier, amount);
      }

      public long incrementColumnValue(byte[] row, byte[] family,
          byte[] qualifier, long amount, boolean writeToWAL)
          throws IOException {
        return table.incrementColumnValue(row, family, qualifier, amount,
          writeToWAL);
      }

      @Override
      public Result increment(Increment increment) throws IOException {
        return table.increment(increment);
      }

      public void flushCommits() throws IOException {
        table.flushCommits();
      }

      public boolean isAutoFlush() {
        return table.isAutoFlush();
      }

      public ResultScanner getScanner(Scan scan) throws IOException {
        return table.getScanner(scan);
      }

      public ResultScanner getScanner(byte[] family) throws IOException {
        return table.getScanner(family);
      }

      public ResultScanner getScanner(byte[] family, byte[] qualifier)
          throws IOException {
        return table.getScanner(family, qualifier);
      }

      public HTableDescriptor getTableDescriptor() throws IOException {
        return table.getTableDescriptor();
      }

      public byte[] getTableName() {
        return tableName;
      }

      public RowLock lockRow(byte[] row) throws IOException {
        throw new RuntimeException(
          "row locking is not allowed within the coprocessor environment");
      }

      public void unlockRow(RowLock rl) throws IOException {
        throw new RuntimeException(
          "row locking is not allowed within the coprocessor environment");
      }

      @Override
      public void batch(List<Row> actions, Object[] results)
          throws IOException, InterruptedException {
        table.batch(actions, results);
      }

      @Override
      public Object[] batch(List<Row> actions)
          throws IOException, InterruptedException {
        return table.batch(actions);
      }

      @Override
      public Result[] get(List<Get> gets) throws IOException {
        return table.get(gets);
      }

      @Override
      public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol,
          byte[] startKey, byte[] endKey, Batch.Call<T, R> callable,
          Batch.Callback<R> callback) throws IOException, Throwable {
        table.coprocessorExec(protocol, startKey, endKey, callable, callback);
      }

      @Override
      public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
          Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable)
          throws IOException, Throwable {
        return table.coprocessorExec(protocol, startKey, endKey, callable);
      }

      @Override
      public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol,
          byte[] row) {
        return table.coprocessorProxy(protocol, row);
      }
    }

    /** The coprocessor */
    public Coprocessor impl;
    /** Chaining priority */
    protected int priority = Coprocessor.PRIORITY_USER;
    /** Current coprocessor state */
    Coprocessor.State state = Coprocessor.State.UNINSTALLED;
    /** Accounting for tables opened by the coprocessor */
    protected List<HTableInterface> openTables =
      Collections.synchronizedList(new ArrayList<HTableInterface>());
    private int seq;
    private Configuration conf;

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public Environment(final Coprocessor impl, final int priority,
        final int seq, final Configuration conf) {
      this.impl = impl;
      this.priority = priority;
      this.state = Coprocessor.State.INSTALLED;
      this.seq = seq;
      this.conf = conf;
    }

    /** Initialize the environment */
    public void startup() {
      if (state == Coprocessor.State.INSTALLED ||
          state == Coprocessor.State.STOPPED) {
        state = Coprocessor.State.STARTING;
        try {
          impl.start(this);
          state = Coprocessor.State.ACTIVE;
        } catch (IOException ioe) {
          LOG.error("Error starting coprocessor "+impl.getClass().getName(), ioe);
        }
      } else {
        LOG.warn("Not starting coprocessor "+impl.getClass().getName()+
            " because not inactive (state="+state.toString()+")");
      }
    }

    /** Clean up the environment */
    protected void shutdown() {
      if (state == Coprocessor.State.ACTIVE) {
        state = Coprocessor.State.STOPPING;
        try {
          impl.stop(this);
          state = Coprocessor.State.STOPPED;
        } catch (IOException ioe) {
          LOG.error("Error stopping coprocessor "+impl.getClass().getName(), ioe);
        }
      } else {
        LOG.warn("Not stopping coprocessor "+impl.getClass().getName()+
            " because not active (state="+state.toString()+")");
      }
      // clean up any table references
      for (HTableInterface table: openTables) {
        try {
          ((HTableWrapper)table).internalClose();
        } catch (IOException e) {
          // nothing can be done here
          LOG.warn("Failed to close " +
              Bytes.toStringBinary(table.getTableName()), e);
        }
      }
    }

    @Override
    public Coprocessor getInstance() {
      return impl;
    }

    @Override
    public int getPriority() {
      return priority;
    }

    @Override
    public int getLoadSequence() {
      return seq;
    }

    /** @return the coprocessor environment version */
    @Override
    public int getVersion() {
      return Coprocessor.VERSION;
    }

    /** @return the HBase release */
    @Override
    public String getHBaseVersion() {
      return VersionInfo.getVersion();
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    /**
     * Open a table from within the Coprocessor environment
     * @param tableName the table name
     * @return an interface for manipulating the table
     * @exception java.io.IOException Exception
     */
    @Override
    public HTableInterface getTable(byte[] tableName) throws IOException {
      return new HTableWrapper(tableName);
    }
  }
}
