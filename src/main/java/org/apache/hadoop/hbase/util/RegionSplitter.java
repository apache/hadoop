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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * The {@link RegionSplitter} class provides several utilities to help in the
 * administration lifecycle for developers who choose to manually split regions
 * instead of having HBase handle that automatically. The most useful utilities
 * are:
 * <p>
 * <ul>
 * <li>Create a table with a specified number of pre-split regions
 * <li>Execute a rolling split of all regions on an existing table
 * </ul>
 * <p>
 * Both operations can be safely done on a live server.
 * <p>
 * <b>Question:</b> How do I turn off automatic splitting? <br>
 * <b>Answer:</b> Automatic splitting is determined by the configuration value
 * <i>"hbase.hregion.max.filesize"</i>. It is not recommended that you set this
 * to Long.MAX_VALUE in case you forget about manual splits. A suggested setting
 * is 100GB, which would result in > 1hr major compactions if reached.
 * <p>
 * <b>Question:</b> Why did the original authors decide to manually split? <br>
 * <b>Answer:</b> Specific workload characteristics of our use case allowed us
 * to benefit from a manual split system.
 * <p>
 * <ul>
 * <li>Data (~1k) that would grow instead of being replaced
 * <li>Data growth was roughly uniform across all regions
 * <li>OLTP workload. Data loss is a big deal.
 * </ul>
 * <p>
 * <b>Question:</b> Why is manual splitting good for this workload? <br>
 * <b>Answer:</b> Although automated splitting is not a bad option, there are
 * benefits to manual splitting.
 * <p>
 * <ul>
 * <li>With growing amounts of data, splits will continually be needed. Since
 * you always know exactly what regions you have, long-term debugging and
 * profiling is much easier with manual splits. It is hard to trace the logs to
 * understand region level problems if it keeps splitting and getting renamed.
 * <li>Data offlining bugs + unknown number of split regions == oh crap! If an
 * HLog or StoreFile was mistakenly unprocessed by HBase due to a weird bug and
 * you notice it a day or so later, you can be assured that the regions
 * specified in these files are the same as the current regions and you have
 * less headaches trying to restore/replay your data.
 * <li>You can finely tune your compaction algorithm. With roughly uniform data
 * growth, it's easy to cause split / compaction storms as the regions all
 * roughly hit the same data size at the same time. With manual splits, you can
 * let staggered, time-based major compactions spread out your network IO load.
 * </ul>
 * <p>
 * <b>Question:</b> What's the optimal number of pre-split regions to create? <br>
 * <b>Answer:</b> Mileage will vary depending upon your application.
 * <p>
 * The short answer for our application is that we started with 10 pre-split
 * regions / server and watched our data growth over time. It's better to err on
 * the side of too little regions and rolling split later.
 * <p>
 * The more complicated answer is that this depends upon the largest storefile
 * in your region. With a growing data size, this will get larger over time. You
 * want the largest region to be just big enough that the {@link Store} compact
 * selection algorithm only compacts it due to a timed major. If you don't, your
 * cluster can be prone to compaction storms as the algorithm decides to run
 * major compactions on a large series of regions all at once. Note that
 * compaction storms are due to the uniform data growth, not the manual split
 * decision.
 * <p>
 * If you pre-split your regions too thin, you can increase the major compaction
 * interval by configuring HConstants.MAJOR_COMPACTION_PERIOD. If your data size
 * grows too large, use this script to perform a network IO safe rolling split
 * of all regions.
 */
public class RegionSplitter {
  static final Log LOG = LogFactory.getLog(RegionSplitter.class);

  /**
   * A generic interface for the RegionSplitter code to use for all it's
   * functionality. Note that the original authors of this code use
   * {@link MD5StringSplit} to partition their table and set it as default, but
   * provided this for your custom algorithm. To use, create a new derived class
   * from this interface and call the RegionSplitter class with the argument: <br>
   * <b>-D split.algorithm=<your_class_path></b>
   */
  public static interface SplitAlgorithm {
    /**
     * Split a pre-existing region into 2 regions.
     *
     * @param start
     *          row
     * @param end
     *          row
     * @return the split row to use
     */
    byte[] split(byte[] start, byte[] end);

    /**
     * Split an entire table.
     *
     * @param numberOfSplits
     *          number of regions to split the table into
     *
     * @return array of split keys for the initial regions of the table
     */
    byte[][] split(int numberOfSplits);

    /**
     * In HBase, the first row is represented by an empty byte array. This might
     * cause problems with your split algorithm or row printing. All your APIs
     * will be passed firstRow() instead of empty array.
     *
     * @return your representation of your first row
     */
    byte[] firstRow();

    /**
     * In HBase, the last row is represented by an empty byte array. This might
     * cause problems with your split algorithm or row printing. All your APIs
     * will be passed firstRow() instead of empty array.
     *
     * @return your representation of your last row
     */
    byte[] lastRow();

    /**
     * @param input
     *          user or file input for row
     * @return byte array representation of this row for HBase
     */
    byte[] strToRow(String input);

    /**
     * @param row
     *          byte array representing a row in HBase
     * @return String to use for debug & file printing
     */
    String rowToStr(byte[] row);

    /**
     * @return the separator character to use when storing / printing the row
     */
    String separator();
  }

  /**
   * The main function for the RegionSplitter application. Common uses:
   * <p>
   * <ul>
   * <li>create a table named 'myTable' with 60 pre-split regions containing 2
   * column families 'test' & 'rs' bin/hbase
   * <ul>
   * <li>org.apache.hadoop.hbase.util.RegionSplitter -c 60 -f test:rs myTable
   * </ul>
   * <li>perform a rolling split of 'myTable' (i.e. 60 => 120 regions), # 2
   * outstanding splits at a time bin/hbase
   * <ul>
   * <li>org.apache.hadoop.hbase.util.RegionSplitter -r -o 2 myTable
   * </ul>
   * </ul>
   *
   * @param args
   *          Usage: RegionSplitter &lt;TABLE&gt; &lt;-c &lt;# regions&gt; -f
   *          &lt;family:family:...&gt; | -r [-o &lt;# outstanding
   *          splits&gt;]&gt; [-D &lt;conf.param=value&gt;]
   * @throws IOException
   *           HBase IO problem
   * @throws InterruptedException
   *           user requested exit
   * @throws ParseException
   *           problem parsing user input
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException,
      InterruptedException, ParseException {
    Configuration conf = HBaseConfiguration.create();

    // parse user input
    Options opt = new Options();
    opt.addOption(OptionBuilder.withArgName("property=value").hasArg()
        .withDescription("Override HBase Configuration Settings").create("D"));
    opt.addOption(OptionBuilder.withArgName("region count").hasArg()
        .withDescription(
            "Create a new table with a pre-split number of regions")
        .create("c"));
    opt.addOption(OptionBuilder.withArgName("family:family:...").hasArg()
        .withDescription(
            "Column Families to create with new table.  Required with -c")
        .create("f"));
    opt.addOption("h", false, "Print this usage help");
    opt.addOption("r", false, "Perform a rolling split of an existing region");
    opt.addOption(OptionBuilder.withArgName("count").hasArg().withDescription(
        "Max outstanding splits that have unfinished major compactions")
        .create("o"));
    opt.addOption(null, "risky", false,
        "Skip verification steps to complete quickly."
            + "STRONGLY DISCOURAGED for production systems.  ");
    CommandLine cmd = new GnuParser().parse(opt, args);

    if (cmd.hasOption("D")) {
      for (String confOpt : cmd.getOptionValues("D")) {
        String[] kv = confOpt.split("=", 2);
        if (kv.length == 2) {
          conf.set(kv[0], kv[1]);
          LOG.debug("-D configuration override: " + kv[0] + "=" + kv[1]);
        } else {
          throw new ParseException("-D option format invalid: " + confOpt);
        }
      }
    }

    if (cmd.hasOption("risky")) {
      conf.setBoolean("split.verify", false);
    }

    boolean createTable = cmd.hasOption("c") && cmd.hasOption("f");
    boolean rollingSplit = cmd.hasOption("r");
    boolean oneOperOnly = createTable ^ rollingSplit;

    if (1 != cmd.getArgList().size() || !oneOperOnly || cmd.hasOption("h")) {
      new HelpFormatter().printHelp("RegionSplitter <TABLE>", opt);
      return;
    }
    String tableName = cmd.getArgs()[0];

    if (createTable) {
      conf.set("split.count", cmd.getOptionValue("c"));
      createPresplitTable(tableName, cmd.getOptionValue("f").split(":"), conf);
    }

    if (rollingSplit) {
      if (cmd.hasOption("o")) {
        conf.set("split.outstanding", cmd.getOptionValue("o"));
      }
      rollingSplit(tableName, conf);
    }
  }

  static void createPresplitTable(String tableName, String[] columnFamilies,
      Configuration conf) throws IOException, InterruptedException {
    Class<? extends SplitAlgorithm> splitClass = conf.getClass(
        "split.algorithm", MD5StringSplit.class, SplitAlgorithm.class);
    SplitAlgorithm splitAlgo;
    try {
      splitAlgo = splitClass.newInstance();
    } catch (Exception e) {
      throw new IOException("Problem loading split algorithm: ", e);
    }
    final int splitCount = conf.getInt("split.count", 0);
    Preconditions.checkArgument(splitCount > 1, "Split count must be > 1");

    Preconditions.checkArgument(columnFamilies.length > 0,
        "Must specify at least one column family. ");
    LOG.debug("Creating table " + tableName + " with " + columnFamilies.length
        + " column families.  Presplitting to " + splitCount + " regions");

    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (String cf : columnFamilies) {
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes(cf)));
    }
    HBaseAdmin admin = new HBaseAdmin(conf);
    Preconditions.checkArgument(!admin.tableExists(tableName),
        "Table already exists: " + tableName);
    admin.createTable(desc, splitAlgo.split(splitCount));
    LOG.debug("Table created!  Waiting for regions to show online in META...");

    if (!conf.getBoolean("split.verify", true)) {
      // NOTE: createTable is synchronous on the table, but not on the regions
      HTable table = new HTable(tableName);
      int onlineRegions = 0;
      while (onlineRegions < splitCount) {
        onlineRegions = table.getRegionsInfo().size();
        LOG.debug(onlineRegions + " of " + splitCount + " regions online...");
        if (onlineRegions < splitCount) {
          Thread.sleep(10 * 1000); // sleep
        }
      }
    }

    LOG.debug("Finished creating table with " + splitCount + " regions");
  }

  static void rollingSplit(String tableName, Configuration conf)
      throws IOException, InterruptedException {
    Class<? extends SplitAlgorithm> splitClass = conf.getClass(
        "split.algorithm", MD5StringSplit.class, SplitAlgorithm.class);
    SplitAlgorithm splitAlgo;
    try {
      splitAlgo = splitClass.newInstance();
    } catch (Exception e) {
      throw new IOException("Problem loading split algorithm: ", e);
    }
    final int minOS = conf.getInt("split.outstanding", 2);

    HTable table = new HTable(conf, tableName);

    // max outstanding splits. default == 50% of servers
    final int MAX_OUTSTANDING =
        Math.max(table.getConnection().getCurrentNrHRS() / 2, minOS);

    Path hbDir = new Path(conf.get(HConstants.HBASE_DIR));
    Path tableDir = HTableDescriptor.getTableDir(hbDir, table.getTableName());
    Path splitFile = new Path(tableDir, "_balancedSplit");
    FileSystem fs = FileSystem.get(conf);

    // get a list of daughter regions to create
    LinkedList<Pair<byte[], byte[]>> tmpRegionSet = getSplits(table, splitAlgo);
    LinkedList<Pair<byte[], byte[]>> outstanding = Lists.newLinkedList();
    int splitCount = 0;
    final int origCount = tmpRegionSet.size();

    // all splits must compact & we have 1 compact thread, so 2 split
    // requests to the same RS can stall the outstanding split queue.
    // To fix, group the regions into an RS pool and round-robin through it
    LOG.debug("Bucketing regions by regionserver...");
    TreeMap<HServerAddress, LinkedList<Pair<byte[], byte[]>>> daughterRegions = Maps
        .newTreeMap();
    for (Pair<byte[], byte[]> dr : tmpRegionSet) {
      HServerAddress rsLocation = table.getRegionLocation(dr.getSecond())
          .getServerAddress();
      if (!daughterRegions.containsKey(rsLocation)) {
        LinkedList<Pair<byte[], byte[]>> entry = Lists.newLinkedList();
        daughterRegions.put(rsLocation, entry);
      }
      daughterRegions.get(rsLocation).add(dr);
    }
    LOG.debug("Done with bucketing.  Split time!");
    long startTime = System.currentTimeMillis();

    // open the split file and modify it as splits finish
    FSDataInputStream tmpIn = fs.open(splitFile);
    byte[] rawData = new byte[tmpIn.available()];
    tmpIn.readFully(rawData);
    tmpIn.close();
    FSDataOutputStream splitOut = fs.create(splitFile);
    splitOut.write(rawData);

    try {
      // *** split code ***
      while (!daughterRegions.isEmpty()) {
        LOG.debug(daughterRegions.size() + " RS have regions to splt.");

        // round-robin through the RS list
        for (HServerAddress rsLoc = daughterRegions.firstKey();
             rsLoc != null;
             rsLoc = daughterRegions.higherKey(rsLoc)) {
          Pair<byte[], byte[]> dr = null;

          // find a region in the RS list that hasn't been moved
          LOG.debug("Finding a region on " + rsLoc);
          LinkedList<Pair<byte[], byte[]>> regionList = daughterRegions
              .get(rsLoc);
          while (!regionList.isEmpty()) {
            dr = regionList.pop();

            // get current region info
            byte[] split = dr.getSecond();
            HRegionLocation regionLoc = table.getRegionLocation(split);

            // if this region moved locations
            HServerAddress newRs = regionLoc.getServerAddress();
            if (newRs.compareTo(rsLoc) != 0) {
              LOG.debug("Region with " + splitAlgo.rowToStr(split)
                  + " moved to " + newRs + ". Relocating...");
              // relocate it, don't use it right now
              if (!daughterRegions.containsKey(newRs)) {
                LinkedList<Pair<byte[], byte[]>> entry = Lists.newLinkedList();
                daughterRegions.put(newRs, entry);
              }
              daughterRegions.get(newRs).add(dr);
              dr = null;
              continue;
            }

            // make sure this region wasn't already split
            byte[] sk = regionLoc.getRegionInfo().getStartKey();
            if (sk.length != 0) {
              if (Bytes.equals(split, sk)) {
                LOG.debug("Region already split on "
                    + splitAlgo.rowToStr(split) + ".  Skipping this region...");
                ++splitCount;
                dr = null;
                continue;
              }
              byte[] start = dr.getFirst();
              Preconditions.checkArgument(Bytes.equals(start, sk), splitAlgo
                  .rowToStr(start) + " != " + splitAlgo.rowToStr(sk));
            }

            // passed all checks! found a good region
            break;
          }
          if (regionList.isEmpty()) {
            daughterRegions.remove(rsLoc);
          }
          if (dr == null)
            continue;

          // we have a good region, time to split!
          byte[] split = dr.getSecond();
          LOG.debug("Splitting at " + splitAlgo.rowToStr(split));
          HBaseAdmin admin = new HBaseAdmin(table.getConfiguration());
          admin.split(table.getTableName(), split);

          LinkedList<Pair<byte[], byte[]>> finished = Lists.newLinkedList();
          if (conf.getBoolean("split.verify", true)) {
            // we need to verify and rate-limit our splits
            outstanding.addLast(dr);
            // with too many outstanding splits, wait for some to finish
            while (outstanding.size() >= MAX_OUTSTANDING) {
              finished = splitScan(outstanding, table, splitAlgo);
              if (finished.isEmpty()) {
                Thread.sleep(30 * 1000);
              } else {
                outstanding.removeAll(finished);
              }
            }
          } else {
            finished.add(dr);
          }

          // mark each finished region as successfully split.
          for (Pair<byte[], byte[]> region : finished) {
            splitOut.writeChars("- " + splitAlgo.rowToStr(region.getFirst())
                + " " + splitAlgo.rowToStr(region.getSecond()) + "\n");
            splitCount++;
            if (splitCount % 10 == 0) {
              long tDiff = (System.currentTimeMillis() - startTime)
                  / splitCount;
              LOG.debug("STATUS UPDATE: " + splitCount + " / " + origCount
                  + ". Avg Time / Split = "
                  + org.apache.hadoop.util.StringUtils.formatTime(tDiff));
            }
          }
        }
      }
      if (conf.getBoolean("split.verify", true)) {
        while (!outstanding.isEmpty()) {
          LinkedList<Pair<byte[], byte[]>> finished = splitScan(outstanding,
              table, splitAlgo);
          if (finished.isEmpty()) {
            Thread.sleep(30 * 1000);
          } else {
            outstanding.removeAll(finished);
            for (Pair<byte[], byte[]> region : finished) {
              splitOut.writeChars("- " + splitAlgo.rowToStr(region.getFirst())
                  + " " + splitAlgo.rowToStr(region.getSecond()) + "\n");
            }
          }
        }
      }
      LOG.debug("All regions have been sucesfully split!");
    } finally {
      long tDiff = System.currentTimeMillis() - startTime;
      LOG.debug("TOTAL TIME = "
          + org.apache.hadoop.util.StringUtils.formatTime(tDiff));
      LOG.debug("Splits = " + splitCount);
      LOG.debug("Avg Time / Split = "
          + org.apache.hadoop.util.StringUtils.formatTime(tDiff / splitCount));

      splitOut.close();
    }
    fs.delete(splitFile, false);
  }

  static LinkedList<Pair<byte[], byte[]>> splitScan(
      LinkedList<Pair<byte[], byte[]>> regionList, HTable table,
      SplitAlgorithm splitAlgo)
      throws IOException, InterruptedException {
    LinkedList<Pair<byte[], byte[]>> finished = Lists.newLinkedList();
    LinkedList<Pair<byte[], byte[]>> logicalSplitting = Lists.newLinkedList();
    LinkedList<Pair<byte[], byte[]>> physicalSplitting = Lists.newLinkedList();

    // get table info
    Path hbDir = new Path(table.getConfiguration().get(HConstants.HBASE_DIR));
    Path tableDir = HTableDescriptor.getTableDir(hbDir, table.getTableName());
    Path splitFile = new Path(tableDir, "_balancedSplit");
    FileSystem fs = FileSystem.get(table.getConfiguration());

    // clear the cache to forcibly refresh region information
    table.clearRegionCache();

    // for every region that hasn't been verified as a finished split
    for (Pair<byte[], byte[]> region : regionList) {
      byte[] start = region.getFirst();
      byte[] split = region.getSecond();

      // see if the new split daughter region has come online
      HRegionInfo dri = table.getRegionLocation(split).getRegionInfo();
      if (dri.isOffline() || !Bytes.equals(dri.getStartKey(), split)) {
        logicalSplitting.add(region);
        continue;
      }

      try {
        // when a daughter region is opened, a compaction is triggered
        // wait until compaction completes for both daughter regions
        LinkedList<HRegionInfo> check = Lists.newLinkedList();
        check.add(table.getRegionLocation(start).getRegionInfo());
        check.add(table.getRegionLocation(split).getRegionInfo());
        for (HRegionInfo hri : check.toArray(new HRegionInfo[] {})) {
          boolean refFound = false;
          byte[] sk = hri.getStartKey();
          if (sk.length == 0)
            sk = splitAlgo.firstRow();
          String startKey = splitAlgo.rowToStr(sk);
          HTableDescriptor htd = table.getTableDescriptor();
          // check every Column Family for that region
          for (HColumnDescriptor c : htd.getFamilies()) {
            Path cfDir = Store.getStoreHomedir(tableDir, hri.getEncodedName(),
                c.getName());
            if (fs.exists(cfDir)) {
              for (FileStatus file : fs.listStatus(cfDir)) {
                refFound |= StoreFile.isReference(file.getPath());
                if (refFound)
                  break;
              }
            }
            if (refFound)
              break;
          }
          // compaction is completed when all reference files are gone
          if (!refFound) {
            check.remove(hri);
          }
        }
        if (check.isEmpty()) {
          finished.add(region);
        } else {
          physicalSplitting.add(region);
        }
      } catch (NoServerForRegionException nsfre) {
        LOG.debug("No Server Exception thrown for: "
            + splitAlgo.rowToStr(start));
        physicalSplitting.add(region);
        table.clearRegionCache();
      }
    }

    LOG.debug("Split Scan: " + finished.size() + " finished / "
        + logicalSplitting.size() + " split wait / "
        + physicalSplitting.size() + " reference wait");

    return finished;
  }

  static LinkedList<Pair<byte[], byte[]>> getSplits(HTable table,
      SplitAlgorithm splitAlgo) throws IOException {
    Path hbDir = new Path(table.getConfiguration().get(HConstants.HBASE_DIR));
    Path tableDir = HTableDescriptor.getTableDir(hbDir, table.getTableName());
    Path splitFile = new Path(tableDir, "_balancedSplit");
    FileSystem fs = FileSystem.get(table.getConfiguration());

    // using strings because (new byte[]{0}).equals(new byte[]{0}) == false
    Set<Pair<String, String>> daughterRegions = Sets.newHashSet();

    // does a split file exist?
    if (!fs.exists(splitFile)) {
      // NO = fresh start. calculate splits to make
      LOG.debug("No _balancedSplit file.  Calculating splits...");

      // query meta for all regions in the table
      Set<Pair<byte[], byte[]>> rows = Sets.newHashSet();
      Pair<byte[][], byte[][]> tmp = table.getStartEndKeys();
      Preconditions.checkArgument(
          tmp.getFirst().length == tmp.getSecond().length,
          "Start and End rows should be equivalent");
      for (int i = 0; i < tmp.getFirst().length; ++i) {
        byte[] start = tmp.getFirst()[i], end = tmp.getSecond()[i];
        if (start.length == 0)
          start = splitAlgo.firstRow();
        if (end.length == 0)
          end = splitAlgo.lastRow();
        rows.add(Pair.newPair(start, end));
      }
      LOG.debug("Table " + Bytes.toString(table.getTableName()) + " has "
          + rows.size() + " regions that will be split.");

      // prepare the split file
      Path tmpFile = new Path(tableDir, "_balancedSplit_prepare");
      FSDataOutputStream tmpOut = fs.create(tmpFile);

      // calculate all the splits == [daughterRegions] = [(start, splitPoint)]
      for (Pair<byte[], byte[]> r : rows) {
        byte[] splitPoint = splitAlgo.split(r.getFirst(), r.getSecond());
        String startStr = splitAlgo.rowToStr(r.getFirst());
        String splitStr = splitAlgo.rowToStr(splitPoint);
        daughterRegions.add(Pair.newPair(startStr, splitStr));
        LOG.debug("Will Split [" + startStr + " , "
            + splitAlgo.rowToStr(r.getSecond()) + ") at " + splitStr);
        tmpOut.writeChars("+ " + startStr + splitAlgo.separator() + splitStr
            + "\n");
      }
      tmpOut.close();
      fs.rename(tmpFile, splitFile);
    } else {
      LOG.debug("_balancedSplit file found. Replay log to restore state...");
      FSUtils.getInstance(fs, table.getConfiguration())
        .recoverFileLease(fs, splitFile, table.getConfiguration());

      // parse split file and process remaining splits
      FSDataInputStream tmpIn = fs.open(splitFile);
      StringBuilder sb = new StringBuilder(tmpIn.available());
      while (tmpIn.available() > 0) {
        sb.append(tmpIn.readChar());
      }
      tmpIn.close();
      for (String line : sb.toString().split("\n")) {
        String[] cmd = line.split(splitAlgo.separator());
        Preconditions.checkArgument(3 == cmd.length);
        byte[] start = splitAlgo.strToRow(cmd[1]);
        String startStr = splitAlgo.rowToStr(start);
        byte[] splitPoint = splitAlgo.strToRow(cmd[2]);
        String splitStr = splitAlgo.rowToStr(splitPoint);
        Pair<String, String> r = Pair.newPair(startStr, splitStr);
        if (cmd[0].equals("+")) {
          LOG.debug("Adding: " + r);
          daughterRegions.add(r);
        } else {
          LOG.debug("Removing: " + r);
          Preconditions.checkArgument(cmd[0].equals("-"),
              "Unknown option: " + cmd[0]);
          Preconditions.checkState(daughterRegions.contains(r),
              "Missing row: " + r);
          daughterRegions.remove(r);
        }
      }
      LOG.debug("Done reading. " + daughterRegions.size() + " regions left.");
    }
    LinkedList<Pair<byte[], byte[]>> ret = Lists.newLinkedList();
    for (Pair<String, String> r : daughterRegions) {
      ret.add(Pair.newPair(splitAlgo.strToRow(r.getFirst()), splitAlgo
          .strToRow(r.getSecond())));
    }
    return ret;
  }

  /**
   * MD5StringSplit is the default {@link SplitAlgorithm} for creating pre-split
   * tables. The format of MD5StringSplit is the ASCII representation of an MD5
   * checksum. Row are long values in the range <b>"00000000" => "7FFFFFFF"</b>
   * and are left-padded with zeros to keep the same order lexographically as if
   * they were binary.
   */
  public static class MD5StringSplit implements SplitAlgorithm {
    final static String MAXMD5 = "7FFFFFFF";
    final static BigInteger MAXMD5_INT = new BigInteger(MAXMD5, 16);
    final static int rowComparisonLength = MAXMD5.length();

    public byte[] split(byte[] start, byte[] end) {
      BigInteger s = convertToBigInteger(start);
      BigInteger e = convertToBigInteger(end);
      Preconditions.checkArgument(!e.equals(BigInteger.ZERO));
      return convertToByte(split2(s, e));
    }

    public byte[][] split(int n) {
      BigInteger[] splits = new BigInteger[n - 1];
      BigInteger sizeOfEachSplit = MAXMD5_INT.divide(BigInteger.valueOf(n));
      for (int i = 1; i < n; i++) {
        // NOTE: this means the last region gets all the slop.
        // This is not a big deal if we're assuming n << MAXMD5
        splits[i - 1] = sizeOfEachSplit.multiply(BigInteger.valueOf(i));
      }
      return convertToBytes(splits);
    }

    public byte[] firstRow() {
      return convertToByte(BigInteger.ZERO);
    }

    public byte[] lastRow() {
      return convertToByte(MAXMD5_INT);
    }

    public byte[] strToRow(String in) {
      return convertToByte(new BigInteger(in, 16));
    }

    public String rowToStr(byte[] row) {
      return Bytes.toStringBinary(row);
    }

    public String separator() {
      return " ";
    }

    static BigInteger split2(BigInteger minValue, BigInteger maxValue) {
      return maxValue.add(minValue).divide(BigInteger.valueOf(2));
    }

    /**
     * Returns an array of bytes corresponding to an array of BigIntegers
     *
     * @param bigIntegers
     * @return bytes corresponding to the bigIntegers
     */
    static byte[][] convertToBytes(BigInteger[] bigIntegers) {
      byte[][] returnBytes = new byte[bigIntegers.length][];
      for (int i = 0; i < bigIntegers.length; i++) {
        returnBytes[i] = convertToByte(bigIntegers[i]);
      }
      return returnBytes;
    }

    /**
     * Returns the bytes corresponding to the BigInteger
     *
     * @param bigInteger
     * @return byte corresponding to input BigInteger
     */
    static byte[] convertToByte(BigInteger bigInteger) {
      String bigIntegerString = bigInteger.toString(16);
      bigIntegerString = StringUtils.leftPad(bigIntegerString,
          rowComparisonLength, '0');
      return Bytes.toBytes(bigIntegerString);
    }

    /**
     * Returns the BigInteger represented by thebyte array
     *
     * @param row
     * @return the corresponding BigInteger
     */
    static BigInteger convertToBigInteger(byte[] row) {
      return (row.length > 0) ? new BigInteger(Bytes.toString(row), 16)
          : BigInteger.ZERO;
    }
  }

}
