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

package org.apache.hadoop.hbase.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.avro.generated.AClusterStatus;
import org.apache.hadoop.hbase.avro.generated.ADelete;
import org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor;
import org.apache.hadoop.hbase.avro.generated.AGet;
import org.apache.hadoop.hbase.avro.generated.AIOError;
import org.apache.hadoop.hbase.avro.generated.AIllegalArgument;
import org.apache.hadoop.hbase.avro.generated.AMasterNotRunning;
import org.apache.hadoop.hbase.avro.generated.APut;
import org.apache.hadoop.hbase.avro.generated.AResult;
import org.apache.hadoop.hbase.avro.generated.AScan;
import org.apache.hadoop.hbase.avro.generated.ATableDescriptor;
import org.apache.hadoop.hbase.avro.generated.ATableExists;
import org.apache.hadoop.hbase.avro.generated.HBase;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Start an Avro server
 */
public class AvroServer {

  /**
   * The HBaseImpl is a glue object that connects Avro RPC calls to the
   * HBase client API primarily defined in the HBaseAdmin and HTable objects.
   */
  public static class HBaseImpl implements HBase {
    //
    // PROPERTIES
    //
    protected Configuration conf = null;
    protected HBaseAdmin admin = null;
    protected HTablePool htablePool = null;
    protected final Log LOG = LogFactory.getLog(this.getClass().getName());

    // nextScannerId and scannerMap are used to manage scanner state
    protected int nextScannerId = 0;
    protected HashMap<Integer, ResultScanner> scannerMap = null;

    //
    // UTILITY METHODS
    //

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal
     * hash-map.
     *
     * @param scanner
     * @return integer scanner id
     */
    protected synchronized int addScanner(ResultScanner scanner) {
      int id = nextScannerId++;
      scannerMap.put(id, scanner);
      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner getScanner(int id) {
      return scannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal
     * id->scanner hash-map.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner removeScanner(int id) {
      return scannerMap.remove(id);
    }

    //
    // CTOR METHODS
    //

    // TODO(hammer): figure out appropriate setting of maxSize for htablePool
    /**
     * Constructs an HBaseImpl object.
     * @throws IOException 
     */
    HBaseImpl() throws IOException {
      this(HBaseConfiguration.create());
    }

    HBaseImpl(final Configuration c) throws IOException {
      conf = c;
      admin = new HBaseAdmin(conf);
      htablePool = new HTablePool(conf, 10);
      scannerMap = new HashMap<Integer, ResultScanner>();
    }

    //
    // SERVICE METHODS
    //

    // TODO(hammer): Investigate use of the Command design pattern

    //
    // Cluster metadata
    //

    public Utf8 getHBaseVersion() throws AIOError {
      try {
	return new Utf8(admin.getClusterStatus().getHBaseVersion());
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public AClusterStatus getClusterStatus() throws AIOError {
      try {
	return AvroUtil.csToACS(admin.getClusterStatus());
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public GenericArray<ATableDescriptor> listTables() throws AIOError {
      try {
        HTableDescriptor[] tables = admin.listTables();
	Schema atdSchema = Schema.createArray(ATableDescriptor.SCHEMA$);
        GenericData.Array<ATableDescriptor> result = null;
	result = new GenericData.Array<ATableDescriptor>(tables.length, atdSchema);
        for (HTableDescriptor table : tables) {
	  result.add(AvroUtil.htdToATD(table));
	}
        return result;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    //
    // Table metadata
    //

    // TODO(hammer): Handle the case where the table does not exist explicitly?
    public ATableDescriptor describeTable(ByteBuffer table) throws AIOError {
      try {
	return AvroUtil.htdToATD(admin.getTableDescriptor(Bytes.toBytes(table)));
      } catch (IOException e) {
        AIOError ioe = new AIOError();
        ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public boolean isTableEnabled(ByteBuffer table) throws AIOError {
      try {
	return admin.isTableEnabled(Bytes.toBytes(table));
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public boolean tableExists(ByteBuffer table) throws AIOError {
      try {
	return admin.tableExists(Bytes.toBytes(table));
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    //
    // Family metadata
    //

    // TODO(hammer): Handle the case where the family does not exist explicitly?
    public AFamilyDescriptor describeFamily(ByteBuffer table, ByteBuffer family) throws AIOError {
      try {
	HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(table));
	return AvroUtil.hcdToAFD(htd.getFamily(Bytes.toBytes(family)));
      } catch (IOException e) {
        AIOError ioe = new AIOError();
        ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    //
    // Table admin
    //

    public Void createTable(ATableDescriptor table) throws AIOError, 
                                                           AIllegalArgument,
                                                           ATableExists,
                                                           AMasterNotRunning {
      try {
        admin.createTable(AvroUtil.atdToHTD(table));
	return null;
      } catch (IllegalArgumentException e) {
	AIllegalArgument iae = new AIllegalArgument();
	iae.message = new Utf8(e.getMessage());
        throw iae;
      } catch (TableExistsException e) {
	ATableExists tee = new ATableExists();
	tee.message = new Utf8(e.getMessage());
        throw tee;
      } catch (MasterNotRunningException e) {
	AMasterNotRunning mnre = new AMasterNotRunning();
	mnre.message = new Utf8(e.getMessage());
        throw mnre;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // Note that disable, flush and major compaction of .META. needed in client
    // TODO(hammer): more selective cache dirtying than flush?
    public Void deleteTable(ByteBuffer table) throws AIOError {
      try {
	admin.deleteTable(Bytes.toBytes(table));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // NB: Asynchronous operation
    public Void modifyTable(ByteBuffer tableName, ATableDescriptor tableDescriptor) throws AIOError {
      try {
	admin.modifyTable(Bytes.toBytes(tableName),
                          AvroUtil.atdToHTD(tableDescriptor));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public Void enableTable(ByteBuffer table) throws AIOError {
      try {
	admin.enableTable(Bytes.toBytes(table));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }
    
    public Void disableTable(ByteBuffer table) throws AIOError {
      try {
	admin.disableTable(Bytes.toBytes(table));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }
    
    // NB: Asynchronous operation
    public Void flush(ByteBuffer table) throws AIOError {
      try {
	admin.flush(Bytes.toBytes(table));
	return null;
      } catch (InterruptedException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // NB: Asynchronous operation
    public Void split(ByteBuffer table) throws AIOError {
      try {
	admin.split(Bytes.toBytes(table));
	return null;
      } catch (InterruptedException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    //
    // Family admin
    //

    public Void addFamily(ByteBuffer table, AFamilyDescriptor family) throws AIOError {
      try {
	admin.addColumn(Bytes.toBytes(table), 
                        AvroUtil.afdToHCD(family));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // NB: Asynchronous operation
    public Void deleteFamily(ByteBuffer table, ByteBuffer family) throws AIOError {
      try {
	admin.deleteColumn(Bytes.toBytes(table), Bytes.toBytes(family));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // NB: Asynchronous operation
    public Void modifyFamily(ByteBuffer table, ByteBuffer familyName, AFamilyDescriptor familyDescriptor) throws AIOError {
      try {
	admin.modifyColumn(Bytes.toBytes(table), AvroUtil.afdToHCD(familyDescriptor));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    //
    // Single-row DML
    //

    // TODO(hammer): Java with statement for htablepool concision?
    // TODO(hammer): Can Get have timestamp and timerange simultaneously?
    // TODO(hammer): Do I need to catch the RuntimeException of getTable?
    // TODO(hammer): Handle gets with no results
    // TODO(hammer): Uses exists(Get) to ensure columns exist
    public AResult get(ByteBuffer table, AGet aget) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
        return AvroUtil.resultToAResult(htable.get(AvroUtil.agetToGet(aget)));
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        try {
          htablePool.putTable(htable);
        } catch (IOException e) {
          AIOError ioe = new AIOError();
          ioe.message = new Utf8(e.getMessage());
          throw ioe;
        }
      }
    }

    public boolean exists(ByteBuffer table, AGet aget) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
        return htable.exists(AvroUtil.agetToGet(aget));
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        try {
          htablePool.putTable(htable);
        } catch (IOException e) {
          AIOError ioe = new AIOError();
          ioe.message = new Utf8(e.getMessage());
          throw ioe;
        } 
      }
    }

    public Void put(ByteBuffer table, APut aput) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
	htable.put(AvroUtil.aputToPut(aput));
        return null;
      } catch (IOException e) {
        AIOError ioe = new AIOError();
        ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        try {
          htablePool.putTable(htable);
        } catch (IOException e) {
          AIOError ioe = new AIOError();
          ioe.message = new Utf8(e.getMessage());
          throw ioe;
        }
      }
    }

    public Void delete(ByteBuffer table, ADelete adelete) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
        htable.delete(AvroUtil.adeleteToDelete(adelete));
        return null;
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        try {
          htablePool.putTable(htable);
        } catch (IOException e) {
          AIOError ioe = new AIOError();
          ioe.message = new Utf8(e.getMessage());
          throw ioe;
        }
      }
    }

    public long incrementColumnValue(ByteBuffer table, ByteBuffer row, ByteBuffer family, ByteBuffer qualifier, long amount, boolean writeToWAL) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
	return htable.incrementColumnValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qualifier), amount, writeToWAL);
      } catch (IOException e) {
        AIOError ioe = new AIOError();
        ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        try {
          htablePool.putTable(htable);
        } catch (IOException e) {
          AIOError ioe = new AIOError();
          ioe.message = new Utf8(e.getMessage());
          throw ioe;
        }
      }
    }

    //
    // Multi-row DML
    //

    public int scannerOpen(ByteBuffer table, AScan ascan) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
        Scan scan = AvroUtil.ascanToScan(ascan);
        return addScanner(htable.getScanner(scan));
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        try {
          htablePool.putTable(htable);
        } catch (IOException e) {
          AIOError ioe = new AIOError();
          ioe.message = new Utf8(e.getMessage());
          throw ioe;
        }
      }
    }

    public Void scannerClose(int scannerId) throws AIOError, AIllegalArgument {
      try {
        ResultScanner scanner = getScanner(scannerId);
        if (scanner == null) {
      	  AIllegalArgument aie = new AIllegalArgument();
	  aie.message = new Utf8("scanner ID is invalid: " + scannerId);
          throw aie;
        }
        scanner.close();
        removeScanner(scannerId);
        return null;
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public GenericArray<AResult> scannerGetRows(int scannerId, int numberOfRows) throws AIOError, AIllegalArgument {
      try {
        ResultScanner scanner = getScanner(scannerId);
        if (scanner == null) {
      	  AIllegalArgument aie = new AIllegalArgument();
	  aie.message = new Utf8("scanner ID is invalid: " + scannerId);
          throw aie;
        }
        return AvroUtil.resultsToAResults(scanner.next(numberOfRows));
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }
  }

  //
  // MAIN PROGRAM
  //

  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }
  
  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.out.println("Usage: java org.apache.hadoop.hbase.avro.AvroServer " +
      "--help | [--port=PORT] start");
    System.out.println("Arguments:");
    System.out.println(" start Start Avro server");
    System.out.println(" stop  Stop Avro server");
    System.out.println("Options:");
    System.out.println(" port  Port to listen on. Default: 9090");
    System.out.println(" help  Print this message and exit");
    System.exit(0);
  }

  protected static void doMain(final String[] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }
    int port = 9090;
    final String portArgKey = "--port=";
    for (String cmd: args) {
      if (cmd.startsWith(portArgKey)) {
        port = Integer.parseInt(cmd.substring(portArgKey.length()));
        continue;
      } else if (cmd.equals("--help") || cmd.equals("-h")) {
        printUsageAndExit();
      } else if (cmd.equals("start")) {
        continue;
      } else if (cmd.equals("stop")) {
        printUsageAndExit("To shutdown the Avro server run " +
          "bin/hbase-daemon.sh stop avro or send a kill signal to " +
          "the Avro server pid");
      }
      
      // Print out usage if we get to here.
      printUsageAndExit();
    }
    Log LOG = LogFactory.getLog("AvroServer");
    LOG.info("starting HBase Avro server on port " + Integer.toString(port));
    SpecificResponder r = new SpecificResponder(HBase.class, new HBaseImpl());
    HttpServer server = new HttpServer(r, port);
    server.start();
    server.join();
  }

  // TODO(hammer): Look at Cassandra's daemonization and integration with JSVC
  // TODO(hammer): Don't eat it after a single exception
  // TODO(hammer): Figure out why we do doMain()
  // TODO(hammer): Figure out if we want String[] or String [] syntax
  public static void main(String[] args) throws Exception {
    doMain(args);
  }
}
