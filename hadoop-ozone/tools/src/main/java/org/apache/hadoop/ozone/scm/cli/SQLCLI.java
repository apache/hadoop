/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.scm.cli;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_SUFFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_USER_PREFIX;

/**
 * This is the CLI that can be use to convert an ozone metadata DB into
 * a sqlite DB file.
 *
 * NOTE: user should use this CLI in an offline fashion. Namely, this should not
 * be used to convert a DB that is currently being used by Ozone. Instead,
 * this should be used to debug and diagnosis closed DB instances.
 *
 */
public class SQLCLI  extends Configured implements Tool {

  private Options options;
  private BasicParser parser;
  private final Charset encoding = Charset.forName("UTF-8");
  private final OzoneConfiguration conf;

  // for container.db
  private static final String CREATE_CONTAINER_INFO =
      "CREATE TABLE containerInfo (" +
          "containerID LONG PRIMARY KEY NOT NULL, " +
          "replicationType TEXT NOT NULL," +
          "replicationFactor TEXT NOT NULL," +
          "usedBytes LONG NOT NULL," +
          "owner TEXT," +
          "numberOfKeys LONG)";
  private static final String CREATE_DATANODE_INFO =
      "CREATE TABLE datanodeInfo (" +
          "hostName TEXT NOT NULL, " +
          "datanodeUUId TEXT PRIMARY KEY NOT NULL," +
          "ipAddress TEXT, " +
          "containerPort INTEGER NOT NULL);";
  private static final String INSERT_CONTAINER_INFO =
      "INSERT INTO containerInfo (containerID, replicationType, "
          + "replicationFactor, usedBytes, owner, "
          + "numberOfKeys) VALUES (\"%d\", \"%s\", \"%s\", \"%d\", "
          + "\"%s\", \"%d\")";
  private static final String INSERT_DATANODE_INFO =
      "INSERT INTO datanodeInfo (hostname, datanodeUUid, ipAddress, " +
          "containerPort) " +
          "VALUES (\"%s\", \"%s\", \"%s\", \"%d\")";
  private static final String INSERT_CONTAINER_MEMBERS =
      "INSERT INTO containerMembers (containerName, datanodeUUID) " +
          "VALUES (\"%s\", \"%s\")";
  // and reuse CREATE_DATANODE_INFO and INSERT_DATANODE_INFO
  // for openContainer.db
  private static final String CREATE_OPEN_CONTAINER =
      "CREATE TABLE openContainer (" +
          "containerName TEXT PRIMARY KEY NOT NULL, " +
          "containerUsed INTEGER NOT NULL)";
  private static final String INSERT_OPEN_CONTAINER =
      "INSERT INTO openContainer (containerName, containerUsed) " +
          "VALUES (\"%s\", \"%s\")";

  // for om.db
  private static final String CREATE_VOLUME_LIST =
      "CREATE TABLE volumeList (" +
          "userName TEXT NOT NULL," +
          "volumeName TEXT NOT NULL," +
          "PRIMARY KEY (userName, volumeName))";
  private static final String INSERT_VOLUME_LIST =
      "INSERT INTO volumeList (userName, volumeName) " +
          "VALUES (\"%s\", \"%s\")";

  private static final String CREATE_VOLUME_INFO =
      "CREATE TABLE volumeInfo (" +
          "adminName TEXT NOT NULL," +
          "ownerName TEXT NOT NULL," +
          "volumeName TEXT NOT NULL," +
          "PRIMARY KEY (adminName, ownerName, volumeName))";
  private static final String INSERT_VOLUME_INFO =
      "INSERT INTO volumeInfo (adminName, ownerName, volumeName) " +
          "VALUES (\"%s\", \"%s\", \"%s\")";

  private static final String CREATE_ACL_INFO =
      "CREATE TABLE aclInfo (" +
          "adminName TEXT NOT NULL," +
          "ownerName TEXT NOT NULL," +
          "volumeName TEXT NOT NULL," +
          "type TEXT NOT NULL," +
          "userName TEXT NOT NULL," +
          "rights TEXT NOT NULL," +
          "FOREIGN KEY (adminName, ownerName, volumeName, userName, type)" +
          "REFERENCES " +
          "volumeInfo(adminName, ownerName, volumeName, userName, type)" +
          "PRIMARY KEY (adminName, ownerName, volumeName, userName, type))";
  private static final String INSERT_ACL_INFO =
      "INSERT INTO aclInfo (adminName, ownerName, volumeName, type, " +
          "userName, rights) " +
          "VALUES (\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\")";

  private static final String CREATE_BUCKET_INFO =
      "CREATE TABLE bucketInfo (" +
          "volumeName TEXT NOT NULL," +
          "bucketName TEXT NOT NULL," +
          "versionEnabled BOOLEAN NOT NULL," +
          "storageType TEXT," +
          "PRIMARY KEY (volumeName, bucketName))";
  private static final String INSERT_BUCKET_INFO =
      "INSERT INTO bucketInfo(volumeName, bucketName, " +
          "versionEnabled, storageType)" +
          "VALUES (\"%s\", \"%s\", \"%s\", \"%s\")";

  private static final String CREATE_KEY_INFO =
      "CREATE TABLE keyInfo (" +
          "volumeName TEXT NOT NULL," +
          "bucketName TEXT NOT NULL," +
          "keyName TEXT NOT NULL," +
          "dataSize INTEGER," +
          "blockKey TEXT NOT NULL," +
          "containerName TEXT NOT NULL," +
          "PRIMARY KEY (volumeName, bucketName, keyName))";
  private static final String INSERT_KEY_INFO =
      "INSERT INTO keyInfo (volumeName, bucketName, keyName, dataSize, " +
          "blockKey, containerName)" +
          "VALUES (\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\")";

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLCLI.class);

  public SQLCLI(OzoneConfiguration conf) {
    this.options = getOptions();
    this.parser = new BasicParser();
    this.conf = conf;
  }

  @SuppressWarnings("static-access")
  private Options getOptions() {
    Options allOptions = new Options();
    Option helpOpt = OptionBuilder
        .hasArg(false)
        .withLongOpt("help")
        .withDescription("display help message")
        .create("h");
    allOptions.addOption(helpOpt);

    Option dbPathOption = OptionBuilder
        .withArgName("DB path")
        .withLongOpt("dbPath")
        .hasArgs(1)
        .withDescription("specify DB path")
        .create("p");
    allOptions.addOption(dbPathOption);

    Option outPathOption = OptionBuilder
        .withArgName("output path")
        .withLongOpt("outPath")
        .hasArgs(1)
        .withDescription("specify output DB file path")
        .create("o");
    allOptions.addOption(outPathOption);

    return allOptions;
  }

  public void displayHelp() {
    HelpFormatter helpFormatter = new HelpFormatter();
    Options allOpts = getOptions();
    helpFormatter.printHelp("hdfs oz_debug -p <DB path>"
        + " -o <Output DB file path>", allOpts);
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLine commandLine = parseArgs(args);
    if (commandLine.hasOption("help")) {
      displayHelp();
      return 0;
    }
    if (!commandLine.hasOption("p") || !commandLine.hasOption("o")) {
      displayHelp();
      return -1;
    }
    String value = commandLine.getOptionValue("p");
    LOG.info("DB path {}", value);
    // the value is supposed to be an absolute path to a container file
    Path dbPath = Paths.get(value);
    if (!Files.exists(dbPath)) {
      LOG.error("DB path not exist:{}", dbPath);
    }
    Path parentPath = dbPath.getParent();
    Path dbName = dbPath.getFileName();
    if (parentPath == null || dbName == null) {
      LOG.error("Error processing db path {}", dbPath);
      return -1;
    }

    value = commandLine.getOptionValue("o");
    Path outPath = Paths.get(value);
    if (outPath == null || outPath.getParent() == null) {
      LOG.error("Error processing output path {}", outPath);
      return -1;
    }

    if (outPath.toFile().isDirectory()) {
      LOG.error("The db output path should be a file instead of a directory");
      return -1;
    }

    Path outParentPath = outPath.getParent();
    if (outParentPath != null) {
      if (!Files.exists(outParentPath)) {
        Files.createDirectories(outParentPath);
      }
    }
    LOG.info("Parent path [{}] db name [{}]", parentPath, dbName);
    if (dbName.toString().endsWith(CONTAINER_DB_SUFFIX)) {
      LOG.info("Converting container DB");
      convertContainerDB(dbPath, outPath);
    } else if (dbName.toString().equals(OM_DB_NAME)) {
      LOG.info("Converting om DB");
      convertOMDB(dbPath, outPath);
    } else {
      LOG.error("Unrecognized db name {}", dbName);
    }
    return 0;
  }

  private Connection connectDB(String dbPath) throws Exception {
    Class.forName("org.sqlite.JDBC");
    String connectPath =
        String.format("jdbc:sqlite:%s", dbPath);
    return DriverManager.getConnection(connectPath);
  }

  private void executeSQL(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    }
  }

  /**
   * Convert om.db to sqlite db file. With following schema.
   * (* for primary key)
   *
   * 1. for key type USER, it contains a username and a list volumes
   * volumeList
   * --------------------------------
   *   userName*     |  volumeName*
   * --------------------------------
   *
   * 2. for key type VOLUME:
   *
   * volumeInfo
   * ----------------------------------------------
   * adminName | ownerName* | volumeName* | aclID
   * ----------------------------------------------
   *
   * aclInfo
   * ----------------------------------------------
   * aclEntryID* | type* | userName* | rights
   * ----------------------------------------------
   *
   * 3. for key type BUCKET
   * bucketInfo
   * --------------------------------------------------------
   * volumeName* | bucketName* | versionEnabled | storageType
   * --------------------------------------------------------
   *
   * TODO : the following table will be changed when key partition is added.
   * Only has the minimum entries for test purpose now.
   * 4. for key type KEY
   * -----------------------------------------------
   * volumeName* | bucketName* | keyName* | dataSize
   * -----------------------------------------------
   *
   *
   *
   * @param dbPath
   * @param outPath
   * @throws Exception
   */
  private void convertOMDB(Path dbPath, Path outPath) throws Exception {
    LOG.info("Create tables for sql om db.");
    File dbFile = dbPath.toFile();
    try (MetadataStore dbStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf).setDbFile(dbFile).build();
         Connection conn = connectDB(outPath.toString())) {
      executeSQL(conn, CREATE_VOLUME_LIST);
      executeSQL(conn, CREATE_VOLUME_INFO);
      executeSQL(conn, CREATE_ACL_INFO);
      executeSQL(conn, CREATE_BUCKET_INFO);
      executeSQL(conn, CREATE_KEY_INFO);

      dbStore.iterate(null, (key, value) -> {
        String keyString = DFSUtilClient.bytes2String(key);
        KeyType type = getKeyType(keyString);
        try {
          insertOMDB(conn, type, keyString, value);
        } catch (IOException | SQLException ex) {
          LOG.error("Exception inserting key {} type {}", keyString, type, ex);
        }
        return true;
      });
    }
  }

  private void insertOMDB(Connection conn, KeyType type, String keyName,
                          byte[] value) throws IOException, SQLException {
    switch (type) {
    case USER:
      VolumeList volumeList = VolumeList.parseFrom(value);
      for (String volumeName : volumeList.getVolumeNamesList()) {
        String insertVolumeList =
            String.format(INSERT_VOLUME_LIST, keyName, volumeName);
        executeSQL(conn, insertVolumeList);
      }
      break;
    case VOLUME:
      VolumeInfo volumeInfo = VolumeInfo.parseFrom(value);
      String adminName = volumeInfo.getAdminName();
      String ownerName = volumeInfo.getOwnerName();
      String volumeName = volumeInfo.getVolume();
      String insertVolumeInfo =
          String.format(INSERT_VOLUME_INFO, adminName, ownerName, volumeName);
      executeSQL(conn, insertVolumeInfo);
      for (OzoneAclInfo aclInfo : volumeInfo.getVolumeAclsList()) {
        String insertAclInfo =
            String.format(INSERT_ACL_INFO, adminName, ownerName, volumeName,
                aclInfo.getType(), aclInfo.getName(), aclInfo.getRights());
        executeSQL(conn, insertAclInfo);
      }
      break;
    case BUCKET:
      BucketInfo bucketInfo = BucketInfo.parseFrom(value);
      String insertBucketInfo =
          String.format(INSERT_BUCKET_INFO, bucketInfo.getVolumeName(),
              bucketInfo.getBucketName(), bucketInfo.getIsVersionEnabled(),
              bucketInfo.getStorageType());
      executeSQL(conn, insertBucketInfo);
      break;
    case KEY:
      KeyInfo keyInfo = KeyInfo.parseFrom(value);
      // TODO : the two fields container name and block id are no longer used,
      // need to revisit this later.
      String insertKeyInfo =
          String.format(INSERT_KEY_INFO, keyInfo.getVolumeName(),
              keyInfo.getBucketName(), keyInfo.getKeyName(),
              keyInfo.getDataSize(), "EMPTY",
              "EMPTY");
      executeSQL(conn, insertKeyInfo);
      break;
    default:
      throw new IOException("Unknown key from om.db");
    }
  }

  // TODO: This has to be fixed.
  // we don't have prefix anymore. now each key is written into different
  // table. The logic has to be changed.
  private KeyType getKeyType(String key) {
    if (key.startsWith(OM_USER_PREFIX)) {
      return KeyType.USER;
    } else if (key.startsWith(OM_KEY_PREFIX)) {
      return key.replaceFirst(OM_KEY_PREFIX, "")
          .contains(OM_KEY_PREFIX) ? KeyType.BUCKET : KeyType.VOLUME;
    }else {
      return KeyType.KEY;
    }
  }

  private enum KeyType {
    USER,
    VOLUME,
    BUCKET,
    KEY,
    UNKNOWN
  }

  /**
   * Convert container.db to sqlite. The schema of sql db:
   * three tables, containerId, containerMachines, datanodeInfo
   * (* for primary key)
   *
   * containerInfo:
   * ----------------------------------------------
   * container name* | container lead datanode uuid
   * ----------------------------------------------
   *
   * containerMembers:
   * --------------------------------
   * container name* |  datanodeUUid*
   * --------------------------------
   *
   * datanodeInfo:
   * ---------------------------------------------------------
   * hostname | datanodeUUid* | xferPort | ipcPort
   * ---------------------------------------------------------
   *
   * --------------------------------
   * | containerPort
   * --------------------------------
   *
   * @param dbPath path to container db.
   * @param outPath path to output sqlite
   * @throws IOException throws exception.
   */
  private void convertContainerDB(Path dbPath, Path outPath)
      throws Exception {
    LOG.info("Create tables for sql container db.");
    File dbFile = dbPath.toFile();
    try (MetadataStore dbStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf).setDbFile(dbFile).build();
        Connection conn = connectDB(outPath.toString())) {
      executeSQL(conn, CREATE_CONTAINER_INFO);

      dbStore.iterate(null, (key, value) -> {
        long containerID = Longs.fromByteArray(key);
        ContainerInfo containerInfo = null;
        containerInfo = ContainerInfo.fromProtobuf(
            HddsProtos.ContainerInfoProto.PARSER.parseFrom(value));
        Preconditions.checkNotNull(containerInfo);
        try {
          //TODO: include container state to sqllite schema
          insertContainerDB(conn, containerInfo, containerID);
          return true;
        } catch (SQLException e) {
          throw new IOException(e);
        }
      });
    }
  }

  /**
   * Insert into the sqlite DB of container.db.
   * @param conn the connection to the sqlite DB.
   * @param containerInfo
   * @param containerID
   * @throws SQLException throws exception.
   */
  private void insertContainerDB(Connection conn, ContainerInfo containerInfo,
      long containerID) throws SQLException {
    LOG.info("Insert to sql container db, for container {}", containerID);
    String insertContainerInfo = String.format(
        INSERT_CONTAINER_INFO, containerID,
        containerInfo.getReplicationType(),
        containerInfo.getReplicationFactor(),
        containerInfo.getUsedBytes(),
        containerInfo.getOwner(),
        containerInfo.getNumberOfKeys());

    executeSQL(conn, insertContainerInfo);
    LOG.info("Insertion completed.");
  }


  /**
   * Convert openContainer.db to sqlite db file. This is rather simple db,
   * the schema has only one table:
   *
   * openContainer
   * -------------------------------
   * containerName* | containerUsed
   * -------------------------------
   *
   * @param dbPath path to container db.
   * @param outPath path to output sqlite
   * @throws IOException throws exception.
   */
  private void convertOpenContainerDB(Path dbPath, Path outPath)
      throws Exception {
    LOG.info("Create table for open container db.");
    File dbFile = dbPath.toFile();
    try (MetadataStore dbStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf).setDbFile(dbFile).build();
        Connection conn = connectDB(outPath.toString())) {
      executeSQL(conn, CREATE_OPEN_CONTAINER);

      dbStore.iterate(null, (key, value) -> {
        String containerName = DFSUtil.bytes2String(key);
        Long containerUsed =
            Long.parseLong(DFSUtil.bytes2String(value));
        String insertOpenContainer = String
            .format(INSERT_OPEN_CONTAINER, containerName, containerUsed);
        try {
          executeSQL(conn, insertOpenContainer);
          return true;
        } catch (SQLException e) {
          throw new IOException(e);
        }
      });
    }
  }

  private CommandLine parseArgs(String[] argv)
      throws ParseException {
    return parser.parse(options, argv);
  }

  public static void main(String[] args) {
    Tool shell = new SQLCLI(new OzoneConfiguration());
    int res = 0;
    try {
      ToolRunner.run(shell, args);
    } catch (Exception ex) {
      LOG.error(ex.toString());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Command execution failed", ex);
      }
      res = 1;
    }
    System.exit(res);
  }
}
