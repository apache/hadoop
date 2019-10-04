/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone;

import com.google.common.base.Joiner;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Strings;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless helper functions for the server and client side of OM
 * communication.
 */
public final class OmUtils {
  public static final Logger LOG = LoggerFactory.getLogger(OmUtils.class);
  private static final SecureRandom SRAND = new SecureRandom();
  private static byte[] randomBytes = new byte[32];

  private OmUtils() {
  }

  /**
   * Retrieve the socket address that is used by OM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static InetSocketAddress getOmAddress(Configuration conf) {
    return NetUtils.createSocketAddr(getOmRpcAddress(conf));
  }

  /**
   * Retrieve the socket address that is used by OM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static String getOmRpcAddress(Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    return host.orElse(OZONE_OM_BIND_HOST_DEFAULT) + ":" +
        getOmRpcPort(conf);
  }

  /**
   * Retrieve the socket address that is used by OM as specified by the confKey.
   * Return null if the specified conf key is not set.
   * @param conf configuration
   * @param confKey configuration key to lookup address from
   * @return Target InetSocketAddress for the OM RPC server.
   */
  public static String getOmRpcAddress(Configuration conf, String confKey) {
    final Optional<String> host = getHostNameFromConfigKeys(conf, confKey);

    if (host.isPresent()) {
      return host.get() + ":" + getOmRpcPort(conf, confKey);
    } else {
      // The specified confKey is not set
      return null;
    }
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to OM.
   * @param conf
   * @return Target InetSocketAddress for the OM service endpoint.
   */
  public static InetSocketAddress getOmAddressForClients(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          OZONE_OM_ADDRESS_KEY + " must be defined. See" +
              " https://wiki.apache.org/hadoop/Ozone#Configuration for" +
              " details on configuring Ozone.");
    }

    return NetUtils.createSocketAddr(
        host.get() + ":" + getOmRpcPort(conf));
  }

  /**
   * Returns true if OZONE_OM_SERVICE_IDS_KEY is defined and not empty.
   * @param conf Configuration
   * @return true if OZONE_OM_SERVICE_IDS_KEY is defined and not empty;
   * else false.
   */
  public static boolean isServiceIdsDefined(Configuration conf) {
    String val = conf.get(OZONE_OM_SERVICE_IDS_KEY);
    return val != null && val.length() > 0;
  }

  /**
   * Returns true if HA for OzoneManager is configured for the given service id.
   * @param conf Configuration
   * @param serviceId OM HA cluster service ID
   * @return true if HA is configured in the configuration; else false.
   */
  public static boolean isOmHAServiceId(Configuration conf, String serviceId) {
    Collection<String> omServiceIds = conf.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY);
    return omServiceIds.contains(serviceId);
  }

  public static int getOmRpcPort(Configuration conf) {
    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    return port.orElse(OZONE_OM_PORT_DEFAULT);
  }

  /**
   * Retrieve the port that is used by OM as specified by the confKey.
   * Return default port if port is not specified in the confKey.
   * @param conf configuration
   * @param confKey configuration key to lookup address from
   * @return Port on which OM RPC server will listen on
   */
  public static int getOmRpcPort(Configuration conf, String confKey) {
    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf, confKey);
    return port.orElse(OZONE_OM_PORT_DEFAULT);
  }

  public static int getOmRestPort(Configuration conf) {
    // If no port number is specified then we'll just try the default
    // HTTP BindPort.
    final Optional<Integer> port =
        getPortNumberFromConfigKeys(conf, OZONE_OM_HTTP_ADDRESS_KEY);
    return port.orElse(OZONE_OM_HTTP_BIND_PORT_DEFAULT);
  }

  /**
   * Get the location where OM should store its metadata directories.
   * Fall back to OZONE_METADATA_DIRS if not defined.
   *
   * @param conf - Config
   * @return File path, after creating all the required Directories.
   */
  public static File getOmDbDir(Configuration conf) {
    return ServerUtils.getDBPath(conf, OMConfigKeys.OZONE_OM_DB_DIRS);
  }

  /**
   * Checks if the OM request is read only or not.
   * @param omRequest OMRequest proto
   * @return True if its readOnly, false otherwise.
   */
  public static boolean isReadOnly(
      OzoneManagerProtocolProtos.OMRequest omRequest) {
    OzoneManagerProtocolProtos.Type cmdType = omRequest.getCmdType();
    switch (cmdType) {
    case CheckVolumeAccess:
    case InfoVolume:
    case ListVolume:
    case InfoBucket:
    case ListBuckets:
    case LookupKey:
    case ListKeys:
    case InfoS3Bucket:
    case ListS3Buckets:
    case ServiceList:
    case ListMultiPartUploadParts:
    case GetFileStatus:
    case LookupFile:
    case ListStatus:
    case GetAcl:
    case DBUpdates:
    case ListMultipartUploads:
      return true;
    case CreateVolume:
    case SetVolumeProperty:
    case DeleteVolume:
    case CreateBucket:
    case SetBucketProperty:
    case DeleteBucket:
    case CreateKey:
    case RenameKey:
    case DeleteKey:
    case CommitKey:
    case AllocateBlock:
    case CreateS3Bucket:
    case DeleteS3Bucket:
    case InitiateMultiPartUpload:
    case CommitMultiPartUpload:
    case CompleteMultiPartUpload:
    case AbortMultiPartUpload:
    case GetS3Secret:
    case GetDelegationToken:
    case RenewDelegationToken:
    case CancelDelegationToken:
    case CreateDirectory:
    case CreateFile:
    case RemoveAcl:
    case SetAcl:
    case AddAcl:
    case PurgeKeys:
      return false;
    default:
      LOG.error("CmdType {} is not categorized as readOnly or not.", cmdType);
      return false;
    }
  }

  public static byte[] getMD5Digest(String input) throws IOException {
    try {
      MessageDigest md = MessageDigest.getInstance(OzoneConsts.MD5_HASH);
      return md.digest(input.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException("Error creating an instance of MD5 digest.\n" +
          "This could possibly indicate a faulty JRE");
    }
  }

  public static byte[] getSHADigest() throws IOException {
    try {
      SRAND.nextBytes(randomBytes);
      MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      return sha.digest(randomBytes);
    } catch (NoSuchAlgorithmException ex) {
      throw new IOException("Error creating an instance of SHA-256 digest.\n" +
          "This could possibly indicate a faulty JRE");
    }
  }

  /**
   * Add non empty and non null suffix to a key.
   */
  private static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
        "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  /**
   * Concatenate list of suffix strings '.' separated.
   */
  private static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }

  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN.
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
  }

  /**
   * Match input address to local address.
   * Return true if it matches, false otherwsie.
   */
  public static boolean isAddressLocal(InetSocketAddress addr) {
    return NetUtils.isLocalAddress(addr.getAddress());
  }

  /**
   * Get a collection of all omNodeIds for the given omServiceId.
   */
  public static Collection<String> getOMNodeIds(Configuration conf,
      String omServiceId) {
    String key = addSuffix(OZONE_OM_NODES_KEY, omServiceId);
    return conf.getTrimmedStringCollection(key);
  }

  /**
   * @return <code>coll</code> if it is non-null and non-empty. Otherwise,
   * returns a list with a single null value.
   */
  public static Collection<String> emptyAsSingletonNull(Collection<String>
      coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
  }

  /**
   * Write OM DB Checkpoint to an output stream as a compressed file (tgz).
   * @param checkpoint checkpoint file
   * @param destination desination output stream.
   * @throws IOException
   */
  public static void writeOmDBCheckpointToStream(DBCheckpoint checkpoint,
                                                 OutputStream destination)
      throws IOException {

    try (CompressorOutputStream gzippedOut = new CompressorStreamFactory()
        .createCompressorOutputStream(CompressorStreamFactory.GZIP,
            destination)) {

      try (ArchiveOutputStream archiveOutputStream =
               new TarArchiveOutputStream(gzippedOut)) {

        Path checkpointPath = checkpoint.getCheckpointLocation();
        for (Path path : Files.list(checkpointPath)
            .collect(Collectors.toList())) {
          if (path != null) {
            Path fileName = path.getFileName();
            if (fileName != null) {
              includeFile(path.toFile(), fileName.toString(),
                  archiveOutputStream);
            }
          }
        }
      }
    } catch (CompressorException e) {
      throw new IOException(
          "Can't compress the checkpoint: " +
              checkpoint.getCheckpointLocation(), e);
    }
  }

  private static void includeFile(File file, String entryName,
                           ArchiveOutputStream archiveOutputStream)
      throws IOException {
    ArchiveEntry archiveEntry =
        archiveOutputStream.createArchiveEntry(file, entryName);
    archiveOutputStream.putArchiveEntry(archiveEntry);
    try (FileInputStream fis = new FileInputStream(file)) {
      IOUtils.copy(fis, archiveOutputStream);
    }
    archiveOutputStream.closeArchiveEntry();
  }

  /**
   * If a OM conf is only set with key suffixed with OM Node ID, return the
   * set value.
   * @return if the value is set for key suffixed with OM Node ID, return the
   * value, else return null.
   */
  public static String getConfSuffixedWithOMNodeId(Configuration conf,
      String confKey, String omServiceID, String omNodeId) {
    String suffixedConfKey = OmUtils.addKeySuffixes(
        confKey, omServiceID, omNodeId);
    String confValue = conf.getTrimmed(suffixedConfKey);
    if (StringUtils.isNotEmpty(confValue)) {
      return confValue;
    }
    return null;
  }

  /**
   * Returns the http address of peer OM node.
   * @param conf Configuration
   * @param omNodeId peer OM node ID
   * @param omNodeHostAddr peer OM node host address
   * @return http address of peer OM node in the format <hostName>:<port>
   */
  public static String getHttpAddressForOMPeerNode(Configuration conf,
      String omServiceId, String omNodeId, String omNodeHostAddr) {
    final Optional<String> bindHost = getHostNameFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTP_BIND_HOST_KEY, omServiceId, omNodeId));

    final Optional<Integer> addressPort = getPortNumberFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId));

    final Optional<String> addressHost = getHostNameFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTP_ADDRESS_KEY, omServiceId, omNodeId));

    String hostName = bindHost.orElse(addressHost.orElse(omNodeHostAddr));

    return hostName + ":" + addressPort.orElse(OZONE_OM_HTTP_BIND_PORT_DEFAULT);
  }

  /**
   * Returns the https address of peer OM node.
   * @param conf Configuration
   * @param omNodeId peer OM node ID
   * @param omNodeHostAddr peer OM node host address
   * @return https address of peer OM node in the format <hostName>:<port>
   */
  public static String getHttpsAddressForOMPeerNode(Configuration conf,
      String omServiceId, String omNodeId, String omNodeHostAddr) {
    final Optional<String> bindHost = getHostNameFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTPS_BIND_HOST_KEY, omServiceId, omNodeId));

    final Optional<Integer> addressPort = getPortNumberFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId));

    final Optional<String> addressHost = getHostNameFromConfigKeys(conf,
        addKeySuffixes(OZONE_OM_HTTPS_ADDRESS_KEY, omServiceId, omNodeId));

    String hostName = bindHost.orElse(addressHost.orElse(omNodeHostAddr));

    return hostName + ":" +
        addressPort.orElse(OZONE_OM_HTTPS_BIND_PORT_DEFAULT);
  }

  /**
   * Get the local directory where ratis logs will be stored.
   */
  public static String getOMRatisDirectory(Configuration conf) {
    String storageDir = conf.get(OMConfigKeys.OZONE_OM_RATIS_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = HddsServerUtil.getDefaultRatisDirectory(conf);
    }
    return storageDir;
  }

  public static String getOMRatisSnapshotDirectory(Configuration conf) {
    String snapshotDir = conf.get(OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_DIR);

    if (Strings.isNullOrEmpty(snapshotDir)) {
      snapshotDir = Paths.get(getOMRatisDirectory(conf),
          "snapshot").toString();
    }
    return snapshotDir;
  }

  public static File createOMDir(String dirPath) {
    File dirFile = new File(dirPath);
    if (!dirFile.exists() && !dirFile.mkdirs()) {
      throw new IllegalArgumentException("Unable to create path: " + dirFile);
    }
    return dirFile;
  }

  /**
   * Prepares key info to be moved to deletedTable.
   * 1. It strips GDPR metadata from key info
   * 2. For given object key, if the repeatedOmKeyInfo instance is null, it
   * implies that no entry for the object key exists in deletedTable so we
   * create a new instance to include this key, else we update the existing
   * repeatedOmKeyInfo instance.
   * @param keyInfo args supplied by client
   * @param repeatedOmKeyInfo key details from deletedTable
   * @return {@link RepeatedOmKeyInfo}
   * @throws IOException if I/O Errors when checking for key
   */
  public static RepeatedOmKeyInfo prepareKeyForDelete(OmKeyInfo keyInfo,
      RepeatedOmKeyInfo repeatedOmKeyInfo) throws IOException{
    // If this key is in a GDPR enforced bucket, then before moving
    // KeyInfo to deletedTable, remove the GDPR related metadata from
    // KeyInfo.
    if(Boolean.valueOf(keyInfo.getMetadata().get(OzoneConsts.GDPR_FLAG))) {
      keyInfo.getMetadata().remove(OzoneConsts.GDPR_FLAG);
      keyInfo.getMetadata().remove(OzoneConsts.GDPR_ALGORITHM);
      keyInfo.getMetadata().remove(OzoneConsts.GDPR_SECRET);
    }

    if(repeatedOmKeyInfo == null) {
      //The key doesn't exist in deletedTable, so create a new instance.
      repeatedOmKeyInfo = new RepeatedOmKeyInfo(keyInfo);
    } else {
      //The key exists in deletedTable, so update existing instance.
      repeatedOmKeyInfo.addOmKeyInfo(keyInfo);
    }

    return repeatedOmKeyInfo;
  }
}
