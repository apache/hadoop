/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.metadata;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import java.io.IOException;
import org.apache.hadoop.hdds.security.x509.certificate.authority
    .CertificateStore;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.DBStoreBuilder;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_DB_NAME;

/**
 * A RocksDB based implementation of SCM Metadata Store.
 * <p>
 * <p>
 * +---------------+------------------+-------------------------+
 * | Column Family |    Key           |          Value          |
 * +---------------+------------------+-------------------------+
 * | DeletedBlocks | TXID(Long)       | DeletedBlockTransaction |
 * +---------------+------------------+-------------------------+
 * | ValidCerts    | Serial (BigInt)  | X509Certificate         |
 * +---------------+------------------+-------------------------+
 * |RevokedCerts   | Serial (BigInt)  | X509Certificate         |
 * +---------------+------------------+-------------------------+
 */
public class SCMMetadataStoreRDBImpl implements SCMMetadataStore {

  private static final String DELETED_BLOCKS_TABLE = "deletedBlocks";
  private Table deletedBlocksTable;

  private static final String VALID_CERTS_TABLE = "validCerts";
  private Table validCertsTable;

  private static final String REVOKED_CERTS_TABLE = "revokedCerts";
  private Table revokedCertsTable;



  private static final Logger LOG =
      LoggerFactory.getLogger(SCMMetadataStoreRDBImpl.class);
  private DBStore store;
  private final OzoneConfiguration configuration;
  private final AtomicLong txID;

  /**
   * Constructs the metadata store and starts the DB Services.
   *
   * @param config - Ozone Configuration.
   * @throws IOException - on Failure.
   */
  public SCMMetadataStoreRDBImpl(OzoneConfiguration config)
      throws IOException {
    this.configuration = config;
    start(this.configuration);
    this.txID = new AtomicLong(this.getLargestRecordedTXID());
  }

  @Override
  public void start(OzoneConfiguration config)
      throws IOException {
    if (this.store == null) {
      File metaDir = ServerUtils.getScmDbDir(configuration);

      this.store = DBStoreBuilder.newBuilder(configuration)
          .setName(SCM_DB_NAME)
          .setPath(Paths.get(metaDir.getPath()))
          .addTable(DELETED_BLOCKS_TABLE)
          .addTable(VALID_CERTS_TABLE)
          .addTable(REVOKED_CERTS_TABLE)
          .addCodec(DeletedBlocksTransaction.class,
              new DeletedBlocksTransactionCodec())
          .addCodec(Long.class, new LongCodec())
          .addCodec(BigInteger.class, new BigIntegerCodec())
          .addCodec(X509Certificate.class, new X509CertificateCodec())
          .build();

      deletedBlocksTable = this.store.getTable(DELETED_BLOCKS_TABLE,
          Long.class, DeletedBlocksTransaction.class);
      checkTableStatus(deletedBlocksTable, DELETED_BLOCKS_TABLE);

      validCertsTable = this.store.getTable(VALID_CERTS_TABLE,
          BigInteger.class, X509Certificate.class);
      checkTableStatus(validCertsTable, VALID_CERTS_TABLE);

      revokedCertsTable = this.store.getTable(REVOKED_CERTS_TABLE,
          BigInteger.class, X509Certificate.class);
      checkTableStatus(revokedCertsTable, REVOKED_CERTS_TABLE);
    }
  }

  @Override
  public void stop() throws Exception {
    if (store != null) {
      store.close();
      store = null;
    }
  }

  @Override
  public org.apache.hadoop.utils.db.DBStore getStore() {
    return this.store;
  }

  @Override
  public Table<Long, DeletedBlocksTransaction> getDeletedBlocksTXTable() {
    return deletedBlocksTable;
  }

  @Override
  public Long getNextDeleteBlockTXID() {
    return this.txID.incrementAndGet();
  }

  @Override
  public Table<BigInteger, X509Certificate> getValidCertsTable() {
    return validCertsTable;
  }

  @Override
  public Table<BigInteger, X509Certificate> getRevokedCertsTable() {
    return revokedCertsTable;
  }

  @Override
  public TableIterator getAllCerts(CertificateStore.CertType certType) {
    if(certType == CertificateStore.CertType.VALID_CERTS) {
      return validCertsTable.iterator();
    }

    if(certType == CertificateStore.CertType.REVOKED_CERTS) {
      return revokedCertsTable.iterator();
    }

    return null;
  }

  @Override
  public Long getCurrentTXID() {
    return this.txID.get();
  }

  /**
   * Returns the largest recorded TXID from the DB.
   *
   * @return Long
   * @throws IOException
   */
  private Long getLargestRecordedTXID() throws IOException {
    try (TableIterator<Long, DeletedBlocksTransaction> txIter =
             deletedBlocksTable.iterator()) {
      txIter.seekToLast();
      Long txid = txIter.key();
      if (txid != null) {
        return txid;
      }
    }
    return 0L;
  }


  private void checkTableStatus(Table table, String name) throws IOException {
    String logMessage = "Unable to get a reference to %s table. Cannot " +
        "continue.";
    String errMsg = "Inconsistent DB state, Table - %s. Please check the" +
        " logs for more info.";
    if (table == null) {
      LOG.error(String.format(logMessage, name));
      throw new IOException(String.format(errMsg, name));
    }
  }

}
