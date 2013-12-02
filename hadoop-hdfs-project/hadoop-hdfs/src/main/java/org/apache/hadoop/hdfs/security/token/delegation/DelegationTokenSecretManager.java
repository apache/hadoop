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

package org.apache.hadoop.hdfs.security.token.delegation;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * A HDFS specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
@InterfaceAudience.Private
public class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

  private static final Log LOG = LogFactory
      .getLog(DelegationTokenSecretManager.class);
  
  private final FSNamesystem namesystem;

  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, FSNamesystem namesystem) {
    this(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval, false,
        namesystem);
  }

  /**
   * Create a secret manager
   * @param delegationKeyUpdateInterval the number of seconds for rolling new
   *        secret keys.
   * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
   *        tokens
   * @param delegationTokenRenewInterval how often the tokens must be renewed
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   *        for expired tokens
   * @param storeTokenTrackingId whether to store the token's tracking id
   */
  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, boolean storeTokenTrackingId,
      FSNamesystem namesystem) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.namesystem = namesystem;
    this.storeTokenTrackingId = storeTokenTrackingId;
  }

  @Override //SecretManager
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }
  
  @Override
  public byte[] retrievePassword(
      DelegationTokenIdentifier identifier) throws InvalidToken {
    try {
      // this check introduces inconsistency in the authentication to a
      // HA standby NN.  non-token auths are allowed into the namespace which
      // decides whether to throw a StandbyException.  tokens are a bit
      // different in that a standby may be behind and thus not yet know
      // of all tokens issued by the active NN.  the following check does
      // not allow ANY token auth, however it should allow known tokens in
      namesystem.checkOperation(OperationCategory.READ);
    } catch (StandbyException se) {
      // FIXME: this is a hack to get around changing method signatures by
      // tunneling a non-InvalidToken exception as the cause which the
      // RPC server will unwrap before returning to the client
      InvalidToken wrappedStandby = new InvalidToken("StandbyException");
      wrappedStandby.initCause(se);
      throw wrappedStandby;
    }
    return super.retrievePassword(identifier);
  }
  
  /**
   * Returns expiry time of a token given its identifier.
   * 
   * @param dtId DelegationTokenIdentifier of a token
   * @return Expiry time of the token
   * @throws IOException
   */
  public synchronized long getTokenExpiryTime(
      DelegationTokenIdentifier dtId) throws IOException {
    DelegationTokenInformation info = currentTokens.get(dtId);
    if (info != null) {
      return info.getRenewDate();
    } else {
      throw new IOException("No delegation token found for this identifier");
    }
  }
  
  /**
   * Load SecretManager state from fsimage.
   * 
   * @param in input stream to read fsimage
   * @throws IOException
   */
  public synchronized void loadSecretManagerState(DataInput in)
      throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't load state from image in a running SecretManager.");
    }
    currentId = in.readInt();
    loadAllKeys(in);
    delegationTokenSequenceNumber = in.readInt();
    loadCurrentTokens(in);
  }
  
  /**
   * Store the current state of the SecretManager for persistence
   * 
   * @param out Output stream for writing into fsimage.
   * @param sdPath String storage directory path
   * @throws IOException
   */
  public synchronized void saveSecretManagerState(DataOutputStream out,
      String sdPath) throws IOException {
    out.writeInt(currentId);
    saveAllKeys(out, sdPath);
    out.writeInt(delegationTokenSequenceNumber);
    saveCurrentTokens(out, sdPath);
  }
  
  /**
   * This method is intended to be used only while reading edit logs.
   * 
   * @param identifier DelegationTokenIdentifier read from the edit logs or
   * fsimage
   * 
   * @param expiryTime token expiry time
   * @throws IOException
   */
  public synchronized void addPersistedDelegationToken(
      DelegationTokenIdentifier identifier, long expiryTime) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    int keyId = identifier.getMasterKeyId();
    DelegationKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG
          .warn("No KEY found for persisted identifier "
              + identifier.toString());
      return;
    }
    byte[] password = createPassword(identifier.getBytes(), dKey.getKey());
    if (identifier.getSequenceNumber() > this.delegationTokenSequenceNumber) {
      this.delegationTokenSequenceNumber = identifier.getSequenceNumber();
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new DelegationTokenInformation(expiryTime,
          password, getTrackingIdIfEnabled(identifier)));
    } else {
      throw new IOException(
          "Same delegation token being added twice; invalid entry in fsimage or editlogs");
    }
  }

  /**
   * Add a MasterKey to the list of keys.
   * 
   * @param key DelegationKey
   * @throws IOException
   */
  public synchronized void updatePersistedMasterKey(DelegationKey key)
      throws IOException {
    addKey(key);
  }
  
  /**
   * Update the token cache with renewal record in edit logs.
   * 
   * @param identifier DelegationTokenIdentifier of the renewed token
   * @param expiryTime
   * @throws IOException
   */
  public synchronized void updatePersistedTokenRenewal(
      DelegationTokenIdentifier identifier, long expiryTime) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    DelegationTokenInformation info = null;
    info = currentTokens.get(identifier);
    if (info != null) {
      int keyId = identifier.getMasterKeyId();
      byte[] password = createPassword(identifier.getBytes(), allKeys
          .get(keyId).getKey());
      currentTokens.put(identifier, new DelegationTokenInformation(expiryTime,
          password, getTrackingIdIfEnabled(identifier)));
    }
  }

  /**
   *  Update the token cache with the cancel record in edit logs
   *  
   *  @param identifier DelegationTokenIdentifier of the canceled token
   *  @throws IOException
   */
  public synchronized void updatePersistedTokenCancellation(
      DelegationTokenIdentifier identifier) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    currentTokens.remove(identifier);
  }
  
  /**
   * Returns the number of delegation keys currently stored.
   * @return number of delegation keys
   */
  public synchronized int getNumberOfKeys() {
    return allKeys.size();
  }

  /**
   * Private helper methods to save delegation keys and tokens in fsimage
   */
  private synchronized void saveCurrentTokens(DataOutputStream out,
      String sdPath) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.DELEGATION_TOKENS, sdPath);
    prog.beginStep(Phase.SAVING_CHECKPOINT, step);
    prog.setTotal(Phase.SAVING_CHECKPOINT, step, currentTokens.size());
    Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
    out.writeInt(currentTokens.size());
    Iterator<DelegationTokenIdentifier> iter = currentTokens.keySet()
        .iterator();
    while (iter.hasNext()) {
      DelegationTokenIdentifier id = iter.next();
      id.write(out);
      DelegationTokenInformation info = currentTokens.get(id);
      out.writeLong(info.getRenewDate());
      counter.increment();
    }
    prog.endStep(Phase.SAVING_CHECKPOINT, step);
  }
  
  /*
   * Save the current state of allKeys
   */
  private synchronized void saveAllKeys(DataOutputStream out, String sdPath)
      throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.DELEGATION_KEYS, sdPath);
    prog.beginStep(Phase.SAVING_CHECKPOINT, step);
    prog.setTotal(Phase.SAVING_CHECKPOINT, step, currentTokens.size());
    Counter counter = prog.getCounter(Phase.SAVING_CHECKPOINT, step);
    out.writeInt(allKeys.size());
    Iterator<Integer> iter = allKeys.keySet().iterator();
    while (iter.hasNext()) {
      Integer key = iter.next();
      allKeys.get(key).write(out);
      counter.increment();
    }
    prog.endStep(Phase.SAVING_CHECKPOINT, step);
  }
  
  /**
   * Private helper methods to load Delegation tokens from fsimage
   */
  private synchronized void loadCurrentTokens(DataInput in)
      throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.DELEGATION_TOKENS);
    prog.beginStep(Phase.LOADING_FSIMAGE, step);
    int numberOfTokens = in.readInt();
    prog.setTotal(Phase.LOADING_FSIMAGE, step, numberOfTokens);
    Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
    for (int i = 0; i < numberOfTokens; i++) {
      DelegationTokenIdentifier id = new DelegationTokenIdentifier();
      id.readFields(in);
      long expiryTime = in.readLong();
      addPersistedDelegationToken(id, expiryTime);
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }

  /**
   * Private helper method to load delegation keys from fsimage.
   * @param in
   * @throws IOException
   */
  private synchronized void loadAllKeys(DataInput in) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = new Step(StepType.DELEGATION_KEYS);
    prog.beginStep(Phase.LOADING_FSIMAGE, step);
    int numberOfKeys = in.readInt();
    prog.setTotal(Phase.LOADING_FSIMAGE, step, numberOfKeys);
    Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
    for (int i = 0; i < numberOfKeys; i++) {
      DelegationKey value = new DelegationKey();
      value.readFields(in);
      addKey(value);
      counter.increment();
    }
    prog.endStep(Phase.LOADING_FSIMAGE, step);
  }

  /**
   * Call namesystem to update editlogs for new master key.
   */
  @Override //AbstractDelegationTokenManager
  protected void logUpdateMasterKey(DelegationKey key)
      throws IOException {
    synchronized (noInterruptsLock) {
      // The edit logging code will fail catastrophically if it
      // is interrupted during a logSync, since the interrupt
      // closes the edit log files. Doing this inside the
      // above lock and then checking interruption status
      // prevents this bug.
      if (Thread.interrupted()) {
        throw new InterruptedIOException(
            "Interrupted before updating master key");
      }
      namesystem.logUpdateMasterKey(key);
    }
  }
  
  @Override //AbstractDelegationTokenManager
  protected void logExpireToken(final DelegationTokenIdentifier dtId)
      throws IOException {
    synchronized (noInterruptsLock) {
      // The edit logging code will fail catastrophically if it
      // is interrupted during a logSync, since the interrupt
      // closes the edit log files. Doing this inside the
      // above lock and then checking interruption status
      // prevents this bug.
      if (Thread.interrupted()) {
        throw new InterruptedIOException(
            "Interrupted before expiring delegation token");
      }
      namesystem.logExpireDelegationToken(dtId);
    }
  }

  /** A utility method for creating credentials. */
  public static Credentials createCredentials(final NameNode namenode,
      final UserGroupInformation ugi, final String renewer) throws IOException {
    final Token<DelegationTokenIdentifier> token = namenode.getRpcServer(
        ).getDelegationToken(new Text(renewer));
    if (token == null) {
      throw new IOException("Failed to get the token for " + renewer
          + ", user=" + ugi.getShortUserName());
    }

    final InetSocketAddress addr = namenode.getNameNodeAddress();
    SecurityUtil.setTokenService(token, addr);
    final Credentials c = new Credentials();
    c.addToken(new Text(ugi.getShortUserName()), token);
    return c;
  }
}
