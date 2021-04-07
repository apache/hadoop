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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * A HDFS specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
@InterfaceAudience.Private
public class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(DelegationTokenSecretManager.class);
  
  private final FSNamesystem namesystem;
  private final SerializerCompat serializerCompat = new SerializerCompat();

  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, FSNamesystem namesystem) {
    this(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval, false,
        namesystem);
  }

  /**
   * Create a secret manager
   * @param delegationKeyUpdateInterval the number of milliseconds for rolling
   *        new secret keys.
   * @param delegationTokenMaxLifetime the maximum lifetime of the delegation
   *        tokens in milliseconds
   * @param delegationTokenRenewInterval how often the tokens must be renewed
   *        in milliseconds
   * @param delegationTokenRemoverScanInterval how often the tokens are scanned
   *        for expired tokens in milliseconds
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
  
  @Override
  public byte[] retriableRetrievePassword(DelegationTokenIdentifier identifier)
      throws InvalidToken, StandbyException, RetriableException, IOException {
    namesystem.checkOperation(OperationCategory.READ);
    try {
      return super.retrievePassword(identifier);
    } catch (InvalidToken it) {
      if (namesystem.inTransitionToActive()) {
        // if the namesystem is currently in the middle of transition to 
        // active state, let client retry since the corresponding editlog may 
        // have not been applied yet
        throw new RetriableException(it);
      } else {
        throw it;
      }
    }
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
  public synchronized void loadSecretManagerStateCompat(DataInput in)
      throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't load state from image in a running SecretManager.");
    }
    serializerCompat.load(in);
  }

  public static class SecretManagerState {
    public final SecretManagerSection section;
    public final List<SecretManagerSection.DelegationKey> keys;
    public final List<SecretManagerSection.PersistToken> tokens;

    public SecretManagerState(
        SecretManagerSection s,
        List<SecretManagerSection.DelegationKey> keys,
        List<SecretManagerSection.PersistToken> tokens) {
      this.section = s;
      this.keys = keys;
      this.tokens = tokens;
    }
  }

  public synchronized void loadSecretManagerState(SecretManagerState state, Counter counter)
      throws IOException {
    Preconditions.checkState(!running,
        "Can't load state from image in a running SecretManager.");

    currentId = state.section.getCurrentId();
    delegationTokenSequenceNumber = state.section.getTokenSequenceNumber();
    for (SecretManagerSection.DelegationKey k : state.keys) {
      addKey(new DelegationKey(k.getId(), k.getExpiryDate(), k.hasKey() ? k
          .getKey().toByteArray() : null));
    }

    for (SecretManagerSection.PersistToken t : state.tokens) {
      DelegationTokenIdentifier id = new DelegationTokenIdentifier(new Text(
          t.getOwner()), new Text(t.getRenewer()), new Text(t.getRealUser()));
      id.setIssueDate(t.getIssueDate());
      id.setMaxDate(t.getMaxDate());
      id.setSequenceNumber(t.getSequenceNumber());
      id.setMasterKeyId(t.getMasterKeyId());
      addPersistedDelegationToken(id, t.getExpiryDate());
      counter.increment();
    }
  }

  /**
   * Store the current state of the SecretManager for persistence
   *
   * @param out Output stream for writing into fsimage.
   * @param sdPath String storage directory path
   * @throws IOException
   */
  public synchronized void saveSecretManagerStateCompat(DataOutputStream out,
      String sdPath) throws IOException {
    serializerCompat.save(out, sdPath);
  }

  public synchronized SecretManagerState saveSecretManagerState() {
    SecretManagerSection s = SecretManagerSection.newBuilder()
        .setCurrentId(currentId)
        .setTokenSequenceNumber(delegationTokenSequenceNumber)
        .setNumKeys(allKeys.size()).setNumTokens(currentTokens.size()).build();
    ArrayList<SecretManagerSection.DelegationKey> keys = Lists
        .newArrayListWithCapacity(allKeys.size());
    ArrayList<SecretManagerSection.PersistToken> tokens = Lists
        .newArrayListWithCapacity(currentTokens.size());

    for (DelegationKey v : allKeys.values()) {
      SecretManagerSection.DelegationKey.Builder b = SecretManagerSection.DelegationKey
          .newBuilder().setId(v.getKeyId()).setExpiryDate(v.getExpiryDate());
      if (v.getEncodedKey() != null) {
        b.setKey(ByteString.copyFrom(v.getEncodedKey()));
      }
      keys.add(b.build());
    }

    for (Entry<DelegationTokenIdentifier, DelegationTokenInformation> e : currentTokens
        .entrySet()) {
      DelegationTokenIdentifier id = e.getKey();
      SecretManagerSection.PersistToken.Builder b = SecretManagerSection.PersistToken
          .newBuilder().setOwner(id.getOwner().toString())
          .setRenewer(id.getRenewer().toString())
          .setRealUser(id.getRealUser().toString())
          .setIssueDate(id.getIssueDate()).setMaxDate(id.getMaxDate())
          .setSequenceNumber(id.getSequenceNumber())
          .setMasterKeyId(id.getMasterKeyId())
          .setExpiryDate(e.getValue().getRenewDate());
      tokens.add(b.build());
    }

    return new SecretManagerState(s, keys, tokens);
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
   * @param expiryTime expirty time in milliseconds
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
   * Call namesystem to update editlogs for new master key.
   */
  @Override //AbstractDelegationTokenManager
  protected void logUpdateMasterKey(DelegationKey key)
      throws IOException {
    try {
      // The edit logging code will fail catastrophically if it
      // is interrupted during a logSync, since the interrupt
      // closes the edit log files. Doing this inside the
      // fsn lock will prevent being interrupted when stopping
      // the secret manager.
      namesystem.readLockInterruptibly();
      try {
        // this monitor isn't necessary if stopped while holding write lock
        // but for safety, guard against a stop with read lock.
        synchronized (noInterruptsLock) {
          if (Thread.currentThread().isInterrupted()) {
            return; // leave flag set so secret monitor exits.
          }
          namesystem.logUpdateMasterKey(key);
        }
      } finally {
        namesystem.readUnlock();
      }
    } catch (InterruptedException ie) {
      // AbstractDelegationTokenManager may crash if an exception is thrown.
      // The interrupt flag will be detected when it attempts to sleep.
      Thread.currentThread().interrupt();
    }
  }
  
  @Override //AbstractDelegationTokenManager
  protected void logExpireToken(final DelegationTokenIdentifier dtId)
      throws IOException {
    try {
      // The edit logging code will fail catastrophically if it
      // is interrupted during a logSync, since the interrupt
      // closes the edit log files. Doing this inside the
      // fsn lock will prevent being interrupted when stopping
      // the secret manager.
      namesystem.readLockInterruptibly();
      try {
        // this monitor isn't necessary if stopped while holding write lock
        // but for safety, guard against a stop with read lock.
        synchronized (noInterruptsLock) {
          if (Thread.currentThread().isInterrupted()) {
            return; // leave flag set so secret monitor exits.
          }
          namesystem.logExpireDelegationToken(dtId);
        }
      } finally {
        namesystem.readUnlock();
      }
    } catch (InterruptedException ie) {
      // AbstractDelegationTokenManager may crash if an exception is thrown.
      // The interrupt flag will be detected when it attempts to sleep.
      Thread.currentThread().interrupt();
    }
  }

  /** A utility method for creating credentials. */
  public static Credentials createCredentials(final NameNode namenode,
      final UserGroupInformation ugi, final String renewer) throws IOException {
    final Token<DelegationTokenIdentifier> token = namenode.getRpcServer(
        ).getDelegationToken(new Text(renewer));
    if (token == null) {
      return null;
    }

    final InetSocketAddress addr = namenode.getNameNodeAddress();
    SecurityUtil.setTokenService(token, addr);
    final Credentials c = new Credentials();
    c.addToken(new Text(ugi.getShortUserName()), token);
    return c;
  }

  private final class SerializerCompat {
    private void load(DataInput in) throws IOException {
      currentId = in.readInt();
      loadAllKeys(in);
      delegationTokenSequenceNumber = in.readInt();
      loadCurrentTokens(in);
    }

    private void save(DataOutputStream out, String sdPath) throws IOException {
      out.writeInt(currentId);
      saveAllKeys(out, sdPath);
      out.writeInt(delegationTokenSequenceNumber);
      saveCurrentTokens(out, sdPath);
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
     * @throws IOException on error
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
  }

}
