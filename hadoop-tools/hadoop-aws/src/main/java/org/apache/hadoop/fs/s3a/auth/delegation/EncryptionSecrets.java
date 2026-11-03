/*
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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_S3_ENCRYPTION_CONTEXT;

/**
 * Encryption options in a form which can serialized or marshalled as a hadoop
 * Writeable.
 *
 * Maintainers: For security reasons, don't print any of this.
 *
 * Note this design marshalls/unmarshalls its serialVersionUID
 * in its writable, which is used to compare versions.
 *
 * <i>Important.</i>
 * If the wire format is ever changed incompatibly,
 * update the serial version UID to ensure that older clients get safely
 * rejected.
 *
 * <i>Important</i>
 * Do not import any AWS SDK classes, directly or indirectly.
 * This is to ensure that S3A Token identifiers can be unmarshalled even
 * without that SDK.
 */
public class EncryptionSecrets implements Writable, Serializable {

  private static final Logger LOG =
          LoggerFactory.getLogger(EncryptionSecrets.class);

  public static final int MAX_SECRET_LENGTH = 2048;

  /**
   * Change this after any change to the payload: {@value}.
   */
  private static final long serialVersionUID = 8834417969966697162L;

  @VisibleForTesting
  public static final long SERIAL_VERSION_UID_CURRENT = serialVersionUID;

  /**
   * Serial version ID prior to {@link #encryptionContext} field being added: {@value}.
   */
  @VisibleForTesting
  public static final long SERIAL_VERSION_UID_1 = 1208329045511296375L;

  /**
   * Encryption algorithm to use: must match one in
   * {@link S3AEncryptionMethods}.
   */
  private String encryptionAlgorithm = "";

  /**
   * Encryption key: possibly sensitive information.
   */
  private String encryptionKey = "";

  /**
   * Encryption context: base64-encoded UTF-8 string.
   */
  private String encryptionContext = DEFAULT_S3_ENCRYPTION_CONTEXT;

  /**
   * This field isn't serialized/marshalled; it is rebuilt from the
   * encryptionAlgorithm field.
   */
  private transient S3AEncryptionMethods encryptionMethod =
      S3AEncryptionMethods.NONE;

  /**
   * Empty constructor, for use in marshalling.
   */
  public EncryptionSecrets() {
  }

  /**
   * Create a tuple of secrets. The encryption context is set to "".
   * This constructor is used in external implementations of S3A delegation
   * tokens, sp MUST be retained even if there is no use in our own
   * production code.
   * @param encryptionAlgorithm algorithm enumeration.
   * @param encryptionKey key/key reference.
   * @throws IOException failure to initialize.
   * @deprecated use {@link #EncryptionSecrets(S3AEncryptionMethods, String, String)}
   * which takes an encryption context.
   */
  public EncryptionSecrets(final S3AEncryptionMethods encryptionAlgorithm,
      final String encryptionKey) throws IOException {
    this(encryptionAlgorithm.getMethod(), encryptionKey,
        DEFAULT_S3_ENCRYPTION_CONTEXT);
  }

  /**
   * Create a 3/tuple of secrets.
   * @param encryptionAlgorithm algorithm enumeration.
   * @param encryptionKey key/key reference.
   * @param encryptionContext  base64-encoded string with the encryption context key-value pairs.
   * @throws IOException failure to initialize.
   */
  public EncryptionSecrets(final S3AEncryptionMethods encryptionAlgorithm,
      final String encryptionKey,
      final String encryptionContext) throws IOException {
    this(encryptionAlgorithm.getMethod(), encryptionKey, encryptionContext);
  }

  /**
   * Create a 3/tuple of secrets.
   * @param encryptionAlgorithm algorithm name
   * @param encryptionKey key/key reference.
   * @param encryptionContext  base64-encoded string with the encryption context key-value pairs.
   * @throws IOException failure to initialize.
   */
  public EncryptionSecrets(final String encryptionAlgorithm,
      final String encryptionKey,
      final String encryptionContext) throws IOException {
    this.encryptionAlgorithm = encryptionAlgorithm;
    this.encryptionKey = encryptionKey;
    this.encryptionContext = encryptionContext;
    init();
  }

  /**
   * Write out the encryption secrets.
   * @param out {@code DataOutput} to serialize this object into.
   * @throws IOException IO failure
   */
  @Override
  public void write(final DataOutput out) throws IOException {
    new LongWritable(serialVersionUID).write(out);
    Text.writeString(out, encryptionAlgorithm);
    Text.writeString(out, encryptionKey);
    Text.writeString(out, encryptionContext);
  }

  /**
   * Read in from the writable stream.
   * After reading, call {@link #init()}.
   * @param in {@code DataInput} to deserialize this object from.
   * @throws IOException failure to read/validate data.
   */
  @Override
  public void readFields(final DataInput in) throws IOException {
    final LongWritable version = new LongWritable();
    version.readFields(in);
    boolean readContext;

    final long versionId = version.get();
    if (versionId == SERIAL_VERSION_UID_1) {
      LOG.info("Unmarshalling Encryption Secrets from older client; "
          + "setting encryption context to \"\"");
      readContext = false;
    } else if (versionId == serialVersionUID) {
      readContext = true;
    } else {
      throw new DelegationTokenIOException(
          "Incompatible EncryptionSecrets version: " + versionId);
    }
    encryptionAlgorithm = Text.readString(in, MAX_SECRET_LENGTH);
    encryptionKey = Text.readString(in, MAX_SECRET_LENGTH);
    if (readContext) {
      encryptionContext = Text.readString(in);
    } else {
      encryptionContext = DEFAULT_S3_ENCRYPTION_CONTEXT;
    }
    init();
  }

  /**
   * For java serialization: read and then call {@link #init()}.
   * @param in input
   * @throws IOException IO problem
   * @throws ClassNotFoundException problem loading inner class.
   */
  private void readObject(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    init();
  }

  /**
   * Init all state, including after any read.
   * @throws IOException error rebuilding state.
   */
  private void init() throws IOException {
    encryptionMethod = S3AEncryptionMethods.getMethod(
        encryptionAlgorithm);
  }

  /**
   * Get the encryption algorithm.
   * @return the encryption algorithm.
   */
  public String getEncryptionAlgorithm() {
    return encryptionAlgorithm;
  }

  /**
   * Get the encryption key.
   * @return the encryption key.
   */
  public String getEncryptionKey() {
    return encryptionKey;
  }

  /**
   * Get the encryption context.
   * @return the encryption context.
   */
  public String getEncryptionContext() {
    return encryptionContext;
  }

  /**
   * Does this instance have encryption options?
   * That is: is the algorithm non-null.
   * @return true if there's an encryption algorithm.
   */
  public boolean hasEncryptionAlgorithm() {
    return StringUtils.isNotEmpty(encryptionAlgorithm);
  }

  /**
   * Does this instance have an encryption key?
   * @return true if there's an encryption key.
   */
  public boolean hasEncryptionKey() {
    return StringUtils.isNotEmpty(encryptionKey);
  }

  /**
   * Does this instance have an encryption context?
   * @return true if there's an encryption context.
   */
  public boolean hasEncryptionContext() {
    return StringUtils.isNotEmpty(encryptionContext);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final EncryptionSecrets that = (EncryptionSecrets) o;
    return Objects.equals(encryptionAlgorithm, that.encryptionAlgorithm)
        && Objects.equals(encryptionKey, that.encryptionKey)
        && Objects.equals(encryptionContext, that.encryptionContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(encryptionAlgorithm, encryptionKey, encryptionContext);
  }

  /**
   * Get the encryption method.
   * @return the encryption method
   */
  public S3AEncryptionMethods getEncryptionMethod() {
    return encryptionMethod;
  }

  /**
   * String function returns the encryption mode but not any other
   * secrets.
   * @return a string safe for logging.
   */
  @Override
  public String toString() {
    return S3AEncryptionMethods.NONE.equals(encryptionMethod)
        ? "(no encryption)"
        : encryptionMethod.getMethod();
  }
}
