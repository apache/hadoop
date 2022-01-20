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
package org.apache.hadoop.ipc;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY;

/**
 * A class defining the caller context for auditing coarse granularity
 * operations.
 *
 * This class is immutable.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class CallerContext {
  public static final Charset SIGNATURE_ENCODING = StandardCharsets.UTF_8;

  /** The caller context.
   *
   * It will be truncated if it exceeds the maximum allowed length.
   * The default length limit is
   * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT}
   */
  private final String context;

  /** The caller's signature for validation.
   *
   * The signature is optional. The null or empty signature will be abandoned.
   * If the signature exceeds the maximum allowed length, the caller context
   * will be abandoned. The default length limit is
   * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT}
   */
  private final byte[] signature;

  private CallerContext(String context, byte[] signature) {
    this.context = context;
    this.signature = signature;
  }

  public String getContext() {
    return context;
  }

  public byte[] getSignature() {
    return signature == null ?
        null : Arrays.copyOf(signature, signature.length);
  }

  @InterfaceAudience.Private
  public boolean isContextValid() {
    return context != null && !context.isEmpty();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(context).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj == this) {
      return true;
    } else if (obj.getClass() != getClass()) {
      return false;
    } else {
      CallerContext rhs = (CallerContext) obj;
      return new EqualsBuilder()
          .append(context, rhs.context)
          .append(signature, rhs.signature)
          .isEquals();
    }
  }

  @Override
  public String toString() {
    if (!isContextValid()) {
      return "";
    }
    String str = context;
    if (signature != null) {
      str += ":";
      str += new String(signature, SIGNATURE_ENCODING);
    }
    return str;
  }

  /**
   * The caller context related configs.
   * Use singleton to avoid creating new instance every time.
   */
  private static final class Configs {

    /** the maximum length limit for a context string*/
    private int contextMaxLen;
    /** the maximum length limit for a signature byte array*/
    private int signatureMaxLen;
    /** the filed separator for a context string */
    private String fieldSeparator;

    private static final String KEY_VALUE_SEPARATOR = ":";

    /**
     * The illegal separators include '\t', '\n', '='.
     * If the configured separator is illegal, use default
     * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT}.
     */
    private static final Set<String> ILLEGAL_SEPARATORS =
        Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("\t", "\n", "=")));

    private static volatile Configs INSTANCE;

    private Configs(Configuration conf) {
      this.init(conf);
    }

    private void init(Configuration conf) {
      this.contextMaxLen = conf.getInt(HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY,
          HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT);
      if (this.contextMaxLen < 0) {
        this.contextMaxLen = HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT;
      }

      this.signatureMaxLen =
          conf.getInt(HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY,
              HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT);
      if (this.signatureMaxLen < 0) {
        this.signatureMaxLen = HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT;
      }

      this.fieldSeparator = conf.get(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY,
          HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT);
      if (ILLEGAL_SEPARATORS.contains(fieldSeparator)) {
        this.fieldSeparator = HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT;
      }
    }

    private static Configs getInstance(Configuration conf) {
      if (INSTANCE == null) {
        synchronized (Configs.class) {
          if (INSTANCE == null) {
            if (conf == null) {
              conf = new Configuration();
            }
            INSTANCE = new Configs(conf);
          }
        }
      }
      return INSTANCE;
    }
  }

  /** The caller context builder. */
  public static final class Builder {

    private byte[] signature;
    private final Configs configs;
    private final StringBuilder ctx = new StringBuilder();

    public Builder(String context) {
      this(context, null);
    }

    public Builder(String context, Configuration conf) {
      if (isValidFiled(context)) {
        ctx.append(context);
      }
      configs = Configs.getInstance(conf);
    }

    @VisibleForTesting
    Builder(String context, Configuration conf, boolean refresh) {
      this(context, null);
      if (refresh) {
        configs.init(conf); // refresh configs for testing
      }
    }

    /**
     * Whether the field is valid.
     * @param field target field.
     * @return true if the field is not null or empty.
     */
    private boolean isValidFiled(String field) {
      return field != null && field.length() > 0;
    }

    public Builder setSignature(byte[] signature) {
      if (isSignatureValid(signature)) {
        this.signature = Arrays.copyOf(signature, signature.length);
      }
      return this;
    }

    /**
     * Whether the signature is valid.
     * @param signature target signature.
     * @return true if the signature is not null and the length of signature
     *         is greater than 0 and less than or equal to the length limit.
     */
    private boolean isSignatureValid(byte[] signature) {
      return signature != null && signature.length > 0
          && signature.length <= configs.signatureMaxLen;
    }

    /**
     * Append new field to the context if the filed is not empty and length
     * of the context is not exceeded limit.
     * @param field one of fields to append.
     * @return the builder.
     */
    public Builder append(String field) {
      if (isValidFiled(field) && !isContextLengthExceedLimit()) {
        if (ctx.length() > 0) {
          ctx.append(configs.fieldSeparator);
        }
        ctx.append(field);
      }
      return this;
    }

    private boolean isContextLengthExceedLimit() {
      return ctx.length() > configs.contextMaxLen;
    }

    /**
     * Append new field which contains key and value to the context if the
     * filed is not empty and length of the context is not exceeded limit.
     *
     * @param key the key of field.
     * @param value the value of field.
     * @return the builder.
     */
    public Builder append(String key, String value) {
      if (isValidFiled(key) && isValidFiled(value)
          && !isContextLengthExceedLimit()) {
        if (ctx.length() > 0) {
          ctx.append(configs.fieldSeparator);
        }
        ctx.append(key).append(Configs.KEY_VALUE_SEPARATOR).append(value);
      }
      return this;
    }

    /**
     * Append new field which contains key and value to the context
     * if the key("key:") is absent.
     * @param key the key of field.
     * @param value the value of field.
     * @return the builder.
     */
    public Builder appendIfAbsent(String key, String value) {
      if (ctx.toString().contains(key + Configs.KEY_VALUE_SEPARATOR)) {
        return this;
      }
      return append(key, value);
    }

    public CallerContext build() {
      String context = isContextLengthExceedLimit() ?
          ctx.substring(0, configs.contextMaxLen) :
          ctx.toString();

      return new CallerContext(context, signature);
    }
  }

  /**
   * The thread local current caller context.
   * <p>
   * Internal class for defered singleton idiom.
   * https://en.wikipedia.org/wiki/Initialization_on_demand_holder_idiom
   */
  private static final class CurrentCallerContextHolder {
    static final ThreadLocal<CallerContext> CALLER_CONTEXT =
        new InheritableThreadLocal<>();
  }

  public static CallerContext getCurrent() {
    return CurrentCallerContextHolder.CALLER_CONTEXT.get();
  }

  public static void setCurrent(CallerContext callerContext) {
    CurrentCallerContextHolder.CALLER_CONTEXT.set(callerContext);
  }
}
