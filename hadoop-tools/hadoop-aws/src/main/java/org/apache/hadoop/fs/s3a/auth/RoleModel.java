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

package org.apache.hadoop.fs.s3a.auth;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.JsonSerialization;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Jackson Role Model for Role Properties, for API clients and tests.
 *
 * Doesn't have complete coverage of the entire AWS IAM policy model;
 * don't expect to be able to parse everything.
 * It can generate simple models.
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-arn-format.html">Example S3 Policies</a>
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/api-permissions-reference.html">Dynamno DB Permissions</a>
 */
@InterfaceAudience.LimitedPrivate("Tests")
@InterfaceStability.Unstable
public class RoleModel {

  public static final String VERSION = "2012-10-17";

  public static final String BUCKET_RESOURCE_F = "arn:aws:s3:::%s/%s";


  private static final AtomicLong SID_COUNTER = new AtomicLong(0);


  private final JsonSerialization<Policy> serialization =
      new JsonSerialization<>(Policy.class, false, true);

  public RoleModel() {
    ObjectMapper mapper = serialization.getMapper();
    mapper.enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED);
  }

  public String toJson(Policy policy) throws JsonProcessingException {
    return serialization.toJson(policy);
  }

  /**
   * Statement ID factory.
   * @return a statement ID unique for this JVM's life.
   */
  public static String newSid() {
    SID_COUNTER.incrementAndGet();
    return SID_COUNTER.toString();
  }

  /**
   * Map a bool to an effect.
   * @param allowed is the statement to allow actions?
   * @return the appropriate effect.
   */
  public static Effects effect(final boolean allowed) {
    return allowed ? Effects.Allow : Effects.Deny;
  }

  /**
   * Create a resource.
   * @param bucket bucket
   * @param key key
   * @param addWildcard add a * to the tail of the key?
   * @return a resource for a statement.
   */
  @SuppressWarnings("StringConcatenationMissingWhitespace")
  public static String resource(String bucket, String key,
      boolean addWildcard) {
    return String.format(BUCKET_RESOURCE_F, bucket,
        key + (addWildcard ? "*" : ""));
  }

  /**
   * Given a path, return the S3 resource to it.
   * If {@code isDirectory} is true, a "/" is added to the path.
   * This is critical when adding wildcard permissions under
   * a directory, and also needed when locking down dir-as-file
   * and dir-as-directory-marker access.
   * @param path a path
   * @param isDirectory is this a directory?
   * @param addWildcard add a * to the tail of the key?
   * @return a resource for a statement.
   */
  public static String resource(Path path,
      final boolean isDirectory,
      boolean addWildcard) {
    String key = pathToKey(path);
    if (isDirectory && !key.isEmpty()) {
      key = key + "/";
    }
    return resource(path.toUri().getHost(), key, addWildcard);
  }

  /**
   * Given a directory path, return the S3 resource to it.
   * @param path a path
   * @return a resource for a statement.
   */
  public static String[] directory(Path path) {
    String host = path.toUri().getHost();
    String key = pathToKey(path);
    if (!key.isEmpty()) {
      return new String[] {
          resource(host, key + "/", true),
          resource(host, key, false),
          resource(host, key + "/", false),
      };
    } else {
      return new String[]{
          resource(host, key, true),
      };
    }
  }

  /**
   * Variant of {@link S3AFileSystem#pathToKey(Path)} which doesn't care
   * about working directories, so can be static and stateless.
   * @param path path to map
   * @return key or ""
   */
  public static String pathToKey(Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }
    return path.toUri().getPath().substring(1);
  }

  /**
   * Create a statement.
   * @param allow allow or deny
   * @param scope scope
   * @param actions actions
   * @return the formatted json statement
   */
  public static Statement statement(boolean allow,
      String scope,
      String... actions) {
    return new Statement(RoleModel.effect(allow))
        .addActions(actions)
        .addResources(scope);
  }

  /**
   * Create a statement.
   * If {@code isDirectory} is true, a "/" is added to the path.
   * This is critical when adding wildcard permissions under
   * a directory, and also needed when locking down dir-as-file
   * and dir-as-directory-marker access.
   * @param allow allow or deny
   * @param path path
   * @param isDirectory is this a directory?
   * @param actions action
   * @return the formatted json statement
   */
  public static Statement statement(
      final boolean allow,
      final Path path,
      final boolean isDirectory,
      final boolean wildcards,
      final String... actions) {
    return new Statement(RoleModel.effect(allow))
        .addActions(actions)
        .addResources(resource(path, isDirectory, wildcards));
  }

  /**
   * From a set of statements, create a policy.
   * @param statements statements
   * @return the policy
   */
  public static Policy policy(Statement... statements) {
    return new Policy(statements);
  }


  /**
   * Effect options.
   */
  public enum Effects {
    Allow,
    Deny
  }

  /**
   * Any element in a role.
   */
  public static abstract class RoleElt {

    protected RoleElt() {
    }

    /**
     * validation operation.
     */
    public void validate() {

    }
  }

  /**
   * A single statement.
   */
  public static class Statement extends RoleElt {

    @JsonProperty("Sid")
    public String sid = newSid();

    /**
     * Default effect is Deny; forces callers to switch on Allow.
     */
    @JsonProperty("Effect")
    public Effects effect;

    @JsonProperty("Action")
    public List<String> action = new ArrayList<>(1);

    @JsonProperty("Resource")
    public List<String> resource = new ArrayList<>(1);

    public Statement(final Effects effect) {
      this.effect = effect;
    }

    @Override
    public void validate() {
      checkNotNull(sid, "Sid");
      checkNotNull(effect, "Effect");
      checkState(!(action.isEmpty()), "Empty Action");
      checkState(!(resource.isEmpty()), "Empty Resource");
    }

    public Statement setAllowed(boolean f) {
      effect = effect(f);
      return this;
    }

    public Statement addActions(String... actions) {
      Collections.addAll(action, actions);
      return this;
    }

    public Statement addResources(String... resources) {
      Collections.addAll(resource, resources);
      return this;
    }

  }

  /**
   * A policy is one or more statements.
   */
  public static class Policy extends RoleElt {

    @JsonProperty("Version")
    public String version = VERSION;

    @JsonProperty("Statement")
    public List<Statement> statement;

    public Policy(final List<RoleModel.Statement> statement) {
      this.statement = statement;
    }

    public Policy(RoleModel.Statement... statements) {
      statement = Arrays.asList(statements);
    }

    /**
     * Validation includes validating all statements.
     */
    @Override
    public void validate() {
      checkNotNull(statement, "Statement");
      checkState(VERSION.equals(version), "Invalid Version: %s", version);
      statement.stream().forEach((a) -> a.validate());
    }

  }


}
