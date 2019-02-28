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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/**
 * Token identifier for testing ABFS DT support; matched with
 * a service declaration so it can be unmarshalled.
 */
public class StubAbfsTokenIdentifier extends DelegationTokenIdentifier {

  public static final String ID = "StubAbfsTokenIdentifier";

  public static final int MAX_TEXT_LENGTH = 512;

  public static final Text TOKEN_KIND = new Text(ID);

  /** Canonical URI of the store. */
  private URI uri;

  /**
   * Timestamp of creation.
   * This is set to the current time; it will be overridden when
   * deserializing data.
   */
  private long created = System.currentTimeMillis();

  /**
   * This marshalled UUID can be used in testing to verify transmission,
   * and reuse; as it is printed you can see what is happending too.
   */
  private String uuid = UUID.randomUUID().toString();


  /**
   * This is the constructor used for deserialization, so there's
   * no need to fill in all values.
   */
  public StubAbfsTokenIdentifier() {
    super(TOKEN_KIND);
  }

  /**
   * Create.
   * @param uri owner UI
   * @param owner token owner
   * @param renewer token renewer
   */
  public StubAbfsTokenIdentifier(
      final URI uri,
      final Text owner,
      final Text renewer) {

    super(TOKEN_KIND, owner, renewer, new Text());
    this.uri = uri;
    Clock clock = Clock.systemDefaultZone();

    long now = clock.millis();
    Instant nowTime = Instant.ofEpochMilli(now);
    setIssueDate(now);
    setMaxDate(nowTime.plus(1, ChronoUnit.HOURS).toEpochMilli());
  }

  public static StubAbfsTokenIdentifier decodeIdentifier(final Token<?> token)
      throws IOException {
    StubAbfsTokenIdentifier id
        = (StubAbfsTokenIdentifier) token.decodeIdentifier();
    Preconditions.checkNotNull(id, "Null decoded identifier");
    return id;
  }

  public URI getUri() {
    return uri;
  }

  public long getCreated() {
    return created;
  }


  public String getUuid() {
    return uuid;
  }

  /**
   * Write state.
   * {@link org.apache.hadoop.io.Writable#write(DataOutput)}.
   * @param out destination
   * @throws IOException failure
   */
  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, uri.toString());
    Text.writeString(out, uuid);
    out.writeLong(created);
  }

  /**
   * Read state.
   * {@link org.apache.hadoop.io.Writable#readFields(DataInput)}.
   *
   * Note: this operation gets called in toString() operations on tokens, so
   * must either always succeed, or throw an IOException to trigger the
   * catch & downgrade. RuntimeExceptions (e.g. Preconditions checks) are
   * not to be used here for this reason.)
   *
   * @param in input stream
   * @throws IOException IO problems.
   */
  @Override
  public void readFields(final DataInput in)
      throws IOException {
    super.readFields(in);
    uri = URI.create(Text.readString(in, MAX_TEXT_LENGTH));
    uuid = Text.readString(in, MAX_TEXT_LENGTH);
    created = in.readLong();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbfsIDBTokenIdentifier{");
    sb.append("uri=").append(uri);
    sb.append(", uuid='").append(uuid).append('\'');
    sb.append(", created='").append(new Date(created)).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final StubAbfsTokenIdentifier that = (StubAbfsTokenIdentifier) o;
    return created == that.created
        && uri.equals(that.uri)
        && uuid.equals(that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), uri, uuid);
  }
}
