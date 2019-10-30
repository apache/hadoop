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
package org.apache.hadoop.tools.dynamometer.workloadgenerator.audit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * UserCommandKey is a {@link org.apache.hadoop.io.Writable} used as a composite
 * key combining the user id and type of a replayed command. It is used as the
 * output key for AuditReplayMapper and the keys for AuditReplayReducer.
 */
public class UserCommandKey implements WritableComparable {
  private Text user;
  private Text command;

  public UserCommandKey() {
    user = new Text();
    command = new Text();
  }

  public UserCommandKey(Text user, Text command) {
    this.user = user;
    this.command = command;
  }

  public UserCommandKey(String user, String command) {
    this.user = new Text(user);
    this.command = new Text(command);
  }

  public String getUser() {
    return user.toString();
  }

  public String getCommand() {
    return command.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    user.write(out);
    command.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    user.readFields(in);
    command.readFields(in);
  }

  @Override
  public int compareTo(@Nonnull Object o) {
    return toString().compareTo(o.toString());
  }

  @Override
  public String toString() {
    return getUser() + "," + getCommand();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UserCommandKey that = (UserCommandKey) o;
    return getUser().equals(that.getUser())
        && getCommand().equals(that.getCommand());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getUser(), getCommand());
  }
}
