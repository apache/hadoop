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

package org.apache.hadoop.ozone.web.ozShell.token;

import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.security.token.Token;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Executes cancelDelegationToken api.
 */
@Command(name = "cancel",
    description = "cancel a delegation token.")
public class CancelTokenHandler extends Handler {

  @CommandLine.Option(names = {"--token", "-t"},
      description = "file containing encoded token",
      defaultValue = "/tmp/token.txt",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private String tokenFile;

  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {
    OzoneAddress address = new OzoneAddress("");
    OzoneClient client = address.createClient(createOzoneConfiguration());

    if(!OzoneSecurityUtil.isSecurityEnabled(createOzoneConfiguration())) {
      System.err.println("Error:Token operations work only when security is " +
          "enabled. To enable security set ozone.security.enabled to true.");
      return null;
    }

    if (Files.notExists(Paths.get(tokenFile))) {
      System.err.println("Error:Cancel token operation failed as token file: "
          + tokenFile + " containing encoded token doesn't exist.");
      return null;
    }
    Token token = new Token();
    token.decodeFromUrlString(
        new String(Files.readAllBytes(Paths.get(tokenFile)),
            StandardCharsets.UTF_8));
    client.getObjectStore().cancelDelegationToken(token);
    return null;
  }
}
