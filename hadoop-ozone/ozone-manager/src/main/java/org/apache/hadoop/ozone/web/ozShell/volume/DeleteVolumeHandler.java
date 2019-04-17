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

package org.apache.hadoop.ozone.web.ozShell.volume;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Executes deleteVolume call for the shell.
 */
@Command(name = "delete",
    description = "deletes a volume if it is empty")
public class DeleteVolumeHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_VOLUME_URI_DESCRIPTION)
  private String uri;

  /**
   * Executes the delete volume call.
   */
  @Override
  public Void call() throws Exception {

    OzoneAddress address = new OzoneAddress(uri);
    address.ensureVolumeAddress();
    OzoneClient client = address.createClient(createOzoneConfiguration());

    String volumeName = address.getVolumeName();

    if (isVerbose()) {
      System.out.printf("Volume name : %s%n", volumeName);
    }

    client.getObjectStore().deleteVolume(volumeName);
    System.out.printf("Volume %s is deleted%n", volumeName);
    return null;
  }
}
