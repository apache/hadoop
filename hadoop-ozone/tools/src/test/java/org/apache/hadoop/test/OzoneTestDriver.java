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

package org.apache.hadoop.test;

import org.apache.hadoop.ozone.freon.Freon;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Driver for Ozone tests.
 */
public class OzoneTestDriver {

  private final ProgramDriver pgd;

  public OzoneTestDriver() {
    this(new ProgramDriver());
  }

  public OzoneTestDriver(ProgramDriver pgd) {
    this.pgd = pgd;
    try {
      pgd.addClass("freon", Freon.class,
          "Populates ozone with data.");
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }

  public void run(String[] args) {
    int exitCode = -1;
    try {
      exitCode = pgd.run(args);
    } catch(Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);
  }

  public static void main(String[] args){
    new OzoneTestDriver().run(args);
  }
}
