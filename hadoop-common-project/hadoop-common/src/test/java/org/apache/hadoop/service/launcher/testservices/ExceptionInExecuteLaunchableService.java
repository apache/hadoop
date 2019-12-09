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

package org.apache.hadoop.service.launcher.testservices;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.launcher.AbstractLaunchableService;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;
import org.apache.hadoop.util.ExitCodeProvider;

import java.io.IOException;
import java.util.List;

/**
 * Raise an exception in the execute() method; the exception type can
 * be configured from the CLI.
 */
public class ExceptionInExecuteLaunchableService extends
    AbstractLaunchableService {

  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.ExceptionInExecuteLaunchableService";
  public static final String ARG_THROW_SLE = "--SLE";
  public static final String ARG_THROW_IOE = "--IOE";
  public static final String ARG_THROWABLE = "--throwable";
  public static final String SLE_TEXT = "SLE raised in execute()";
  public static final String OTHER_EXCEPTION_TEXT = "Other exception";

  public static final String EXIT_IN_IOE_TEXT = "Exit in IOE";
  public static final int IOE_EXIT_CODE = 64;
  private ExType exceptionType = ExType.EX;

  public ExceptionInExecuteLaunchableService() {
    super("ExceptionInExecuteLaunchedService");
  }

  @Override
  public Configuration bindArgs(Configuration config, List<String> args) throws
      Exception {
    if (args.contains(ARG_THROW_SLE)) {
      exceptionType = ExType.SLE;
    } else if (args.contains(ARG_THROW_IOE)) {
      exceptionType = ExType.IOE;
    } else if (args.contains(ARG_THROWABLE)) {
      exceptionType = ExType.THROWABLE;
    }
    return super.bindArgs(config, args);
  }

  @Override
  public int execute() throws Exception {
    switch (exceptionType) {
    case SLE:
      throw new ServiceLaunchException(LauncherExitCodes.EXIT_OTHER_FAILURE,
          SLE_TEXT);
    case IOE:
      throw new IOECodedException();
    case THROWABLE:
      throw new OutOfMemoryError("OOM");
    case EX:
    default:
      throw new Exception(OTHER_EXCEPTION_TEXT);
    }
  }

  enum ExType {EX, SLE, IOE, THROWABLE}

  public static class IOECodedException extends IOException implements
      ExitCodeProvider {

    public IOECodedException() {
      super(EXIT_IN_IOE_TEXT);
    }

    @Override
    public int getExitCode() {
      return IOE_EXIT_CODE;
    }
  }
}
