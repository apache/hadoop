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

#include <signal.h>

#ifndef __CYGWIN__
#include <execinfo.h>
#endif

#include <stdexcept>
#include "lib/commons.h"
#include "lib/Buffers.h"
#include "lib/FileSystem.h"
#include "lib/NativeObjectFactory.h"
#include "test_commons.h"

extern "C" {

static void handler(int sig);

// TODO: just for debug, should be removed
void handler(int sig) {
  void *array[10];
  size_t size;

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);

#ifndef __CYGWIN__
  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  backtrace_symbols_fd(array, size, 2);
#endif

  exit(1);
}
}

typedef char * CString;

namespace NativeTask {

int DoMain(int argc, char** argv) {
  signal(SIGSEGV, handler);
  CString * newArgv = new CString[argc + 1];
  memcpy(newArgv, argv, argc * sizeof(CString));

  bool gen = false;
  if (argc > 1) {
    if (string("perf") == newArgv[1]) {
      newArgv[1] = (char *)"--gtest_filter=Perf.*";
    } else if (string("noperf") == newArgv[1]) {
      newArgv[1] = (char *)"--gtest_filter=-Perf.*";
    } else if (string("gen") == newArgv[1]) {
      gen = true;
    }
  }
  testing::InitGoogleTest(&argc, newArgv);
  if (argc > 0) {
    int skip = gen ? 2 : 1;
    TestConfig.parse(argc - skip, (const char **)(newArgv + skip));
  }
  delete [] newArgv;
  try {
    if (gen == true) {
      string type = TestConfig.get("generate.type", "word");
      string codec = TestConfig.get("generate.codec", "");
      int64_t len = TestConfig.getInt("generate.length", 1024);
      string temp;
      GenerateKVTextLength(temp, len, type);
      if (codec.length() == 0) {
        fprintf(stdout, "%s", temp.c_str());
      } else {
        OutputStream * fout = FileSystem::getLocal().create("/dev/stdout");
        AppendBuffer app = AppendBuffer();
        app.init(128 * 1024, fout, codec);
        app.write(temp.data(), temp.length());
        fout->close();
        delete fout;
      }
      NativeObjectFactory::Release();
      return 0;
    } else {
      int ret = RUN_ALL_TESTS();
      NativeObjectFactory::Release();
      return ret;
    }
  } catch (std::exception & e) {
    fprintf(stderr, "Exception: %s", e.what());
    NativeObjectFactory::Release();
    return 1;
  }
}

} // namespace NativeTask


int main(int argc, char ** argv) {
  return NativeTask::DoMain(argc, argv);
}
