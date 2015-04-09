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

#include "StackPrinter.h"

#include <boost/format.hpp>
#include <DbgHelp.h>
#pragma comment(lib, "dbghelp.lib")
#include <sstream>
#include <string>
#include <vector>
namespace hdfs {
namespace internal {

const std::string PrintStack(int skip, int maxDepth) {
    std::ostringstream ss;
    unsigned int i;
    std::vector<void *> stack;
    stack.resize(maxDepth);
    unsigned short frames;
    SYMBOL_INFO *symbol;
    HANDLE process;
    process = GetCurrentProcess();

    SymInitialize(process, NULL, TRUE);

    frames = CaptureStackBackTrace(0, maxDepth, &stack[0], NULL);
    symbol = (SYMBOL_INFO *)
        calloc(sizeof(SYMBOL_INFO) + 256 * sizeof(char), 1);
    symbol->MaxNameLen = 255;
    symbol->SizeOfStruct = sizeof(SYMBOL_INFO);

    for (i = 0; i < frames; i++) {
        SymFromAddr(process, (DWORD64)(stack[i]), 0, symbol);
        printf("%i: %s - 0x%0X\n",
            frames - i - 1, symbol->Name, symbol->Address);
        // We use boost here, this may not be optimized for performance.
        // TODO: fix this when we decide not to use boost for VS 2010
        ss << boost::format("%i: %s - 0x%0X\n")
            % (frames - i - 1) % symbol->Name % symbol->Address;
    }
    free(symbol);
    return ss.str();
}

}
}
