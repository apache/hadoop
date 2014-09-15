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

#include "lib/commons.h"
#include "lib/Streams.h"
#include "lib/FileSystem.h"
#include "lib/Buffers.h"
#include "lib/SpillInfo.h"

namespace NativeTask {

void SingleSpillInfo::deleteSpillFile() {
  if (path.length() > 0) {
    struct stat st;
    if (0 == stat(path.c_str(), &st)) {
      remove(path.c_str());
    }
  }
}

void SingleSpillInfo::writeSpillInfo(const std::string & filepath) {
  OutputStream * fout = FileSystem::getLocal().create(filepath, true);
  {
    ChecksumOutputStream dest = ChecksumOutputStream(fout, CHECKSUM_CRC32);
    AppendBuffer appendBuffer;
    appendBuffer.init(32 * 1024, &dest, "");
    uint64_t base = 0;

    for (size_t j = 0; j < this->length; j++) {
      IFileSegment * segment = &(this->segments[j]);
      const bool firstSegment = (j == 0);
      if (firstSegment) {
        appendBuffer.write_uint64_be(base);
        appendBuffer.write_uint64_be(segment->uncompressedEndOffset);
        appendBuffer.write_uint64_be(segment->realEndOffset);
      } else {
        appendBuffer.write_uint64_be(base + this->segments[j - 1].realEndOffset);
        appendBuffer.write_uint64_be(
            segment->uncompressedEndOffset - this->segments[j - 1].uncompressedEndOffset);
        appendBuffer.write_uint64_be(segment->realEndOffset - this->segments[j - 1].realEndOffset);
      }
    }
    appendBuffer.flush();
    uint32_t chsum = dest.getChecksum();
#ifdef SPILLRECORD_CHECKSUM_UINT
    chsum = bswap(chsum);
    fout->write(&chsum, sizeof(uint32_t));
#else
    uint64_t wtchsum = bswap64((uint64_t)chsum);
    fout->write(&wtchsum, sizeof(uint64_t));
#endif
  }
  fout->close();
  delete fout;
}

} // namespace NativeTask

