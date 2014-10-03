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

#include "BigEndian.h"
#include "datatransfer.pb.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "HWCrc32c.h"
#include "LocalBlockReader.h"
#include "SWCrc32c.h"

#include "hdfs.pb.h"

#include <inttypes.h>
#include <limits>

#define BMVERSION 1
#define BMVERSION_SIZE 2

#define HEADER_SIZE (BMVERSION_SIZE + \
      CHECKSUM_TYPE_SIZE + CHECKSUM_BYTES_PER_CHECKSUM_SIZE)

using hadoop::hdfs::ChecksumTypeProto;

namespace hdfs {
namespace internal {

LocalBlockReader::LocalBlockReader(const BlockLocalPathInfo &info,
        const ExtendedBlock &block, int64_t offset, bool verify,
        SessionConfig &conf, std::vector<char> &buffer) :
    verify(verify), pbuffer(NULL), pMetaBuffer(NULL), block(block),
      checksumSize(0), chunkSize(0), position(0), size(0), cursor(
        0), length(block.getNumBytes()),
          dataFilePath(info.getLocalBlockPath()), metaFilePath(
            info.getLocalMetaPath()), buffer(buffer) {
    exception_ptr lastError;

    try {
        if (conf.doUseMappedFile()) {
            metaFd = shared_ptr<MappedFileWrapper>(new MappedFileWrapper);
            dataFd = shared_ptr<MappedFileWrapper>(new MappedFileWrapper);
        } else {
            metaFd = shared_ptr<CFileWrapper>(new CFileWrapper);
            dataFd = shared_ptr<CFileWrapper>(new CFileWrapper);
        }

        if (!metaFd->open(metaFilePath)) {
            THROW(HdfsIOException,
                  "LocalBlockReader cannot open metadata file \"%s\", %s",
                  metaFilePath.c_str(), GetSystemErrorInfo(errno));
        }

        std::vector<char> header;
        pMetaBuffer = metaFd->read(header, HEADER_SIZE);
        int16_t version = ReadBigEndian16FromArray(&pMetaBuffer[0]);

        if (BMVERSION != version) {
            THROW(HdfsIOException,
                  "LocalBlockReader get an unmatched block, expected block "
                  "version %d, real version is %d",
                  BMVERSION, static_cast<int>(version));
        }

        switch (pMetaBuffer[BMVERSION_SIZE]) {
        case ChecksumTypeProto::CHECKSUM_NULL:
            this->verify = false;
            checksumSize = 0;
            metaFd.reset();
            break;

        case ChecksumTypeProto::CHECKSUM_CRC32:
            THROW(HdfsIOException,
                  "LocalBlockReader does not support CRC32 checksum.");
            break;

        case ChecksumTypeProto::CHECKSUM_CRC32C:
            if (HWCrc32c::available()) {
                checksum = shared_ptr<Checksum>(new HWCrc32c());
            } else {
                checksum = shared_ptr<Checksum>(new SWCrc32c());
            }

            chunkSize = ReadBigEndian32FromArray(
                            &pMetaBuffer[BMVERSION_SIZE + CHECKSUM_TYPE_SIZE]);
            checksumSize = sizeof(int32_t);
            break;

        default:
            THROW(HdfsIOException,
                  "LocalBlockReader cannot recognize checksum type: %d.",
                  static_cast<int>(pMetaBuffer[BMVERSION_SIZE]));
        }

        if (verify && chunkSize <= 0) {
            THROW(HdfsIOException,
                  "LocalBlockReader get an invalid checksum parameter, "
                  "bytes per chunk: %d.",
                  chunkSize);
        }

        if (!dataFd->open(dataFilePath)) {
            THROW(HdfsIOException,
                  "LocalBlockReader cannot open data file \"%s\", %s",
                  dataFilePath.c_str(), GetSystemErrorInfo(errno));
        }

        localBufferSize = conf.getLocalReadBufferSize();

        if (verify) {
            localBufferSize = (localBufferSize + chunkSize - 1) /
                (chunkSize * chunkSize);
        }

        if (offset > 0) {
            skip(offset);
        }
    } catch (...) {
        if (metaFd) {
            metaFd->close();
        }

        if (dataFd) {
            dataFd->close();
        }

        lastError = current_exception();
    }

    try {
        if (lastError != exception_ptr()) {
            rethrow_exception(lastError);
        }
    } catch (const HdfsCanceled &e) {
        throw;
    } catch (const HdfsException &e) {
        NESTED_THROW(HdfsIOException,
                 "Failed to construct LocalBlockReader for block: %s.",
                 block.toString().c_str());
    }
}

LocalBlockReader::~LocalBlockReader() {
}

void LocalBlockReader::readAndVerify(int32_t bufferSize) {
    assert(true == verify);
    assert(cursor % chunkSize == 0);
    int chunks = (bufferSize + chunkSize - 1) / chunkSize;
    pbuffer = dataFd->read(buffer, bufferSize);
    pMetaBuffer = metaFd->read(metaBuffer, chunks * checksumSize);

    for (int i = 0; i < chunks; ++i) {
        checksum->reset();
        int chunk = chunkSize;

        if (chunkSize * (i + 1) > bufferSize) {
            chunk = bufferSize % chunkSize;
        }

        checksum->update(&pbuffer[i * chunkSize], chunk);
        uint32_t target = ReadBigEndian32FromArray(
                              &pMetaBuffer[i * checksumSize]);

        if (target != checksum->getValue()) {
            THROW(ChecksumException,
                  "LocalBlockReader checksum not match for block file: %s",
                  dataFilePath.c_str());
        }
    }
}

int32_t LocalBlockReader::readInternal(char * buf, int32_t len) {
    int32_t todo = len;

    /*
     * read from buffer.
     */
    if (position < size) {
        todo = todo < size - position ? todo : size - position;
        memcpy(buf, &pbuffer[position], todo);
        position += todo;
        cursor += todo;
        return todo;
    }

    /*
     * end of block
     */
    todo = todo < length - cursor ? todo : length - cursor;

    if (0 == todo) {
        return 0;
    }

    /*
     * bypass the buffer
     */
    if (!verify
            && (todo > localBufferSize || todo == length - cursor)) {
        dataFd->copy(buf, todo);
        cursor += todo;
        return todo;
    }

    /*
     * fill buffer.
     */
    int bufferSize = localBufferSize;
    bufferSize = bufferSize < length - cursor ? bufferSize : length - cursor;
    assert(bufferSize > 0);

    if (verify) {
        readAndVerify(bufferSize);
    } else {
        pbuffer = dataFd->read(buffer, bufferSize);
    }

    position = 0;
    size = bufferSize;
    assert(position < size);
    return readInternal(buf, todo);
}

int32_t LocalBlockReader::read(char *buf, int32_t size) {
    try {
        return readInternal(buf, size);
    } catch (const HdfsCanceled &e) {
        throw;
    } catch (const HdfsException &e) {
        NESTED_THROW(HdfsIOException,
                     "LocalBlockReader failed to read from position: %" PRId64
                     ", length: %d, block: %s.",
                     cursor, size, block.toString().c_str());
    }

    assert(!"cannot reach here");
    return 0;
}

void LocalBlockReader::skip(int64_t len) {
    assert(len < length - cursor);

    try {
        int64_t todo = len;

        while (todo > 0) {
            /*
             * skip the data in buffer.
             */
            if (size - position > 0) {
                int batch = todo < size - position ? todo : size - position;
                position += batch;
                todo -= batch;
                cursor += batch;
                continue;
            }

            if (verify) {
                int64_t lastChunkSize = (cursor + todo) % chunkSize;
                cursor = (cursor + todo) / chunkSize * chunkSize;
                int64_t metaCursor = HEADER_SIZE
                                     + checksumSize * (cursor / chunkSize);
                metaFd->seek(metaCursor);
                todo = lastChunkSize;
            } else {
                cursor += todo;
                todo = 0;
            }

            if (cursor > 0) {
                dataFd->seek(cursor);
            }

            /*
             * fill buffer again and verify checksum
             */
            if (todo > 0) {
                assert(true == verify);
                int bufferSize = localBufferSize;
                bufferSize =
                    bufferSize < length - cursor ?
                    bufferSize : length - cursor;
                readAndVerify(bufferSize);
                position = 0;
                size = bufferSize;
            }
        }
    } catch (const HdfsCanceled &e) {
        throw;
    } catch (const HdfsException &e) {
        NESTED_THROW(HdfsIOException,
                     "LocalBlockReader failed to skip from position: %" PRId64
                     ", length: %d, block: %s.",
                     cursor, size, block.toString().c_str());
    }
}

}
}
