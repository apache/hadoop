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

#ifndef _HDFS_LIBHDFS3_NETWORK_BUFFEREDSOCKET_H_
#define _HDFS_LIBHDFS3_NETWORK_BUFFEREDSOCKET_H_

#include <vector>
#include <stdint.h>
#include <cstdlib>

#include "Socket.h"

namespace hdfs {
namespace internal {

/**
 * A warper of Socket, read big endian int and varint from socket.
 */
class BufferedSocketReader {
public:
    virtual ~BufferedSocketReader() {
    }

    /**
     * Read data from socket, if there is data buffered, read from buffer first.
     * If there is nothing can be read, the caller will be blocked.
     * @param b The buffer used to receive data.
     * @param s The size of bytes to be read.
     * @return The size of data already read.
     * @throw HdfsNetworkException
     * @throw HdfsEndOfStream
     */
    virtual int32_t read(char * b, int32_t s) = 0;

    /**
     * Read data form socket, if there is data buffered, read from buffer first.
     * If there is not enough data can be read, the caller will be blocked.
     * @param b The buffer used to receive data.
     * @param s The size of bytes to read.
     * @param timeout The timeout interval of this read operation, negative means infinite.
     * @throw HdfsNetworkException
     * @throw HdfsEndOfStream
     * @throw HdfsTimeout
     */
    virtual void readFully(char * b, int32_t s, int timeout) = 0;

    /**
     * Read a 32 bit big endian integer from socket.
     * If there is not enough data can be read, the caller will be blocked.
     * @param timeout The timeout interval of this read operation, negative means infinite.
     * @return A 32 bit integer.
     * @throw HdfsNetworkException
     * @throw HdfsEndOfStream
     * @throw HdfsTimeout
     */
    virtual int32_t readBigEndianInt32(int timeout) = 0;

    /**
     * Read a variable length encoding 32bit integer from socket.
     * If there is not enough data can be read, the caller will be blocked.
     * @param timeout The timeout interval of this read operation, negative means infinite.
     * @return A 32 bit integer.
     * @throw HdfsNetworkException
     * @throw HdfsEndOfStream
     * @throw HdfsTimeout
     */
    virtual int32_t readVarint32(int timeout) = 0;

    /**
     * Test if the socket can be read without blocking.
     * @param timeout Time timeout interval of this operation, negative means infinite.
     * @return Return true if the socket can be read without blocking, false on timeout.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     */
    virtual bool poll(int timeout) = 0;

};

/**
 * An implement of BufferedSocketReader.
 */
class BufferedSocketReaderImpl: public BufferedSocketReader {
public:
    BufferedSocketReaderImpl(Socket & s);

    int32_t read(char * b, int32_t s);

    void readFully(char * b, int32_t s, int timeout);

    int32_t readBigEndianInt32(int timeout);

    int32_t readVarint32(int timeout);

    bool poll(int timeout);

private:
    //for test
    BufferedSocketReaderImpl(Socket & s, const std::vector<char> & buffer) :
        cursor(0), size(buffer.size()), sock(s), buffer(buffer) {
    }

private:
    int32_t cursor;
    int32_t size;
    Socket & sock;
    std::vector<char> buffer;
};

}
}

#endif /* _HDFS_LIBHDFS3_NETWORK_BUFFEREDSOCKET_H_ */
