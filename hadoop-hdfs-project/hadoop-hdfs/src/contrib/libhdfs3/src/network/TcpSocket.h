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

#ifndef _HDFS_LIBHDFS3_NETWORK_TCPSOCKET_H_
#define _HDFS_LIBHDFS3_NETWORK_TCPSOCKET_H_

#include "Socket.h"

namespace hdfs {
namespace internal {

/**
 * A tcp socket client
 */
class TcpSocketImpl: public Socket {
public:
    /**
     * Construct a Socket object.
     * @throw nothrow
     */
    TcpSocketImpl();

    /**
     * Destroy a TcpSocketImpl instance.
     */
    ~TcpSocketImpl();

    /**
     * Read data from socket.
     * If there is nothing can be read, the caller will be blocked.
     * @param buffer The buffer to store the data.
     * @param size The size of bytes to be read.
     * @return The size of data already read.
     * @throw HdfsNetworkException
     * @throw HdfsEndOfStream
     */
    int32_t read(char * buffer, int32_t size);

    /**
     * Read data from socket until get enough data.
     * If there is not enough data can be read, the caller will be blocked.
     * @param buffer The buffer to store the data.
     * @param size The size of bytes to be read.
     * @param timeout The timeout interval of this read operation, negative means infinite.
     * @throw HdfsNetworkException
     * @throw HdfsEndOfStream
     * @throw HdfsTimeout
     */
    void readFully(char * buffer, int32_t size, int timeout);

    /**
     * Send data to socket.
     * The caller will be blocked until send operation finished,
     *      but not guarantee that all data has been sent.
     * @param buffer The data to be sent.
     * @param size The size of bytes to be sent.
     * @return The size of data already be sent.
     * @throw HdfsNetworkException
     */
    int32_t write(const char * buffer, int32_t size);

    /**
     * Send all data to socket.
     * The caller will be blocked until all data has been sent.
     * @param buffer The data to be sent.
     * @param size The size of bytes to be sent.
     * @param timeout The timeout interval of this write operation, negative means infinite.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     */
    void writeFully(const char * buffer, int32_t size, int timeout);

    /**
     * Connection to a tcp server.
     * @param host The host of server.
     * @param port The port of server.
     * @param timeout The timeout interval of this read operation, negative means infinite.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     */
    void connect(const char * host, int port, int timeout);

    /**
     * Connection to a tcp server.
     * @param host The host of server.
     * @param port The port of server.
     * @param timeout The timeout interval of this read operation, negative means infinite.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     */
    void connect(const char * host, const char * port, int timeout);

    /**
     * Connection to a tcp server.
     * @param paddr The address of server.
     * @param host The host of server used in error message.
     * @param port The port of server used in error message.
     * @param timeout The timeout interval of this read operation, negative means infinite.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     */
    void connect(struct addrinfo * paddr, const char * host, const char * port,
                 int timeout);

    /**
     * Test if the socket can be read or written without blocking.
     * @param read Test socket if it can be read.
     * @param write Test socket if it can be written.
     * @param timeout Time timeout interval of this operation, negative means infinite.
     * @return Return true if the socket can be read or written without blocking, false on timeout.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     */
    bool poll(bool read, bool write, int timeout);

    /**
     * Set socket no delay mode.
     * @param enable If true, set socket into no delay mode, else delay mode.
     * @throw HdfsNetworkException
     */
    void setNoDelay(bool enable);

    /**
     * Set socket blocking mode.
     * @param enable If true, set socket into blocking mode, else non-block mode.
     * @throw HdfsNetworkException
     */
    void setBlockMode(bool enable);

    /**
     * Set socket linger timeout
     * @param timeout Linger timeout of the socket in millisecond, disable linger if it is less than 0.
     * @throw HdfsNetworkException
     */
    void setLingerTimeout(int timeout);

    /**
     * Shutdown and close the socket.
     * @throw nothrow
     */
    void close();

private:
    void setNoSigPipe();
    void setLingerTimeoutInternal(int timeout);
    void setSendTimeout(int timeout);

private:
    int sock;
    int lingerTimeout;
    std::string remoteAddr;  //used for error message
};

}
}

#endif /* _HDFS_LIBHDFS3_NETWORK_TCPSOCKET_H_ */
