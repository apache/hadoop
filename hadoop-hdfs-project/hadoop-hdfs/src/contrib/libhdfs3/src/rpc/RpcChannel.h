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

#ifndef _HDFS_LIBHDFS3_RPC_RPCCHANNEL_H_
#define _HDFS_LIBHDFS3_RPC_RPCCHANNEL_H_

#include "Atomic.h"
#include "DateTime.h"
#include "ExceptionInternal.h"
#include "IpcConnectionContext.pb.h"
#include "RpcCall.h"
#include "RpcChannelKey.h"
#include "RpcHeader.pb.h"
#include "RpcRemoteCall.h"
#include "SaslClient.h"
#include "SharedPtr.h"
#include "Thread.h"
#include "UnorderedMap.h"
#include "network/BufferedSocketReader.h"
#include "network/TcpSocket.h"

#include <google/protobuf/message.h>

namespace hdfs {
namespace internal {

class RpcClient;

using hadoop::common::RpcSaslProto;
using hadoop::common::RpcSaslProto_SaslAuth;

class RpcChannel {
public:
    /**
     * Destroy a channel
     */
    virtual ~RpcChannel() {
    }

    /**
     * The caller finished the rpc call,
     * this channel may be reused later if immediate is false.
     * @param immediate Do not reuse the channel any more if immediate is true.
     */
    virtual void close(bool immediate) = 0;

    /**
     * Invoke a rpc call.
     * @param call The call is to be invoked.
     * @return The remote call object.
     */
    virtual void invoke(const RpcCall &call) = 0;

    /**
     * Close the channel if it idle expired.
     * @return true if the channel idle expired.
     */
    virtual bool checkIdle() = 0;

    /**
     * Wait for all reference exiting.
     * The channel cannot be reused any more.
     * @pre RpcClient is not running.
     */
    virtual void waitForExit() = 0;

    /**
     * Add reference count to this channel.
     */
    virtual void addRef() = 0;
};

/**
 * RpcChannel represent a rpc connect to the server.
 */
class RpcChannelImpl: public RpcChannel {
public:
    /**
     * Construct a RpcChannelImpl instance.
     * @param k The key of this channel.
     */
    RpcChannelImpl(const RpcChannelKey &k, RpcClient &c);

    /**
     * Destroy a RpcChannelImpl instance.
     */
    ~RpcChannelImpl();

    /**
     * The caller finished the rpc call,
     * this channel may be reused later if immediate is false.
     * @param immediate Do not reuse the channel any more if immediate is true.
     */
    void close(bool immediate);

    /**
     * Invoke a rpc call.
     * @param call The call is to be invoked.
     * @return The remote call object.
     */
    void invoke(const RpcCall &call);

    /**
     * Close the channel if it idle expired.
     * @return true if the channel idle expired.
     */
    bool checkIdle();

    /**
     * Wait for all reference exiting.
     * The channel cannot be reused any more.
     * @pre RpcClient is not running.
     */
    void waitForExit();

    /**
     * Add reference count to this channel.
     */
    void addRef() {
        ++refs;
    }

private:
    /**
     * Setup the RPC connection.
     * @pre Already hold write lock.
     */
    void connect();

    /**
     * Cleanup all pending calls.
     * @param reason The reason to cancel the call.
     * @pre Already hold write lock.
     */
    void cleanupPendingCalls(exception_ptr reason);

    /**
     * Send rpc connect protocol header.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     */
    void sendConnectionHeader();

    /**
     * Send rpc connection protocol content.
     */
    void sendConnectionContent(const RpcAuth &auth);

    /**
     * Build rpc connect context.
     */
    void buildConnectionContext(
          hadoop::common::IpcConnectionContextProto &connectionContext,
          const RpcAuth &auth);

    /**
     * Send ping packet to server.
     * @throw HdfsNetworkException
     * @throw HdfsTimeout
     * @pre Caller should hold the write lock.
     */
    void sendPing();

    /**
     * Send the call message to rpc server.
     * @param remote The remote call.
     * @pre Already hold write lock.
     */
    void sendRequest(RpcRemoteCallPtr remote);

    /**
     * Issue a rpc call and check response.
     * Catch all recoverable error in this function
     *
     * @param remote The remote call
     */
    exception_ptr invokeInternal(RpcRemoteCallPtr remote);

    /**
     * Check response, block until get one response.
     * @pre Channel already hold read lock.
     */
    void checkOneResponse();

    /**
     * read and handle one response.
     * @pre Channel already hold read lock.
     */
    void readOneResponse(bool writeLock);

    /**
     * Get the call object with given id, and then remove it from pending call list.
     * @param id The id of the call object to be returned.
     * @return The call object with given id.
     * @throw HdfsIOException
     * @pre Channel already locked.
     */
    RpcRemoteCallPtr getPendingCall(int32_t id);

    /**
     * Check if there is data available for reading on socket.
     * @return true if response is available.
     */
    bool getResponse();

    /**
     * wake up one caller to check response.
     * @param id The call id which current caller handled.
     */
    void wakeupOneCaller(int32_t id);

    /**
     * shutdown the RPC connection since error.
     * @param reason The reason to cancel the call
     * @pre Already hold write lock.
     */
    void shutdown(exception_ptr reason);

    const RpcSaslProto_SaslAuth *createSaslClient(
        const ::google::protobuf::RepeatedPtrField<RpcSaslProto_SaslAuth> *auths);

    void sendSaslMessage(RpcSaslProto *msg, ::google::protobuf::Message *resp);

    std::string saslEvaluateToken(RpcSaslProto &response, bool serverIsDone);

    RpcAuth setupSaslConnection();

private:
    /**
     * Construct a RpcChannelImpl instance for test.
     * @param key The key of this channel.
     * @param sock The socket instance.
     * @param in The BufferedSocketReader instance build on sock.
     * @param client The RpcClient instance.
     */
    RpcChannelImpl(const RpcChannelKey &key, Socket *sock,
                   BufferedSocketReader *in, RpcClient &client);

private:
    atomic<int> refs;
    bool available;
    mutex readMut;
    mutex writeMut;
    RpcChannelKey key;
    RpcClient &client;
    shared_ptr<BufferedSocketReader> in;
    shared_ptr<SaslClient> saslClient;
    shared_ptr<Socket> sock;
    steady_clock::time_point lastActivity; // ping is a kind of activity, lastActivity will be updated after ping
    steady_clock::time_point lastIdle; // ping cannot change idle state. If there is still pending calls, lastIdle is always "NOW".
    unordered_map<int32_t, RpcRemoteCallPtr> pendingCalls;
};

}
}

#endif /* _HDFS_LIBHDFS3_RPC_RPCCHANNEL_H_ */
