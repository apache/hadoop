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

#include "Exception.h"
#include "ExceptionInternal.h"
#include "Logger.h"
#include "SharedPtr.h"
#include "RpcClient.h"
#include "Thread.h"

#include <uuid/uuid.h>

namespace hdfs {
namespace internal {

once_flag RpcClient::once;
shared_ptr<RpcClient> RpcClient::client;

void RpcClient::createSinglten() {
    client = shared_ptr < RpcClient > (new RpcClientImpl());
}

RpcClient & RpcClient::getClient() {
    call_once(once, &RpcClientImpl::createSinglten);
    assert(client);
    return *client;
}

RpcClientImpl::RpcClientImpl() :
    cleaning(false), running(true), count(0) {
    uuid_t id;
    uuid_generate(id);
    clientId.resize(sizeof(uuid_t));
    memcpy(&clientId[0], id, sizeof(uuid_t));
#ifdef MOCK
    stub = NULL;
#endif
}

RpcClientImpl::~RpcClientImpl() {
    running = false;
    cond.notify_all();

    if (cleaner.joinable()) {
        cleaner.join();
    }

    close();
}

void RpcClientImpl::clean() {
    assert(cleaning);

    try {
        while (running) {
            try {
                unique_lock<mutex> lock(mut);
                cond.wait_for(lock, seconds(1));

                if (!running || allChannels.empty()) {
                    break;
                }

                unordered_map<RpcChannelKey, shared_ptr<RpcChannel> >::iterator s, e;
                e = allChannels.end();

                for (s = allChannels.begin(); s != e;) {
                    if (s->second->checkIdle()) {
                        s->second.reset();
                        s = allChannels.erase(s);
                    } else {
                        ++s;
                    }
                }
            } catch (const HdfsCanceled & e) {
                /*
                 * ignore cancel signal here.
                 */
            }
        }
    } catch (const hdfs::HdfsException & e) {
        LOG(LOG_ERROR, "RpcClientImpl's idle cleaner exit: %s",
            GetExceptionDetail(e));
    } catch (const std::exception & e) {
        LOG(LOG_ERROR, "RpcClientImpl's idle cleaner exit: %s", e.what());
    }

    cleaning = false;
}

void RpcClientImpl::close() {
    lock_guard<mutex> lock(mut);
    running = false;
    unordered_map<RpcChannelKey, shared_ptr<RpcChannel> >::iterator s, e;
    e = allChannels.end();

    for (s = allChannels.begin(); s != e; ++s) {
        s->second->waitForExit();
    }

    allChannels.clear();
}

bool RpcClientImpl::isRunning() {
    return running;
}

RpcChannel & RpcClientImpl::getChannel(const RpcAuth &auth,
            const RpcProtocolInfo &protocol, const RpcServerInfo &server,
            const RpcConfig &conf) {
    shared_ptr<RpcChannel> rc;
    RpcChannelKey key(auth, protocol, server, conf);

    try {
        lock_guard<mutex> lock(mut);

        if (!running) {
            THROW(hdfs::HdfsRpcException,
                  "Cannot Setup RPC channel to \"%s:%s\" since RpcClient is closing",
                  key.getServer().getHost().c_str(), key.getServer().getPort().c_str());
        }

        unordered_map<RpcChannelKey, shared_ptr<RpcChannel> >::iterator it;
        it = allChannels.find(key);

        if (it != allChannels.end()) {
            rc = it->second;
        } else {
            rc = createChannelInternal(key);
            allChannels[key] = rc;
        }

        rc->addRef();

        if (!cleaning) {
            cleaning = true;

            if (cleaner.joinable()) {
                cleaner.join();
            }

            CREATE_THREAD(cleaner, bind(&RpcClientImpl::clean, this));
        }
    } catch (const HdfsRpcException & e) {
        throw;
    } catch (...) {
        NESTED_THROW(HdfsRpcException,
                     "RpcClient failed to create a channel to \"%s:%s\"",
                     server.getHost().c_str(), server.getPort().c_str());
    }

    return *rc;
}

shared_ptr<RpcChannel> RpcClientImpl::createChannelInternal(
            const RpcChannelKey & key) {
    shared_ptr<RpcChannel> channel;
#ifdef MOCK

    if (stub) {
        channel = shared_ptr < RpcChannel > (stub->getChannel(key, *this));
    } else {
        channel = shared_ptr < RpcChannel > (new RpcChannelImpl(key, *this));
    }

#else
    channel = shared_ptr<RpcChannel>(new RpcChannelImpl(key, *this));
#endif
    return channel;
}

}
}
