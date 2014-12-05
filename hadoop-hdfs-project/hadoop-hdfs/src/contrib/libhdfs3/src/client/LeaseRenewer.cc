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

#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemImpl.h"
#include "LeaseRenewer.h"
#include "Logger.h"

#include <string>

#define DEFAULT_LEASE_RENEW_INTERVAL (60 * 1000)

using std::map;
using std::string;

namespace hdfs {
namespace internal {

LeaseRenewer::LeaseRenewer()
    : stop(true),
      filesystem(NULL),
      interval(DEFAULT_LEASE_RENEW_INTERVAL),
      openedOutputStream(0) {
}

LeaseRenewer::LeaseRenewer(FileSystemImpl *fs)
    : stop(true),
      filesystem(fs),
      interval(DEFAULT_LEASE_RENEW_INTERVAL),
      openedOutputStream(0) {
}

LeaseRenewer::~LeaseRenewer() {
    stop = true;
    cond.notify_all();

    if (worker.joinable()) {
        worker.join();
    }
}

int LeaseRenewer::getInterval() const {
    return interval;
}

void LeaseRenewer::setInterval(int interval) {
    this->interval = interval;
}

void LeaseRenewer::StartRenew() {
    lock_guard<mutex> lock(mut);

    ++openedOutputStream;

    if (stop && openedOutputStream > 0) {
        if (worker.joinable()) {
            worker.join();
        }

        stop = false;
        CREATE_THREAD(worker, bind(&LeaseRenewer::renewer, this));
    }
}

void LeaseRenewer::StopRenew() {
    lock_guard<mutex> lock(mut);
    assert(openedOutputStream > 0);
    openedOutputStream -= 1;
    /*
     * do not exit lease renew thread immediately if openedOutputStream is 0,
     * it will idle at most "interval" milliseconds before exit.
     */
}

void LeaseRenewer::renewer() {
    assert(stop == false);

    while (!stop) {
        try {
            unique_lock<mutex> lock(mut);
            cond.wait_for(lock, milliseconds(interval));

            if (stop || openedOutputStream <= 0) {
                break;
            }

            filesystem->renewLease();
            continue;
        } catch (const std::bad_alloc &e) {
            try {
                LOG(LOG_ERROR,
                    "Lease renewer will exit caused by out of memory");
            } catch (...) {
                /*
                 * We really can do nothing more here,
                 * ignore error and prevent the process from dying in background
                 * thread.
                 */
            }

            break;
        } catch (const std::exception &e) {
            LOG(LOG_ERROR,
                "Lease renewer will exit since unexpected exception: %s",
                e.what());
            break;
        }
    }

    stop = true;
}
}
}
