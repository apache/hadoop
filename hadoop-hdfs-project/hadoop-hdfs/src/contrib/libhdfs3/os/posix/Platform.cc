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
#include "platform.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Logger.h"
#include "UnorderedMap.h"
#include "client/KerberosName.h"
#include "client/UserInfo.h"
#include "network/Syscall.h"
#include "network/TcpSocket.h"
#include "server/NamenodeProxy.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cassert>
#include <climits>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <inttypes.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pwd.h>
#include <regex.h>
#include <stdint.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include <sstream>

namespace hdfs {
namespace internal {

/* InputStreamImpl.cc */
unordered_set<std::string> BuildLocalAddrSet() {
    unordered_set<std::string> set;
    struct ifaddrs *ifAddr = NULL;
    struct ifaddrs *pifAddr = NULL;
    struct sockaddr *addr;

    if (getifaddrs(&ifAddr)) {
        THROW(HdfsNetworkException,
              "InputStreamImpl: cannot get local network interface: %s",
              GetSystemErrorInfo(errno));
    }

    try {
        std::vector<char> host;
        const char *pHost;
        host.resize(INET6_ADDRSTRLEN + 1);

        for (pifAddr = ifAddr; pifAddr != NULL; pifAddr = pifAddr->ifa_next) {
            addr = pifAddr->ifa_addr;
            memset(&host[0], 0, INET6_ADDRSTRLEN + 1);

            if (addr->sa_family == AF_INET) {
                pHost = inet_ntop(
                    addr->sa_family,
                    &(reinterpret_cast<struct sockaddr_in *>(addr))->sin_addr,
                    &host[0], INET6_ADDRSTRLEN);
            } else if (addr->sa_family == AF_INET6) {
                pHost = inet_ntop(
                    addr->sa_family,
                    &(reinterpret_cast<struct sockaddr_in6 *>(addr))->sin6_addr,
                    &host[0], INET6_ADDRSTRLEN);
            } else {
                continue;
            }

            if (NULL == pHost) {
                THROW(HdfsNetworkException,
                      "InputStreamImpl: cannot get convert network address "
                      "to textual form: %s",
                      GetSystemErrorInfo(errno));
            }

            set.insert(pHost);
        }

        /*
         * add hostname.
         */
        long hostlen = sysconf(_SC_HOST_NAME_MAX);
        host.resize(hostlen + 1);

        if (gethostname(&host[0], host.size())) {
            THROW(HdfsNetworkException,
                  "InputStreamImpl: cannot get hostname: %s",
                  GetSystemErrorInfo(errno));
        }

        set.insert(&host[0]);
    } catch (...) {
        if (ifAddr != NULL) {
            freeifaddrs(ifAddr);
        }

        throw;
    }

    if (ifAddr != NULL) {
        freeifaddrs(ifAddr);
    }

    return set;
}

/* TpcSocket.cc */
void TcpSocketImpl::setBlockMode(bool enable) {
    int flag;
    flag = syscalls::fcntl(sock, F_GETFL, 0);

    if (-1 == flag) {
        THROW(HdfsNetworkException, "Get socket flag failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }

    flag = enable ? (flag & ~O_NONBLOCK) : (flag | O_NONBLOCK);

    if (-1 == syscalls::fcntl(sock, F_SETFL, flag)) {
        THROW(HdfsNetworkException, "Set socket flag failed for remote "
              "node %s: %s", remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }
}

/* NamenodeProxy.cc */
static uint32_t GetInitNamenodeIndex(const std::string &id) {
    std::string path = "/tmp/";
    path += id;
    int fd;
    uint32_t index = 0;
    /*
     * try create the file
     */
    fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);

    if (fd < 0) {
        if (errno == EEXIST) {
            /*
             * the file already exist, try to open it
             */
            fd = open(path.c_str(), O_RDONLY);
        } else {
            /*
             * failed to create, do not care why
             */
            return 0;
        }
    } else {
        if (0 != flock(fd, LOCK_EX)) {
            /*
             * failed to lock
             */
            close(fd);
            return index;
        }

        /*
         * created file, initialize it with 0
         */
        write(fd, &index, sizeof(index));
        flock(fd, LOCK_UN);
        close(fd);
        return index;
    }

    /*
     * the file exist, read it.
     */
    if (fd >= 0) {
        if (0 != flock(fd, LOCK_SH)) {
            /*
             * failed to lock
             */
            close(fd);
            return index;
        }

        if (sizeof(index) != read(fd, &index, sizeof(index))) {
            /*
             * failed to read, do not care why
             */
            index = 0;
        }

        flock(fd, LOCK_UN);
        close(fd);
    }

    return index;
}

static void SetInitNamenodeIndex(const std::string &id, uint32_t index) {
    std::string path = "/tmp/";
    path += id;
    int fd;
    /*
     * try open the file for write
     */
    fd = open(path.c_str(), O_WRONLY);

    if (fd > 0) {
        if (0 != flock(fd, LOCK_EX)) {
            /*
             * failed to lock
             */
            close(fd);
            return;
        }

        write(fd, &index, sizeof(index));
        flock(fd, LOCK_UN);
        close(fd);
    }
}

/* KerberosName.cc */
static void HandleRegError(int rc, regex_t *comp) {
    std::vector<char> buffer;
    size_t size = regerror(rc, comp, NULL, 0);
    buffer.resize(size + 1);
    regerror(rc, comp, &buffer[0], buffer.size());
    THROW(HdfsIOException,
        "KerberosName: Failed to parse Kerberos principal.");
}

void KerberosName::parse(const std::string &principal) {
    int rc;
    static const char * pattern = "([^/@]*)(/([^/@]*))?@([^/@]*)";
    regex_t comp;
    regmatch_t pmatch[5];

    if (principal.empty()) {
        return;
    }

    memset(&comp, 0, sizeof(regex_t));
    rc = regcomp(&comp, pattern, REG_EXTENDED);

    if (rc) {
        HandleRegError(rc, &comp);
    }

    try {
        memset(pmatch, 0, sizeof(pmatch));
        rc = regexec(&comp, principal.c_str(),
                     sizeof(pmatch) / sizeof(pmatch[1]), pmatch, 0);

        if (rc && rc != REG_NOMATCH) {
            HandleRegError(rc, &comp);
        }

        if (rc == REG_NOMATCH) {
            if (principal.find('@') != principal.npos) {
                THROW(HdfsIOException,
                      "KerberosName: Malformed Kerberos name: %s",
                      principal.c_str());
            } else {
                name = principal;
            }
        } else {
            if (pmatch[1].rm_so != -1) {
                name = principal.substr(pmatch[1].rm_so,
                                        pmatch[1].rm_eo - pmatch[1].rm_so);
            }

            if (pmatch[3].rm_so != -1) {
                host = principal.substr(pmatch[3].rm_so,
                                        pmatch[3].rm_eo - pmatch[3].rm_so);
            }

            if (pmatch[4].rm_so != -1) {
                realm = principal.substr(pmatch[4].rm_so,
                                         pmatch[4].rm_eo - pmatch[4].rm_so);
            }
        }
    } catch (...) {
        regfree(&comp);
        throw;
    }

    regfree(&comp);
}

/* UserInfo.cc */
UserInfo UserInfo::LocalUser() {
    UserInfo retval;
    uid_t uid, euid;
    int bufsize;
    struct passwd pwd, epwd, *result = NULL;
    euid = geteuid();
    uid = getuid();

    if ((bufsize = sysconf(_SC_GETPW_R_SIZE_MAX)) == -1) {
        THROW(InvalidParameter,
              "Invalid input: \"sysconf\" function failed to get the "
              "configure with key \"_SC_GETPW_R_SIZE_MAX\".");
    }

    std::vector<char> buffer(bufsize);

    if (getpwuid_r(euid, &epwd, &buffer[0], bufsize, &result) != 0 || !result) {
        THROW(InvalidParameter,
              "Invalid input: effective user name cannot be found with UID %u.",
              euid);
    }

    retval.setEffectiveUser(epwd.pw_name);

    if (getpwuid_r(uid, &pwd, &buffer[0], bufsize, &result) != 0 || !result) {
        THROW(InvalidParameter,
              "Invalid input: real user name cannot be found with UID %u.",
              uid);
    }

    retval.setRealUser(pwd.pw_name);
    return retval;
}

}
}
