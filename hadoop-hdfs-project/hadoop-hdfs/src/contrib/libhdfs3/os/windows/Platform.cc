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

#include <regex>
#include <vector>

int poll(struct pollfd *fds, unsigned long nfds, int timeout) {
    return WSAPoll(fds, nfds, timeout);
}

namespace hdfs {
namespace internal {

/* InputStreamImpl.cc */
unordered_set<std::string> BuildLocalAddrSet() {
#define MALLOC(x) HeapAlloc(GetProcessHeap(), 0, (x))
#define FREE(x) HeapFree(GetProcessHeap(), 0, (x))
    DWORD dwSize = 0;
    DWORD dwRetVal = 0;
    unsigned int i = 0;

    // Set the flags to pass to GetAdaptersAddresses
    ULONG flags = GAA_FLAG_INCLUDE_PREFIX;

    // default to unspecified address family (both)
    ULONG family = AF_UNSPEC;
    PIP_ADAPTER_ADDRESSES pAddresses = NULL;
    ULONG outBufLen = 0;

    PIP_ADAPTER_ADDRESSES pCurrAddresses = NULL;
    PIP_ADAPTER_UNICAST_ADDRESS pUnicast = NULL;
    PIP_ADAPTER_ANYCAST_ADDRESS pAnycast = NULL;
    PIP_ADAPTER_MULTICAST_ADDRESS pMulticast = NULL;

    outBufLen = sizeof (IP_ADAPTER_ADDRESSES);
    pAddresses = (IP_ADAPTER_ADDRESSES *) MALLOC(outBufLen);

	// Make an initial call to GetAdaptersAddresses to get the
	// size needed into the outBufLen variable
	if (GetAdaptersAddresses(
        family,
        flags,
        NULL,
        pAddresses,
        &outBufLen) == ERROR_BUFFER_OVERFLOW) {
        FREE(pAddresses);
        pAddresses = (IP_ADAPTER_ADDRESSES *) MALLOC(outBufLen);
	}

    if (pAddresses == NULL) {
        THROW(HdfsNetworkException,
            "InputStreamImpl: malloc failed, "
            "cannot get local network interface: %s",
            GetSystemErrorInfo(errno));
    }

    // Make a second call to GetAdapters Addresses to get the
    // actual data we want
    dwRetVal =
        GetAdaptersAddresses(family, flags, NULL, pAddresses, &outBufLen);

    if (dwRetVal == NO_ERROR) {
        // If successful, construct the address set
        unordered_set<std::string> set; // to be returned
        std::vector<char> host;
        const char *pHost;
        host.resize(INET6_ADDRSTRLEN + 1);

        pCurrAddresses = pAddresses;
        while (pCurrAddresses) {
            // TODO: scan Anycast, Multicast as well.
            // scan unicast address list
            pUnicast = pCurrAddresses->FirstUnicastAddress;
            while (pUnicast != NULL) {
                memset(&host[0], 0, INET6_ADDRSTRLEN);
                ULONG _family = pUnicast->Address.lpSockaddr->sa_family;
                if (_family == AF_INET) {
                    SOCKADDR_IN *sa_in =
                        (SOCKADDR_IN *)pUnicast->Address.lpSockaddr;
                    pHost = InetNtop(
                        AF_INET,
                        &(sa_in->sin_addr),
                        &host[0],
                        INET6_ADDRSTRLEN);
                }
                else {
                    SOCKADDR_IN6 *sa_in6 =
                        (SOCKADDR_IN6 *)pUnicast->Address.lpSockaddr;
                    pHost = InetNtop(
                        AF_INET,
                        &(sa_in6->sin6_addr),
                        &host[0],
                        INET6_ADDRSTRLEN);
                }
                if (pHost == NULL) {
                    THROW(HdfsNetworkException,
                        "InputStreamImpl: cannot get convert network address to textual form: %s",
                        GetSystemErrorInfo(errno));
                }
                set.insert(pHost);
                pUnicast = pUnicast->Next;
            } // inner while
            pCurrAddresses = pCurrAddresses->Next;
        } // while

        // TODO: replace hardcoded HOST_NAME_MAX
        int _HOST_NAME_MAX = 128;
        host.resize(_HOST_NAME_MAX + 1);
        if (gethostname(&host[0], host.size())) {
            THROW(HdfsNetworkException,
                "InputStreamImpl: cannot get hostname: %s",
                GetSystemErrorInfo(errno));
        }
        set.insert(&host[0]);
        if (pAddresses != NULL) {
            FREE(pAddresses);
        }
        return set;
    }
    else {
        printf("Call to GetAdaptersAddresses failed with error: %d\n",
            dwRetVal);
        if (pAddresses != NULL) {
            FREE(pAddresses);
        }
        THROW(HdfsNetworkException,
            "InputStreamImpl: cannot get local network interface: %s",
            GetSystemErrorInfo(errno));
    }
}

/* TpcSocket.cc */
void TcpSocketImpl::setBlockMode(bool enable) {
    u_long blocking_mode = (enable) ? 0 : 1;
    int rc = syscalls::ioctlsocket(sock, FIONBIO, &blocking_mode);
    if (rc == SOCKET_ERROR) {
        THROW(HdfsNetworkException, "Get socket flag failed for remote node %s: %s",
            remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }
}

/* NamenodeProxy.cc */
static std::string GetTmpPath() {
    char lpTempPathBuffer[MAX_PATH];
    //  Gets the temp path env string (no guarantee it's a valid path).
    DWORD dwRetVal = GetTempPath(
        MAX_PATH, // length of the buffer
        lpTempPathBuffer); // buffer for path
    if (dwRetVal > MAX_PATH || (dwRetVal == 0))
        THROW(HdfsException, "GetTmpPath failed");
    return std::string(lpTempPathBuffer);
}

static uint32_t GetInitNamenodeIndex(const std::string id) {
    std::string path = GetTmpPath();
    path += id;
    HANDLE fd = INVALID_HANDLE_VALUE;
    uint32_t index = 0;

    fd = CreateFile(
        path.c_str(),
        GENERIC_WRITE, // write only
        0, // do not share, is this right shared mode?
        NULL, // default security
        CREATE_NEW, // call fails if file exists, ERROR_FILE_EXISTS
        FILE_ATTRIBUTE_NORMAL, // normal file
        NULL); // no template

    if (fd == INVALID_HANDLE_VALUE) {
        // File already exists, try to open it
        if (GetLastError() == ERROR_FILE_EXISTS) {
            fd = CreateFile(path.c_str(),
                GENERIC_READ, // open for reading
                0, // do not share
                NULL, // default security
                OPEN_EXISTING, // existing file only
                FILE_ATTRIBUTE_NORMAL, // normal file
                NULL // // no template
                );
        }
        else {
            // TODO: log, or throw exception when a file is failed to open
            return 0;
        }
    }
    else {
        DWORD dwBytesToWrite = (DWORD)sizeof(index);
        DWORD dwBytesWritten = 0;
        BOOL bErrorFlag = WriteFile(
            fd,
            &index,
            dwBytesToWrite,
            &dwBytesToWrite,
            NULL);
        // TODO: check error code and number of bytes written
        return index;
    }

    // the file exists, read it
    DWORD dwBytesToRead = 0;
    if (FALSE == ReadFile(fd, &index, sizeof(index), &dwBytesToRead, NULL)) {
        index = 0; // fail to read, don't care
    }
    return index;
}

static void SetInitNamenodeIndex(const std::string & id, uint32_t index) {
    std::string path = GetTmpPath();
    path += id;
    HANDLE fd = INVALID_HANDLE_VALUE;
    fd = CreateFile(
        path.c_str(),
        GENERIC_WRITE, // write only
        0, // do not share, is this right shared mode?
        NULL, // default security
        OPEN_ALWAYS, // call fails if file exists, ERROR_FILE_EXISTS
        FILE_ATTRIBUTE_NORMAL, // normal file
        NULL);                 // no template
    if (fd != INVALID_HANDLE_VALUE) {
        DWORD dwBytesToWrite = (DWORD)sizeof(index);
        DWORD dwBytesWritten = 0;
        BOOL bErrorFlag = WriteFile(
            fd,
            &index,
            dwBytesToWrite,
            &dwBytesToWrite,
            NULL);
    }
}

/* KerberosName.cc */
void KerberosName::parse(const std::string & principal) {
    int rc;
    // primary/instance@REALM
    // [^/@]* = anything but / and @
    static const char * pattern = "([^/@]*)(/([^/@]*))?@([^/@]*)";
    std::tr1::cmatch res;
    std::tr1::regex rx(pattern);
    if (!std::tr1::regex_search(principal.c_str(), res, rx)) {
        // Check if principal is just simply a username without the @thing
        if (principal.find('@') != principal.npos) {
            THROW(HdfsIOException,
                "KerberosName: Malformed Kerberos name: %s",
                principal.c_str());
        }
        else {
            name = principal;
            return;
        }
    }
    if (res[1].length() > 0) {
        name = res[1];
    }
    if (res[3].length() > 0) {
        host = res[3];
    }
    if (res[4].length() > 0) {
        realm = res[4];
    }
}

/* UserInfo.cc */
UserInfo UserInfo::LocalUser() {
    UserInfo retval;
    char username[UNLEN + 1];
    DWORD username_len = UNLEN + 1;
    GetUserName(username, &username_len);
    std::string str(username);

    // Assume for now in Windows real and effective users are the same.
    retval.setRealUser(&str[0]);
    retval.setEffectiveUser(&str[0]);
    return retval;
}

}
}
