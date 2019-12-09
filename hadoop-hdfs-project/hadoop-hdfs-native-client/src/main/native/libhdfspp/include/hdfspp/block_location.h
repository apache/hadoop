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

#ifndef HDFSPP_BLOCK_LOCATION_H
#define HDFSPP_BLOCK_LOCATION_H

namespace hdfs {

class DNInfo {
public:
  DNInfo() : xfer_port_(-1), info_port_(-1), IPC_port_(-1), info_secure_port_(-1) {}

  std::string getHostname() const {
    return hostname_;
  }

  void setHostname(const std::string & hostname) {
    this->hostname_ = hostname;
  }

  std::string getIPAddr() const {
    return ip_addr_;
  }

  void setIPAddr(const std::string & ip_addr) {
    this->ip_addr_ = ip_addr;
  }

  std::string getNetworkLocation() const {
    return network_location_;
  }

  void setNetworkLocation(const std::string & location) {
    this->network_location_ = location;
  }

  int getXferPort() const {
    return xfer_port_;
  }

  void setXferPort(int xfer_port) {
    this->xfer_port_ = xfer_port;
  }

  int getInfoPort() const {
    return info_port_;
  }

  void setInfoPort(int info_port) {
    this->info_port_ = info_port;
  }

  int getIPCPort() const {
    return IPC_port_;
  }

  void setIPCPort(int IPC_port) {
    this->IPC_port_ = IPC_port;
  }

  int getInfoSecurePort() const {
    return info_secure_port_;
  }

  void setInfoSecurePort(int info_secure_port) {
    this->info_secure_port_ = info_secure_port;
  }
private:
  std::string hostname_;
  std::string ip_addr_;
  std::string network_location_;
  int         xfer_port_;
  int         info_port_;
  int         IPC_port_;
  int         info_secure_port_;
};

class BlockLocation {
public:
    bool isCorrupt() const {
        return corrupt_;
    }

    void setCorrupt(bool corrupt) {
        this->corrupt_ = corrupt;
    }

    int64_t getLength() const {
        return length_;
    }

    void setLength(int64_t length) {
        this->length_ = length;
    }

    int64_t getOffset() const {
        return offset_;
    }

    void setOffset(int64_t offset) {
        this->offset_ = offset;
    }

    const std::vector<DNInfo> & getDataNodes() const {
        return dn_info_;
    }

    void setDataNodes(const std::vector<DNInfo> & dn_info) {
        this->dn_info_ = dn_info;
    }

private:
    bool corrupt_;
    int64_t length_;
    int64_t offset_;  // Offset of the block in the file
    std::vector<DNInfo> dn_info_; // Info about who stores each block
};

class FileBlockLocation {
public:
  uint64_t getFileLength() {
    return fileLength_;
  }

  void setFileLength(uint64_t fileLength) {
    this->fileLength_ = fileLength;
  }

  bool isLastBlockComplete() const {
    return this->lastBlockComplete_;
  }

  void setLastBlockComplete(bool lastBlockComplete) {
    this->lastBlockComplete_ = lastBlockComplete;
  }

  bool isUnderConstruction() const {
    return underConstruction_;
  }

  void setUnderConstruction(bool underConstruction) {
    this->underConstruction_ = underConstruction;
  }

  const std::vector<BlockLocation> & getBlockLocations() const {
    return blockLocations_;
  }

  void setBlockLocations(const std::vector<BlockLocation> & blockLocations) {
    this->blockLocations_ = blockLocations;
  }
private:
  uint64_t fileLength_;
  bool     lastBlockComplete_;
  bool     underConstruction_;
  std::vector<BlockLocation> blockLocations_;
};

} // namespace hdfs


#endif /* HDFSPP_BLOCK_LOCATION_H */
