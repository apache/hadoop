// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef STATSPROCESSOR_H
#define STATSPROCESSOR_H

#include <boost/shared_ptr.hpp>
#include <transport/TTransport.h>
#include <protocol/TProtocol.h>
#include <TProcessor.h>

namespace facebook { namespace thrift { namespace processor { 

/*
 * Class for keeping track of function call statistics and printing them if desired
 *
 * @author James Wang <jwang@facebook.com>
 */
class StatsProcessor : public facebook::thrift::TProcessor {
public:
  StatsProcessor(bool print, bool frequency) 
    : print_(print),
      frequency_(frequency)
  {}
  virtual ~StatsProcessor() {};

  virtual bool process(boost::shared_ptr<facebook::thrift::protocol::TProtocol> piprot, boost::shared_ptr<facebook::thrift::protocol::TProtocol> poprot) {

    piprot_ = piprot;

    std::string fname;
    facebook::thrift::protocol::TMessageType mtype;
    int32_t seqid;

    piprot_->readMessageBegin(fname, mtype, seqid);
    if (mtype != facebook::thrift::protocol::T_CALL) {
      if (print_) {
        printf("Unknown message type\n");
      }
      throw facebook::thrift::TException("Unexpected message type");
    }
    if (print_) {
      printf("%s (", fname.c_str());
    }
    if (frequency_) {
      if (frequency_map_.find(fname) != frequency_map_.end()) {
        frequency_map_[fname]++;
      } else {
        frequency_map_[fname] = 1;
      }
    }

    facebook::thrift::protocol::TType ftype;
    int16_t fid;

    while (true) {
      piprot_->readFieldBegin(fname, ftype, fid);
      if (ftype == facebook::thrift::protocol::T_STOP) {
        break;
      }

      printAndPassToBuffer(ftype);
      if (print_) {
        printf(", ");
      }
    }

    if (print_) {
      printf("\b\b)\n");
    }
    return true;
  }

  const std::map<std::string, int64_t>& get_frequency_map() {
    return frequency_map_;
  }

protected:
  void printAndPassToBuffer(facebook::thrift::protocol::TType ftype) {
    switch (ftype) {
      case facebook::thrift::protocol::T_BOOL:
        {
          bool boolv;
          piprot_->readBool(boolv);
          if (print_) {
            printf("%d", boolv);
          }
        }
        break;
      case facebook::thrift::protocol::T_BYTE:
        {
          int8_t bytev;
          piprot_->readByte(bytev);
          if (print_) {
            printf("%d", bytev);
          }
        }
        break;
      case facebook::thrift::protocol::T_I16:
        {
          int16_t i16;
          piprot_->readI16(i16);
          if (print_) {
            printf("%d", i16);
          }
        }
        break;
      case facebook::thrift::protocol::T_I32:
        {
          int32_t i32;
          piprot_->readI32(i32);
          if (print_) {
            printf("%d", i32);
          }
        }
        break;
      case facebook::thrift::protocol::T_I64:
        {
          int64_t i64;
          piprot_->readI64(i64);
          if (print_) {
            printf("%ld", i64);
          }
        }
        break;
      case facebook::thrift::protocol::T_DOUBLE:
        {
          double dub; 
          piprot_->readDouble(dub);
          if (print_) {
            printf("%f", dub);
          }
        }
        break;
      case facebook::thrift::protocol::T_STRING: 
        {
          std::string str;
          piprot_->readString(str);
          if (print_) {
            printf("%s", str.c_str());
          }
        }
        break;
      case facebook::thrift::protocol::T_STRUCT: 
        {
          std::string name;
          int16_t fid;
          facebook::thrift::protocol::TType ftype;
          piprot_->readStructBegin(name);
          if (print_) {
            printf("<");
          }
          while (true) {
            piprot_->readFieldBegin(name, ftype, fid);
            if (ftype == facebook::thrift::protocol::T_STOP) {
              break;
            }
            printAndPassToBuffer(ftype);
            if (print_) {
              printf(",");
            }
            piprot_->readFieldEnd();
          }
          piprot_->readStructEnd();
          if (print_) {
            printf("\b>");
          }
        }
        break;
      case facebook::thrift::protocol::T_MAP:
        {
          facebook::thrift::protocol::TType keyType;
          facebook::thrift::protocol::TType valType;
          uint32_t i, size;
          piprot_->readMapBegin(keyType, valType, size);
          if (print_) {
            printf("{");
          }
          for (i = 0; i < size; i++) {
            printAndPassToBuffer(keyType);
            if (print_) {
              printf("=>");
            }
            printAndPassToBuffer(valType);
            if (print_) {
              printf(",");
            }
          }
          piprot_->readMapEnd();
          if (print_) {
            printf("\b}");
          }
        }
        break;
      case facebook::thrift::protocol::T_SET:
        {
          facebook::thrift::protocol::TType elemType;
          uint32_t i, size;
          piprot_->readSetBegin(elemType, size);
          if (print_) {
            printf("{");
          }
          for (i = 0; i < size; i++) {
            printAndPassToBuffer(elemType);
            if (print_) {
              printf(",");
            }
          }
          piprot_->readSetEnd();
          if (print_) {
            printf("\b}");
          }
        }
        break;
      case facebook::thrift::protocol::T_LIST:
        {
          facebook::thrift::protocol::TType elemType;
          uint32_t i, size;
          piprot_->readListBegin(elemType, size);
          if (print_) {
            printf("[");
          }
          for (i = 0; i < size; i++) {
            printAndPassToBuffer(elemType);
            if (print_) {
              printf(",");
            }
          }
          piprot_->readListEnd();
          if (print_) {
            printf("\b]");
          }
        }
        break;
      default:
        break;
    }
  }

  boost::shared_ptr<facebook::thrift::protocol::TProtocol> piprot_;
  std::map<std::string, int64_t> frequency_map_;

  bool print_;
  bool frequency_;
};

}}} // facebook::thrift::processor

#endif
