// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_PROTOCOL_TDENSEPROTOCOL_H_
#define _THRIFT_PROTOCOL_TDENSEPROTOCOL_H_ 1

#include "TBinaryProtocol.h"

namespace facebook { namespace thrift { namespace protocol {

/**
 * !!!WARNING!!!
 * This class is still highly experimental.  Incompatible changes
 * WILL be made to it without notice.  DO NOT USE IT YET unless
 * you are coordinating your testing with the author.
 *
 * The dense protocol is designed to use as little space as possible.
 *
 * There are two types of dense protocol instances.  Standalone instances
 * are not used for RPC and just encoded and decode structures of 
 * a predetermined type.  Non-standalone instances are used for RPC.
 * Currently, only standalone instances exist.
 *
 * To use a standalone dense protocol object, you must set the type_spec
 * property (either in the constructor, or with setTypeSpec) to the local
 * reflection TypeSpec of the structures you will write to (or read from) the
 * protocol instance.
 *
 * BEST PRACTICES:
 * - Never use optional for primitives or containers.
 * - Only use optional for structures if they are very big and very rarely set.
 * - All integers are variable-length, so you can use i64 without bloating.
 * - NEVER EVER change the struct definitions IN ANY WAY without either
 *   changing your cache keys or talking to dreiss.
 *
 * TODO(dreiss): New class write with old meta.
 *
 * We override all of TBinaryProtocol's methods.
 * We inherit so that we can can explicitly call TBPs's primitive-writing
 * methods within our versions.
 *
 * @author David Reiss <dreiss@facebook.com>
 */
class TDenseProtocol : public TBinaryProtocol {
 protected:
  static const int32_t VERSION_MASK = 0xffff0000;
  // VERSION_1 (0x80010000)  is taken by TBinaryProtocol.
  static const int32_t VERSION_2 = 0x80020000;

 public:
  typedef facebook::thrift::reflection::local::TypeSpec TypeSpec;
  static const int FP_PREFIX_LEN;

  /**
   * @param tran       The transport to use.
   * @param type_spec  The TypeSpec of the structures using this protocol.
   */
  TDenseProtocol(boost::shared_ptr<TTransport> trans,
                 TypeSpec* type_spec = NULL) :
    TBinaryProtocol(trans),
    type_spec_(type_spec),
    standalone_(true)
  {}

  void setTypeSpec(TypeSpec* type_spec) {
    type_spec_ = type_spec;
  }
  TypeSpec* getTypeSpec() {
    return type_spec_;
  }


  /*
   * Writing functions.
   */

  virtual uint32_t writeMessageBegin(const std::string& name,
                                     const TMessageType messageType,
                                     const int32_t seqid);

  virtual uint32_t writeMessageEnd();


  virtual uint32_t writeStructBegin(const std::string& name);

  virtual uint32_t writeStructEnd();

  virtual uint32_t writeFieldBegin(const std::string& name,
                                   const TType fieldType,
                                   const int16_t fieldId);

  virtual uint32_t writeFieldEnd();

  virtual uint32_t writeFieldStop();

  virtual uint32_t writeMapBegin(const TType keyType,
                                 const TType valType,
                                 const uint32_t size);

  virtual uint32_t writeMapEnd();

  virtual uint32_t writeListBegin(const TType elemType,
                                  const uint32_t size);

  virtual uint32_t writeListEnd();

  virtual uint32_t writeSetBegin(const TType elemType,
                                 const uint32_t size);

  virtual uint32_t writeSetEnd();

  virtual uint32_t writeBool(const bool value);

  virtual uint32_t writeByte(const int8_t byte);

  virtual uint32_t writeI16(const int16_t i16);

  virtual uint32_t writeI32(const int32_t i32);

  virtual uint32_t writeI64(const int64_t i64);

  virtual uint32_t writeDouble(const double dub);

  virtual uint32_t writeString(const std::string& str);


  /*
   * Helper writing functions (don't do state transitions).
   */
  inline uint32_t subWriteI32(const int32_t i32);

  inline uint32_t subWriteString(const std::string& str);

  uint32_t subWriteBool(const bool value) {
    return TBinaryProtocol::writeBool(value);
  }


  /*
   * Reading functions
   */

  uint32_t readMessageBegin(std::string& name,
                            TMessageType& messageType,
                            int32_t& seqid);

  uint32_t readMessageEnd();

  uint32_t readStructBegin(std::string& name);

  uint32_t readStructEnd();

  uint32_t readFieldBegin(std::string& name,
                          TType& fieldType,
                          int16_t& fieldId);

  uint32_t readFieldEnd();

  uint32_t readMapBegin(TType& keyType,
                        TType& valType,
                        uint32_t& size);

  uint32_t readMapEnd();

  uint32_t readListBegin(TType& elemType,
                         uint32_t& size);

  uint32_t readListEnd();

  uint32_t readSetBegin(TType& elemType,
                        uint32_t& size);

  uint32_t readSetEnd();

  uint32_t readBool(bool& value);

  uint32_t readByte(int8_t& byte);

  uint32_t readI16(int16_t& i16);

  uint32_t readI32(int32_t& i32);

  uint32_t readI64(int64_t& i64);

  uint32_t readDouble(double& dub);

  uint32_t readString(std::string& str);


  /*
   * Helper reading functions (don't do state transitions).
   */
  inline uint32_t subReadI32(int32_t& i32);

  inline uint32_t subReadString(std::string& str);

  uint32_t subReadBool(bool& value) {
    return TBinaryProtocol::readBool(value);
  }


 private:

  // Implementation functions, documented in the .cpp.
  inline void checkTType(const TType ttype);
  inline void stateTransition();

  // Read and write variable-length integers.
  // Uses the same technique as the MIDI file format.
  inline uint32_t vlqRead(uint64_t& vlq);
  inline uint32_t vlqWrite(uint64_t vlq);

  // Called before throwing an exception to make the object reusable.
  void resetState() {
    ts_stack_.clear();
    idx_stack_.clear();
    mkv_stack_.clear();
  }

  // TypeSpec of the top-level structure to write,
  // for standalone protocol objects.
  TypeSpec* type_spec_;

  std::vector<TypeSpec*> ts_stack_;   // TypeSpec stack.
  std::vector<int>       idx_stack_;  // InDeX stack.
  std::vector<bool>      mkv_stack_;  // Map Key/Vlue stack.
                                      // True = key, False = value.

  // True iff this is a standalone instance (no RPC).
  bool standalone_;
};

}}} // facebook::thrift::protocol

#endif // #ifndef _THRIFT_PROTOCOL_TDENSEPROTOCOL_H_
