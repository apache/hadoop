#ifndef TYPEINFO_HH_
#define TYPEINFO_HH_

#include "recordio.hh"
#include "typeIDs.hh"

namespace hadoop {

class TypeID;

class TypeInfo {
  
private: 
  // we own memory mgmt of these vars
  const std::string* pFieldID;
  const TypeID* pTypeID;

public: 
  TypeInfo(const std::string* pFieldID, const TypeID* pTypeID) : 
    pFieldID(pFieldID), pTypeID(pTypeID) {}
  TypeInfo(const TypeInfo& ti);
  virtual ~TypeInfo();

  const TypeID* getTypeID() const {return pTypeID;}
  const std::string* getFieldID() const {return pFieldID;}
  void serialize(::hadoop::OArchive& a_, const char* tag) const;
  bool operator==(const TypeInfo& peer_) const;
  TypeInfo* clone() const {return new TypeInfo(*this);}

  //TypeInfo& operator =(const TypeInfo& ti);
  void print(int space=0) const;

};

}

#endif // TYPEINFO_HH_

#ifndef TYPEINFO_HH_
#define TYPEINFO_HH_

#include "recordio.hh"
#include "typeIDs.hh"

namespace hadoop {

class TypeID;

class TypeInfo {
  
private: 
  // we own memory mgmt of these vars
  const std::string* pFieldID;
  const TypeID* pTypeID;

public: 
  TypeInfo(const std::string* pFieldID, const TypeID* pTypeID) : 
    pFieldID(pFieldID), pTypeID(pTypeID) {}
  TypeInfo(const TypeInfo& ti);
  virtual ~TypeInfo();

  const TypeID* getTypeID() const {return pTypeID;}
  const std::string* getFieldID() const {return pFieldID;}
  void serialize(::hadoop::OArchive& a_, const char* tag) const;
  bool operator==(const TypeInfo& peer_) const;
  TypeInfo* clone() const {return new TypeInfo(*this);}

  //TypeInfo& operator =(const TypeInfo& ti);
  void print(int space=0) const;

};

}

#endif // TYPEINFO_HH_

