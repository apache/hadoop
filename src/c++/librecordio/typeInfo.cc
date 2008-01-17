
#include "typeInfo.hh"

using namespace hadoop;

TypeInfo::~TypeInfo()
{
  delete pFieldID;
  delete pTypeID;
}

/*TypeInfo& TypeInfo::operator =(const TypeInfo& ti) {
  pFieldID = ti.pFieldID;
  pTypeID = ti.pTypeID;
  return *this;
  }*/

TypeInfo::TypeInfo(const TypeInfo& ti)
{
  pFieldID = new std::string(*ti.pFieldID);
  pTypeID = ti.pTypeID->clone();
}


void TypeInfo::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize(*pFieldID, tag);
  pTypeID->serialize(a_, tag);
}

bool TypeInfo::operator==(const TypeInfo& peer_) const 
{
  // first check if fieldID matches
  if (0 != pFieldID->compare(*(peer_.pFieldID))) {
    return false;
  }
  // now see if typeID matches
  return (*pTypeID == *(peer_.pTypeID));
}

void TypeInfo::print(int space) const
{
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("TypeInfo(%lx):\n", (long)this);
  for (int i=0; i<space+2; i++) {
    printf(" ");
  }
  printf("field = \"%s\"\n", pFieldID->c_str());
  pTypeID->print(space+2);
}

#include "typeInfo.hh"

using namespace hadoop;

TypeInfo::~TypeInfo()
{
  delete pFieldID;
  delete pTypeID;
}

/*TypeInfo& TypeInfo::operator =(const TypeInfo& ti) {
  pFieldID = ti.pFieldID;
  pTypeID = ti.pTypeID;
  return *this;
  }*/

TypeInfo::TypeInfo(const TypeInfo& ti)
{
  pFieldID = new std::string(*ti.pFieldID);
  pTypeID = ti.pTypeID->clone();
}


void TypeInfo::serialize(::hadoop::OArchive& a_, const char* tag) const
{
  a_.serialize(*pFieldID, tag);
  pTypeID->serialize(a_, tag);
}

bool TypeInfo::operator==(const TypeInfo& peer_) const 
{
  // first check if fieldID matches
  if (0 != pFieldID->compare(*(peer_.pFieldID))) {
    return false;
  }
  // now see if typeID matches
  return (*pTypeID == *(peer_.pTypeID));
}

void TypeInfo::print(int space) const
{
  for (int i=0; i<space; i++) {
    printf(" ");
  }
  printf("TypeInfo(%lx):\n", (long)this);
  for (int i=0; i<space+2; i++) {
    printf(" ");
  }
  printf("field = \"%s\"\n", pFieldID->c_str());
  pTypeID->print(space+2);
}
