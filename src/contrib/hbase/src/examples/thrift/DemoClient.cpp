#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>

#include <iostream>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "Hbase.h"

using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;

using namespace apache::hadoop::hbase::thrift;

typedef std::vector<std::string> StrVec;
typedef std::map<std::string,std::string> StrMap;
typedef std::vector<ColumnDescriptor> ColVec;
typedef std::map<std::string,ColumnDescriptor> ColMap;


static void
printRow(const std::string &row, const StrMap &columns)
{
  std::cout << "row: " << row << ", cols: ";
  for (StrMap::const_iterator it = columns.begin(); it != columns.end(); ++it) {
    std::cout << it->first << " => " << it->second << "; ";
  }
  std::cout << std::endl;
}

static void 
printEntry(const ScanEntry &entry)
{
  printRow(entry.row, entry.columns);
}

static void
printVersions(const std::string &row, const StrVec &versions)
{
  std::cout << "row: " << row << ", values: ";
  for (StrVec::const_iterator it = versions.begin(); it != versions.end(); ++it) {
    std::cout << *it << "; ";
  }
  std::cout << std::endl;
}

int 
main(int argc, char** argv) 
{
  boost::shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  HbaseClient client(protocol);

  try {
    transport->open();

    std::string t("demo_table");

    //
    // Scan all tables, look for the demo table and delete it.
    //
    std::cout << "scanning tables..." << std::endl;
    StrVec tables;
    client.getTableNames(tables);
    for (StrVec::const_iterator it = tables.begin(); it != tables.end(); ++it) {
      std::cout << "  found: " << *it << std::endl;
      if (t == *it) {
        std::cout << "    deleting table: " << *it << std::endl;
        client.deleteTable(*it);
      }
    }

    //
    // Create the demo table with two column families, entry: and unused:
    //
    ColVec columns;
    columns.push_back(ColumnDescriptor());
    columns.back().name = "entry:";
    columns.back().maxVersions = 10;
    columns.push_back(ColumnDescriptor());
    columns.back().name = "unused:";

    std::cout << "creating table: " << t << std::endl;
    try {
      client.createTable(t, columns);
    } catch (AlreadyExists &ae) {
      std::cout << "WARN: " << ae.message << std::endl;
    }

    ColMap columnMap;
    client.getColumnDescriptors(columnMap, t);
    std::cout << "column families in " << t << ": " << std::endl;
    for (ColMap::const_iterator it = columnMap.begin(); it != columnMap.end(); ++it) {
      std::cout << "  column: " << it->second.name << ", maxVer: " << it->second.maxVersions << std::endl;
    }

    //
    // Test UTF-8 handling
    //
    std::string invalid("foo-\xfc\xa1\xa1\xa1\xa1\xa1");
    std::string valid("foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB");

    // non-utf8 is fine for data
    client.put(t, "foo", "entry:foo", invalid);

    // try empty strings
    client.put(t, "", "entry:", "");

    // this row name is valid utf8
    client.put(t, valid, "entry:foo", valid);

    // non-utf8 is not allowed in row names
    try {
      client.put(t, invalid, "entry:foo", invalid);
      std::cout << "FATAL: shouldn't get here!" << std::endl;
      exit(-1);
    } catch (IOError e) {
      std::cout << "expected error: " << e.message << std::endl;
    }

    // Run a scanner on the rows we just created
    StrVec columnNames;
    columnNames.push_back("entry:");

    std::cout << "Starting scanner..." << std::endl;
    int scanner = client.scannerOpen(t, "", columnNames);
    try {
      while (true) {
        ScanEntry value;
        client.scannerGet(value, scanner);
        printEntry(value);
      }
    } catch (NotFound &nf) {
      client.scannerClose(scanner);
      std::cout << "Scanner finished" << std::endl;
    }

    //
    // Run some operations on a bunch of rows.
    //
    for (int i = 100; i >= 0; --i) {
      // format row keys as "00000" to "00100"
      char buf[32];
      sprintf(buf, "%0.5d", i);
      std::string row(buf);
      
      StrMap values;

      client.put(t, row, "unused:", "DELETE_ME");
      client.getRow(values, t, row);
      printRow(row, values);
      client.deleteAllRow(t, row);

      client.put(t, row, "entry:num", "0");
      client.put(t, row, "entry:foo", "FOO");
      client.getRow(values, t, row);
      printRow(row, values);

      // sleep to force later timestamp 
      poll(0, 0, 50);

      std::vector<Mutation> mutations;
      mutations.push_back(Mutation());
      mutations.back().column = "entry:foo";
      mutations.back().isDelete = true;
      mutations.push_back(Mutation());
      mutations.back().column = "entry:num";
      mutations.back().value = "-1";
      client.mutateRow(t, row, mutations);
      client.getRow(values, t, row);
      printRow(row, values);
      
      client.put(t, row, "entry:num", boost::lexical_cast<std::string>(i));
      client.put(t, row, "entry:sqr", boost::lexical_cast<std::string>(i*i));
      client.getRow(values, t, row);
      printRow(row, values);

      mutations.clear();
      mutations.push_back(Mutation());
      mutations.back().column = "entry:num";
      mutations.back().value = "-999";
      mutations.push_back(Mutation());
      mutations.back().column = "entry:sqr";
      mutations.back().isDelete = true;
      client.mutateRowTs(t, row, mutations, 1); // shouldn't override latest
      client.getRow(values, t, row);
      printRow(row, values);

      StrVec versions;
      client.getVer(versions, t, row, "entry:num", 10);
      printVersions(row, versions);
      assert(versions.size() == 4);
      std::cout << std::endl;

      try {
        std::string value;
        client.get(value, t, row, "entry:foo");
        std::cout << "FATAL: shouldn't get here!" << std::endl;
        exit(-1);
      } catch (NotFound &nf) {
        // blank
      }
    }

    // scan all rows/columns

    columnNames.clear();
    client.getColumnDescriptors(columnMap, t);
    for (ColMap::const_iterator it = columnMap.begin(); it != columnMap.end(); ++it) {
      columnNames.push_back(it->first);
    }

    std::cout << "Starting scanner..." << std::endl;
    scanner = client.scannerOpenWithStop(t, "00020", "00040", columnNames);
    try {
      while (true) {
        ScanEntry value;
        client.scannerGet(value, scanner);
        printEntry(value);
      }
    } catch (NotFound &nf) {
      client.scannerClose(scanner);
      std::cout << "Scanner finished" << std::endl;
    }

    transport->close();
  } 
  catch (TException &tx) {
    printf("ERROR: %s\n", tx.what());
  }

}
