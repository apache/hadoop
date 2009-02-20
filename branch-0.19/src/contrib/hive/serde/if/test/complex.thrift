namespace java org.apache.hadoop.hive.serde2.thrift.test

struct IntString {
  1: i32  myint;
  2: string myString;
}

struct Complex {
  1: i32 aint;
  2: string aString;
  3: list<i32> lint;
  4: list<string> lString;
  5: list<IntString> lintString;
  6: map<string, string> mStringString;
}
