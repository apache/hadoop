/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#ifndef LIBHDFSPP_TOOLS_HDFS_TOOL_TESTS
#define LIBHDFSPP_TOOLS_HDFS_TOOL_TESTS

#include <memory>
#include <string>

/**
 * This file contains the generalized test cases to run against  the derivatives
 * of {@link hdfs::tools::HdfsTool}.
 *
 * Each test case passes the arguments to the {@link hdfs::tools::HdfsTool} and
 * calls the method to set the expectation on the instance of {@link
 * hdfs::tools::HdfsTool} as defined in its corresponding mock implementation.
 */

template <class T> std::unique_ptr<T> PassAPath() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("a/b/c");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassAPath<T>, {arg1});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassRecursive() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-R");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassRecursive<T>, {arg1});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassRecursivePath() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-R");
  static std::string arg2("a/b/c");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassRecursivePath<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassFOptAndAPath() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-f");
  static std::string arg2("a/b/c");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassFOptAndAPath<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> CallHelp() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-h");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(CallHelp<T>);
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> Pass2Paths() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("a/b/c");
  static std::string arg2("d/e/f");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(Pass2Paths<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> Pass3Paths() {
  constexpr auto argc = 4;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("a/b/c");
  static std::string arg2("d/e/f");
  static std::string arg3("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data(), arg3.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(Pass3Paths<T>, {arg1, arg2, arg3});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassNOptAndAPath() {
  constexpr auto argc = 4;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-n");
  static std::string arg2("some_name");
  static std::string arg3("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data(), arg3.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassNOptAndAPath<T>, {arg1, arg2, arg3});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassOwnerAndAPath() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("new_owner:new_group");
  static std::string arg2("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassOwnerAndAPath<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassRecursiveOwnerAndAPath() {
  constexpr auto argc = 4;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-R");
  static std::string arg2("new_owner:new_group");
  static std::string arg3("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data(), arg3.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassRecursiveOwnerAndAPath<T>, {arg1, arg2, arg3});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassPermissionsAndAPath() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("757");
  static std::string arg2("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassPermissionsAndAPath<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassInvalidPermissionsAndAPath() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("123456789123456789123456789");
  static std::string arg2("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassInvalidPermissionsAndAPath<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassRecursivePermissionsAndAPath() {
  constexpr auto argc = 4;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-R");
  static std::string arg2("757");
  static std::string arg3("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data(), arg3.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassRecursivePermissionsAndAPath<T>,
                             {arg1, arg2, arg3});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassQOpt() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-q");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassQOpt<T>, {arg1});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassQOptAndPath() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-q");
  static std::string arg2("a/b/c");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassQOptAndPath<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassPOpt() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-p");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassPOpt<T>, {arg1});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassMOpt() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-m");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassMOpt<T>, {arg1});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassFOpt() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-f");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassFOpt<T>, {arg1});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassPOptAndPath() {
  constexpr auto argc = 3;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-p");
  static std::string arg2("a/b/c");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassPOptAndPath<T>, {arg1, arg2});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassMOptPermissionsAndAPath() {
  constexpr auto argc = 4;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-m");
  static std::string arg2("757");
  static std::string arg3("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data(), arg3.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassMOptPermissionsAndAPath<T>,
                             {arg1, arg2, arg3});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassMPOptsPermissionsAndAPath() {
  constexpr auto argc = 5;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-m");
  static std::string arg2("757");
  static std::string arg3("-p");
  static std::string arg4("g/h/i");

  static char *argv[] = {exe.data(), arg1.data(), arg2.data(), arg3.data(),
                         arg4.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassMPOptsPermissionsAndAPath<T>,
                             {arg1, arg2, arg3, arg4});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassNStrMNumAndAPath() {
  constexpr auto argc = 6;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-n");
  static std::string arg2("some_str");
  static std::string arg3("-m");
  static std::string arg4("757");
  static std::string arg5("some/path");

  static char *argv[] = {exe.data(),  arg1.data(), arg2.data(),
                         arg3.data(), arg4.data(), arg5.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassNStrMNumAndAPath<T>,
                             {arg1, arg2, arg3, arg4, arg5});
  return hdfs_tool;
}

template <class T> std::unique_ptr<T> PassNOpt() {
  constexpr auto argc = 2;
  static std::string exe("hdfs_tool_name");
  static std::string arg1("-n");

  static char *argv[] = {exe.data(), arg1.data()};

  auto hdfs_tool = std::make_unique<T>(argc, argv);
  hdfs_tool->SetExpectations(PassNOpt<T>, {arg1});
  return hdfs_tool;
}

#endif
