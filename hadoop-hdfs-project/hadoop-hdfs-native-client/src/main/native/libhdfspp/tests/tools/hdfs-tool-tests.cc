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

#include <gtest/gtest.h>

#include "hdfs-allow-snapshot-mock.h"
#include "hdfs-cat-mock.h"
#include "hdfs-chgrp-mock.h"
#include "hdfs-chmod-mock.h"
#include "hdfs-chown-mock.h"
#include "hdfs-copy-to-local-mock.h"
#include "hdfs-count-mock.h"
#include "hdfs-create-snapshot-mock.h"
#include "hdfs-delete-snapshot-mock.h"
#include "hdfs-df-mock.h"
#include "hdfs-disallow-snapshot-mock.h"
#include "hdfs-du-mock.h"
#include "hdfs-find-mock.h"
#include "hdfs-get-mock.h"
#include "hdfs-ls-mock.h"
#include "hdfs-mkdir-mock.h"
#include "hdfs-move-to-local-mock.h"
#include "hdfs-rename-snapshot-mock.h"
#include "hdfs-rm-mock.h"
#include "hdfs-setrep-mock.h"
#include "hdfs-stat-mock.h"
#include "hdfs-tail-mock.h"
#include "hdfs-tool-test-fixtures.h"
#include "hdfs-tool-tests.h"

/**
 * This file combines the test fixtures defined in {@file
 * hdfs-tool-test-fixtures.h} and the test cases defined in {@file
 * hdfs-tool-test.h} to yield the test suite.
 */

// Basic tests
INSTANTIATE_TEST_SUITE_P(
    HdfsAllowSnapshot, HdfsToolBasicTest,
    testing::Values(PassAPath<hdfs::tools::test::AllowSnapshotMock>,
                    CallHelp<hdfs::tools::test::AllowSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDisallowSnapshot, HdfsToolBasicTest,
    testing::Values(PassAPath<hdfs::tools::test::DisallowSnapshotMock>,
                    CallHelp<hdfs::tools::test::DisallowSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsRenameSnapshot, HdfsToolBasicTest,
    testing::Values(Pass3Paths<hdfs::tools::test::RenameSnapshotMock>,
                    CallHelp<hdfs::tools::test::RenameSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCreateSnapshot, HdfsToolBasicTest,
    testing::Values(PassNOptAndAPath<hdfs::tools::test::CreateSnapshotMock>,
                    CallHelp<hdfs::tools::test::CreateSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(HdfsCat, HdfsToolBasicTest,
                         testing::Values(PassAPath<hdfs::tools::test::CatMock>,
                                         CallHelp<hdfs::tools::test::CatMock>));

INSTANTIATE_TEST_SUITE_P(HdfsDf, HdfsToolBasicTest,
                         testing::Values(PassAPath<hdfs::tools::test::DfMock>,
                                         CallHelp<hdfs::tools::test::DfMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDu, HdfsToolBasicTest,
    testing::Values(PassAPath<hdfs::tools::test::DuMock>,
                    CallHelp<hdfs::tools::test::DuMock>,
                    PassRecursivePath<hdfs::tools::test::DuMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsLs, HdfsToolBasicTest,
    testing::Values(PassAPath<hdfs::tools::test::LsMock>,
                    CallHelp<hdfs::tools::test::LsMock>,
                    PassRecursivePath<hdfs::tools::test::LsMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDeleteSnapshot, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::DeleteSnapshotMock>,
                    Pass2Paths<hdfs::tools::test::DeleteSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChown, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::ChownMock>,
                    PassOwnerAndAPath<hdfs::tools::test::ChownMock>,
                    PassRecursiveOwnerAndAPath<hdfs::tools::test::ChownMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChmod, HdfsToolBasicTest,
    testing::Values(
        CallHelp<hdfs::tools::test::ChmodMock>,
        PassPermissionsAndAPath<hdfs::tools::test::ChmodMock>,
        PassRecursivePermissionsAndAPath<hdfs::tools::test::ChmodMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChgrp, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::ChgrpMock>,
                    PassOwnerAndAPath<hdfs::tools::test::ChgrpMock>,
                    PassRecursiveOwnerAndAPath<hdfs::tools::test::ChgrpMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCopyToLocal, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::CopyToLocalMock>,
                    Pass2Paths<hdfs::tools::test::CopyToLocalMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsGet, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::GetMock>,
                    Pass2Paths<hdfs::tools::test::GetMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsMoveToLocal, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::MoveToLocalMock>,
                    Pass2Paths<hdfs::tools::test::MoveToLocalMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCount, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::CountMock>,
                    PassAPath<hdfs::tools::test::CountMock>,
                    PassQOptAndPath<hdfs::tools::test::CountMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsMkdir, HdfsToolBasicTest,
    testing::Values(
        CallHelp<hdfs::tools::test::MkdirMock>,
        PassAPath<hdfs::tools::test::MkdirMock>,
        PassPOptAndPath<hdfs::tools::test::MkdirMock>,
        PassMOptPermissionsAndAPath<hdfs::tools::test::MkdirMock>,
        PassMPOptsPermissionsAndAPath<hdfs::tools::test::MkdirMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsRm, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::RmMock>,
                    PassAPath<hdfs::tools::test::RmMock>,
                    PassRecursivePath<hdfs::tools::test::RmMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsFind, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::FindMock>,
                    PassAPath<hdfs::tools::test::FindMock>,
                    PassNStrMNumAndAPath<hdfs::tools::test::FindMock>,
                    PassMOptPermissionsAndAPath<hdfs::tools::test::FindMock>,
                    PassNOptAndAPath<hdfs::tools::test::FindMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsSetrep, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::SetrepMock>,
                    PassPermissionsAndAPath<hdfs::tools::test::SetrepMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsStat, HdfsToolBasicTest,
    testing::Values(CallHelp<hdfs::tools::test::StatMock>,
                    PassAPath<hdfs::tools::test::StatMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsTail, HdfsToolBasicTest,
    testing::Values(PassAPath<hdfs::tools::test::TailMock>,
                    CallHelp<hdfs::tools::test::TailMock>,
                    PassFOptAndAPath<hdfs::tools::test::TailMock>));

// Negative tests
INSTANTIATE_TEST_SUITE_P(
    HdfsAllowSnapshot, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::AllowSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsRenameSnapshot, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::RenameSnapshotMock>,
                    Pass2Paths<hdfs::tools::test::RenameSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCreateSnapshot, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::CreateSnapshotMock>,
                    Pass3Paths<hdfs::tools::test::CreateSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDisallowSnapshot, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::DisallowSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDf, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::DfMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDu, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::DuMock>,
                    Pass3Paths<hdfs::tools::test::DuMock>,
                    PassNOptAndAPath<hdfs::tools::test::DuMock>,
                    PassOwnerAndAPath<hdfs::tools::test::DuMock>,
                    PassPermissionsAndAPath<hdfs::tools::test::DuMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsLs, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::LsMock>,
                    Pass3Paths<hdfs::tools::test::LsMock>,
                    PassNOptAndAPath<hdfs::tools::test::LsMock>,
                    PassOwnerAndAPath<hdfs::tools::test::LsMock>,
                    PassPermissionsAndAPath<hdfs::tools::test::LsMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCat, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::CatMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCopyToLocal, HdfsToolNegativeTestThrows,
    testing::Values(Pass3Paths<hdfs::tools::test::CopyToLocalMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsGet, HdfsToolNegativeTestThrows,
    testing::Values(Pass3Paths<hdfs::tools::test::GetMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsMoveToLocal, HdfsToolNegativeTestThrows,
    testing::Values(Pass3Paths<hdfs::tools::test::MoveToLocalMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCount, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::CountMock>,
                    Pass3Paths<hdfs::tools::test::CountMock>,
                    PassNOptAndAPath<hdfs::tools::test::CountMock>,
                    PassRecursive<hdfs::tools::test::CountMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsMkdir, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::MkdirMock>,
                    Pass3Paths<hdfs::tools::test::MkdirMock>,
                    PassNOptAndAPath<hdfs::tools::test::MkdirMock>,
                    PassRecursive<hdfs::tools::test::MkdirMock>,
                    PassMOpt<hdfs::tools::test::MkdirMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsRm, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::RmMock>,
                    Pass3Paths<hdfs::tools::test::RmMock>,
                    PassNOptAndAPath<hdfs::tools::test::RmMock>,
                    PassRecursiveOwnerAndAPath<hdfs::tools::test::RmMock>,
                    PassMOpt<hdfs::tools::test::RmMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsFind, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::FindMock>,
                    Pass3Paths<hdfs::tools::test::FindMock>,
                    PassRecursiveOwnerAndAPath<hdfs::tools::test::FindMock>,
                    PassRecursive<hdfs::tools::test::FindMock>,
                    PassRecursivePath<hdfs::tools::test::FindMock>,
                    PassMPOptsPermissionsAndAPath<hdfs::tools::test::FindMock>,
                    PassMOpt<hdfs::tools::test::FindMock>,
                    PassNOpt<hdfs::tools::test::FindMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChgrp, HdfsToolNegativeTestThrows,
    testing::Values(PassNOptAndAPath<hdfs::tools::test::ChgrpMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsSetrep, HdfsToolNegativeTestThrows,
    testing::Values(
        Pass3Paths<hdfs::tools::test::SetrepMock>,
        PassRecursiveOwnerAndAPath<hdfs::tools::test::SetrepMock>,
        PassRecursive<hdfs::tools::test::SetrepMock>,
        PassMPOptsPermissionsAndAPath<hdfs::tools::test::SetrepMock>,
        PassMOpt<hdfs::tools::test::SetrepMock>,
        PassNOpt<hdfs::tools::test::SetrepMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsStat, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::StatMock>,
                    Pass3Paths<hdfs::tools::test::StatMock>,
                    PassRecursiveOwnerAndAPath<hdfs::tools::test::StatMock>,
                    PassRecursive<hdfs::tools::test::StatMock>,
                    PassRecursivePath<hdfs::tools::test::StatMock>,
                    PassMPOptsPermissionsAndAPath<hdfs::tools::test::StatMock>,
                    PassMOpt<hdfs::tools::test::StatMock>,
                    PassNOpt<hdfs::tools::test::StatMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsTail, HdfsToolNegativeTestThrows,
    testing::Values(Pass2Paths<hdfs::tools::test::TailMock>,
                    Pass3Paths<hdfs::tools::test::TailMock>,
                    PassNOptAndAPath<hdfs::tools::test::TailMock>,
                    PassRecursiveOwnerAndAPath<hdfs::tools::test::TailMock>,
                    PassMOpt<hdfs::tools::test::TailMock>,
                    PassRecursive<hdfs::tools::test::TailMock>,
                    PassRecursivePath<hdfs::tools::test::TailMock>,
                    PassNOpt<hdfs::tools::test::TailMock>,
                    PassOwnerAndAPath<hdfs::tools::test::TailMock>,
                    PassMPOptsPermissionsAndAPath<hdfs::tools::test::TailMock>,
                    PassPermissionsAndAPath<hdfs::tools::test::TailMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsRm, HdfsToolNegativeTestNoThrow,
    testing::Values(PassRecursive<hdfs::tools::test::RmMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsMkdir, HdfsToolNegativeTestNoThrow,
    testing::Values(PassPOpt<hdfs::tools::test::MkdirMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCount, HdfsToolNegativeTestNoThrow,
    testing::Values(PassQOpt<hdfs::tools::test::CountMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsMoveToLocal, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::MoveToLocalMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsCopyToLocal, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::CopyToLocalMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsGet, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::GetMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDeleteSnapshot, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::DeleteSnapshotMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsDu, HdfsToolNegativeTestNoThrow,
    testing::Values(PassRecursive<hdfs::tools::test::DuMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsLs, HdfsToolNegativeTestNoThrow,
    testing::Values(PassRecursive<hdfs::tools::test::LsMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChown, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::ChownMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChown, HdfsToolNegativeTestThrows,
    testing::Values(PassNOptAndAPath<hdfs::tools::test::ChownMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChmod, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::ChmodMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChmod, HdfsToolNegativeTestThrows,
    testing::Values(PassNOptAndAPath<hdfs::tools::test::ChmodMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsChgrp, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::ChgrpMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsSetrep, HdfsToolNegativeTestNoThrow,
    testing::Values(PassAPath<hdfs::tools::test::SetrepMock>));

INSTANTIATE_TEST_SUITE_P(
    HdfsTail, HdfsToolNegativeTestNoThrow,
    testing::Values(PassFOpt<hdfs::tools::test::TailMock>));
