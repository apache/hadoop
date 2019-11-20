package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.INodeType;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.syncservice.DefaultSyncServiceFileFilterImpl;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceFileFilter;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.RenameEntryWithTemporaryName;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.TranslatedEntry;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestPartitionedDiffReport {

  @Mock
  private SnapshotDiffReport diffReport;

  @Test
  public void emptyDiffReport() throws Exception {
    List<DiffReportEntry> diffReports = Lists.newArrayList();
    when(diffReport.getDiffList()).thenReturn(diffReports);

    List<RenameEntryWithTemporaryName> actual = PartitionedDiffReport
        .getRenameEntriesAndGenerateTemporaryNames(diffReport.getDiffList());

    assertThat(actual).isEmpty();
  }

  @Test
  public void renameDiffReports () {
    DiffReportEntry entry1 = new DiffReportEntry(INodeType.FILE, DiffType.RENAME,
        "sourcePath1".getBytes());
    DiffReportEntry entry2 = new DiffReportEntry(INodeType.FILE, DiffType.RENAME,
        "sourcePath2".getBytes());
    DiffReportEntry entry3 = new DiffReportEntry(INodeType.FILE, DiffType.RENAME,
        "sourcePath3".getBytes());

    List<DiffReportEntry> diffReports = Lists.newArrayList(entry1, entry2, entry3);

    when(diffReport.getDiffList()).thenReturn(diffReports);

    List<RenameEntryWithTemporaryName> actual = PartitionedDiffReport
        .getRenameEntriesAndGenerateTemporaryNames(diffReport.getDiffList());

    assertThat(actual).hasSize(3);
    List<DiffReportEntry> diffReportEntries = actual
        .stream()
        .map(RenameEntryWithTemporaryName::getEntry)
        .collect(Collectors.toList());

    assertThat(diffReportEntries).contains(entry1, entry2, entry3);
  }

  @Test
  public void deletesWithoutRenames () {
    DiffReportEntry entry1 = new DiffReportEntry(INodeType.FILE, DiffType.DELETE,
        "sourcePath1".getBytes());
    DiffReportEntry entry2 = new DiffReportEntry(INodeType.FILE, DiffType.DELETE,
        "sourcePath2".getBytes());
    DiffReportEntry entry3 = new DiffReportEntry(INodeType.FILE, DiffType.DELETE,
        "sourcePath3".getBytes());
    List<DiffReportEntry> diffReports = Lists.newArrayList(entry1, entry2,
        entry3);
    when(diffReport.getDiffList()).thenReturn(diffReports);

    List<TranslatedEntry> actual =
        PartitionedDiffReport
          .handleDeletes(Lists.newArrayList(), diffReport, new DefaultSyncServiceFileFilterImpl());

    assertThat(actual).hasSize(3);
    List<DiffReportEntry> diffReportEntries = actual
        .stream()
        .map(TranslatedEntry::getEntry)
        .collect(Collectors.toList());

    assertThat(diffReportEntries).contains(entry1, entry2, entry3);
  }


  @Test
  public void deletesWithRenames () {
    String filePath2 = "sourcePath2";
    String filePath3 = "sourcePath3";
    byte[] sourcePath1 = "sourcePath1".getBytes();
    byte[] sourcePath2 = ("sourcePath1/" + filePath2).getBytes();
    byte[] sourcePath3 = ("sourcePath1/" + filePath3).getBytes();
    DiffReportEntry entry2 = new DiffReportEntry(INodeType.FILE, DiffType.DELETE,
        sourcePath2);
    DiffReportEntry entry3 = new DiffReportEntry(INodeType.FILE, DiffType.DELETE,
        sourcePath3);
    List<DiffReportEntry> diffReports = Lists.newArrayList(entry2,
        entry3);
    when(diffReport.getDiffList()).thenReturn(diffReports);

    RenameEntryWithTemporaryName rename1 = new RenameEntryWithTemporaryName(
        new DiffReportEntry(INodeType.FILE, DiffType.RENAME,
            sourcePath1));
    String temporaryName = rename1.getTemporaryName();

    List<RenameEntryWithTemporaryName> renames = Lists.newArrayList(rename1);

    List<TranslatedEntry> actual =
        PartitionedDiffReport
            .handleDeletes(renames, diffReport, new DefaultSyncServiceFileFilterImpl());

    assertThat(actual).hasSize(2);
    List<String> diffReportEntries = actual
        .stream()
        .map(TranslatedEntry::getTranslatedName)
        .collect(Collectors.toList());

    String expectedName2 = temporaryName + "/" + filePath2;
    String expectedName3 = temporaryName + "/" + filePath3;

    assertThat(diffReportEntries).containsExactly(expectedName2, expectedName3);
  }


  @Test
  public void createsWithRenames () {
    String filePath2 = "sourcePath2";
    String filePath3 = "sourcePath3";
    String targetPath = "doelwit";
    byte[] target = ((String) targetPath).getBytes();

    byte[] sourcePath1 = "sourcePath1".getBytes();
    byte[] sourcePath2 = ("sourcePath1/" + filePath2).getBytes();
    byte[] sourcePath3 = ("sourcePath1/" + filePath3).getBytes();
    DiffReportEntry entry2 = new DiffReportEntry(INodeType.FILE, DiffType.CREATE,
        sourcePath2);
    DiffReportEntry entry3 = new DiffReportEntry(INodeType.FILE, DiffType.CREATE,
        sourcePath3);
    List<DiffReportEntry> diffReports = Lists.newArrayList(entry2,
        entry3);
    when(diffReport.getDiffList()).thenReturn(diffReports);


    RenameEntryWithTemporaryName rename1 = new RenameEntryWithTemporaryName(
        new DiffReportEntry(INodeType.FILE, DiffType.RENAME,
            sourcePath1, target));
    String temporaryName = rename1.getTemporaryName();

    List<RenameEntryWithTemporaryName> renames = Lists.newArrayList(rename1);

    List<TranslatedEntry> actual =
        PartitionedDiffReport
            .handleCreates(renames, diffReport, new DefaultSyncServiceFileFilterImpl());

    assertThat(actual).hasSize(2);
    List<String> diffReportEntries = actual
        .stream()
        .map(TranslatedEntry::getTranslatedName)
        .collect(Collectors.toList());

    String expectedName2 = targetPath + "/" + filePath2;
    String expectedName3 = targetPath + "/" + filePath3;

    assertThat(diffReportEntries).containsExactly(expectedName2, expectedName3);
  }

  @Test
  public void modifiesWithRenames () {
    String filePath2 = "sourcePath2";
    String filePath3 = "sourcePath3";
    String targetPath = "doelwit";
    byte[] target = ((String) targetPath).getBytes();

    byte[] sourcePath1 = "sourcePath1".getBytes();
    byte[] sourcePath2 = ("sourcePath1/" + filePath2).getBytes();
    byte[] sourcePath3 = ("sourcePath1/" + filePath3).getBytes();
    DiffReportEntry entry2 = new DiffReportEntry(INodeType.FILE, DiffType.MODIFY,
        sourcePath2);
    DiffReportEntry entry3 = new DiffReportEntry(INodeType.FILE, DiffType.MODIFY,
        sourcePath3);
    List<DiffReportEntry> diffReports = Lists.newArrayList(entry2,
        entry3);
    when(diffReport.getDiffList()).thenReturn(diffReports);


    RenameEntryWithTemporaryName rename1 = new RenameEntryWithTemporaryName(
        new DiffReportEntry(INodeType.FILE, DiffType.RENAME,
            sourcePath1, target));
    String temporaryName = rename1.getTemporaryName();

    List<RenameEntryWithTemporaryName> renames = Lists.newArrayList(rename1);

    List<TranslatedEntry> actual =
        PartitionedDiffReport
            .handleModifies(renames, diffReport, new DefaultSyncServiceFileFilterImpl());

    assertThat(actual).hasSize(2);
    List<String> diffReportEntries = actual
        .stream()
        .map(TranslatedEntry::getTranslatedName)
        .collect(Collectors.toList());

    String expectedName2 = targetPath + "/" + filePath2;
    String expectedName3 = targetPath + "/" + filePath3;

    assertThat(diffReportEntries).containsExactly(expectedName2, expectedName3);
  }

  @Test
  public void fileFilterUsedCorrectlyForCreate() {
    SyncServiceFileFilter syncServiceFileFilter =
        new DefaultSyncServiceFileFilterImpl();

    String filePath = "sourcePath._COPYING_";
    byte[] sourcePath = ("sourcePath/" + filePath).getBytes();
    DiffReportEntry entry = new DiffReportEntry(
        INodeType.FILE,
        DiffType.CREATE,
        sourcePath);
    List<DiffReportEntry> diffReports = Lists.newArrayList(entry);

    when(diffReport.getDiffList()).thenReturn(diffReports);

    PartitionedDiffReport partition =
        PartitionedDiffReport.partition(diffReport, syncServiceFileFilter);

    assertThat(partition.getRenames()).isEmpty();
    assertThat(partition.getCreates()).isEmpty();
    assertThat(partition.getDeletes()).isEmpty();
    assertThat(partition.getModifies()).isEmpty();
    assertThat(partition.getCreatesFromRenames()).isEmpty();
  }


  @Test
  public void fileFilterUsedCorrectlyForModify() {
    SyncServiceFileFilter syncServiceFileFilter =
        new DefaultSyncServiceFileFilterImpl();

    String filePath = "sourcePath._COPYING_";
    byte[] sourcePath = ("sourcePath/" + filePath).getBytes();
    DiffReportEntry entry = new DiffReportEntry(
        INodeType.FILE,
        DiffType.MODIFY,
        sourcePath);
    List<DiffReportEntry> diffReports = Lists.newArrayList(entry);

    when(diffReport.getDiffList()).thenReturn(diffReports);

    PartitionedDiffReport partition =
        PartitionedDiffReport.partition(diffReport, syncServiceFileFilter);

    assertThat(partition.getRenames()).isEmpty();
    assertThat(partition.getCreates()).isEmpty();
    assertThat(partition.getDeletes()).isEmpty();
    assertThat(partition.getModifies()).isEmpty();
    assertThat(partition.getCreatesFromRenames()).isEmpty();
  }


  @Test
  public void fileFilterUsedCorrectlyForModifyForRename() {
    SyncServiceFileFilter syncServiceFileFilter =
        new DefaultSyncServiceFileFilterImpl();

    String filePath = "sourcePath._COPYING_";
    String filePathTo = "sourcePath.txt";
    byte[] sourcePath = ("sourcePath/" + filePath).getBytes();
    byte[] targetPath = ("targetPath/" + filePathTo).getBytes();
    DiffReportEntry expected = new DiffReportEntry(
        INodeType.FILE,
        DiffType.RENAME,
        sourcePath,
        targetPath);
    List<DiffReportEntry> diffReports = Lists.newArrayList(expected);

    when(diffReport.getDiffList()).thenReturn(diffReports);

    PartitionedDiffReport partition =
        PartitionedDiffReport.partition(diffReport, syncServiceFileFilter);

    assertThat(partition.getRenames()).isEmpty();
    assertThat(partition.getCreates()).isEmpty();
    assertThat(partition.getDeletes()).isEmpty();
    assertThat(partition.getModifies()).isEmpty();
    assertThat(partition.getCreatesFromRenames())
        .containsExactly(expected);
  }

}