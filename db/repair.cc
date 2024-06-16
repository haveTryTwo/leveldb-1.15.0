// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// We recover the contents of the descriptor from the other files we find.
// (1) Any log files are first converted to tables
// (2) We scan every table to compute
//     (a) smallest/largest for the table
//     (b) largest sequence number in the table
// (3) We generate descriptor contents:
//      - log number is set to zero
//      - next-file-number is set to 1 + largest file number we found
//      - last-sequence-number is set to largest sequence# found across
//        all tables (see 2c)
//      - compaction pointers are cleared
//      - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"

namespace leveldb {

namespace {

class Repairer {
 public:
  Repairer(const std::string& dbname, const Options& options)
      : dbname_(dbname),
        env_(options.env),
        icmp_(options.comparator),
        ipolicy_(options.filter_policy),
        options_(SanitizeOptions(dbname, &icmp_, &ipolicy_, options)),
        owns_info_log_(options_.info_log != options.info_log),
        owns_cache_(options_.block_cache != options.block_cache),
        next_file_number_(1) {
    // TableCache can be small since we expect each table to be opened once.
    table_cache_ = new TableCache(dbname_, &options_, 10);
  }

  ~Repairer() {
    delete table_cache_;
    if (owns_info_log_) {  // NOTE:htt, 独有日志info log则删除该文件
      delete options_.info_log;
    }
    if (owns_cache_) {
      delete options_.block_cache;
    }
  }

  Status
  Run() {  // NOTE:htt,
           // 恢复leveldb文件,先将WAL日志数据写入到ldb/sst中,并构建ldb/sst文件列表信息,再重新生成mainfest-1(并写入${db}/CURRENT)
    Status status = FindFiles();  // NOTE:htt, 获取WAL、sst/ldb、mainfest文件
    if (status.ok()) {
      ConvertLogFilesToTables();   // NOTE:htt, 读取WAL日志写入到sst文件中,并将WAL日志移走
      ExtractMetaData();           // NOTE:htt, 获取sst/ldb文件元信息
      status = WriteDescriptor();  // NOTE:htt, 根据next_file_num/max_seq 以及 ldb/sst文件列表重新生成
                                   // mainfest-1文件,并写入到${db}/CURRENT
    }
    if (status.ok()) {
      unsigned long long bytes = 0;
      for (size_t i = 0; i < tables_.size(); i++) {
        bytes += tables_[i].meta.file_size;
      }
      Log(options_.info_log,
          "**** Repaired leveldb %s; "
          "recovered %d files; %llu bytes. "
          "Some data may have been lost. "
          "****",
          dbname_.c_str(), static_cast<int>(tables_.size()),
          bytes);  // NOTE:htt, 记录恢复的文件信息
    }
    return status;
  }

 private:
  struct TableInfo {              // NOTE:htt, sst文件信息
    FileMetaData meta;            // NOTE:htt, 文件元信息
    SequenceNumber max_sequence;  // NOTE:htt, seq
  };

  std::string const dbname_;
  Env* const env_;
  InternalKeyComparator const icmp_;
  InternalFilterPolicy const ipolicy_;
  Options const options_;
  bool owns_info_log_;       // NOTE:htt, 是否有启动日志
  bool owns_cache_;          // NOTE:htt, 是否有独立的cache
  TableCache* table_cache_;  // NOTE:htt, 读取file_number对应文件, 并缓存{ sst_file_numbe, {file, sst_talbe} }
  VersionEdit edit_;         // NOTE:htt, edit信息

  std::vector<std::string> manifests_;   // NOTE:htt, mainfest文件
  std::vector<uint64_t> table_numbers_;  // NOTE:htt, sst或ldb文件num
  std::vector<uint64_t> logs_;           // NOTE:htt, WAL日志文件num
  std::vector<TableInfo> tables_;        // NOTE:htt, 保存每个sst文件的信息
  uint64_t next_file_number_;            // NOTE:htt, 初始化为1, 下一个sst文件num值

  Status FindFiles() {  // NOTE:htt, 获取WAL、sst/ldb、mainfest文件
    std::vector<std::string> filenames;
    Status status = env_->GetChildren(dbname_, &filenames);
    if (!status.ok()) {
      return status;
    }
    if (filenames.empty()) {
      return Status::IOError(dbname_, "repair found no files");
    }

    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        if (type == kDescriptorFile) {  // NOTE:htt, mainfest文件
          manifests_.push_back(filenames[i]);
        } else {
          if (number + 1 > next_file_number_) {
            next_file_number_ = number + 1;  // NOTE:htt, 生成下一个文件文件num
          }
          if (type == kLogFile) {  // NOTE:htt, WAL日志
            logs_.push_back(number);
          } else if (type == kTableFile) {  // NOTE:htt, sst/ldb文件
            table_numbers_.push_back(number);
          } else {
            // Ignore other files
          }
        }
      }
    }
    return status;
  }

  void ConvertLogFilesToTables() {  // NOTE:htt, 读取WAL日志写入到sst文件中,并将WAL日志移走
    for (size_t i = 0; i < logs_.size(); i++) {
      std::string logname = LogFileName(dbname_, logs_[i]);  // NOTE:htt, WAL日志文件名, ${name}/${number}.log
      Status status = ConvertLogToTable(logs_[i]);  // NOTE:htt, 将WAL日志数据写入 sst中(先写入mem,在写入到sst中)
      if (!status.ok()) {
        Log(options_.info_log, "Log #%llu: ignoring conversion error: %s", (unsigned long long)logs_[i],
            status.ToString().c_str());
      }
      ArchiveFile(logname);  // NOTE:htt,移动 WAL 日志文件
    }
  }

  Status ConvertLogToTable(uint64_t log) {  // NOTE:htt, 将WAL日志数据写入 sst中(先写入mem,在写入到sst中)
    struct LogReporter : public log::Reader::Reporter {  // NOTE:htt, info log reporter
      Env* env;
      Logger* info_log;
      uint64_t lognum;  // NOTE:htt, WAL日志num
      virtual void Corruption(size_t bytes, const Status& s) {
        // We print error messages for corruption, but continue repairing.
        Log(info_log, "Log #%llu: dropping %d bytes; %s", (unsigned long long)lognum, static_cast<int>(bytes),
            s.ToString().c_str());
      }
    };

    // Open the log file
    std::string logname = LogFileName(dbname_, log);  // NOTE:htt, WAL日志文件名, ${name}/${number}.log
    SequentialFile* lfile;
    Status status = env_->NewSequentialFile(logname, &lfile);
    if (!status.ok()) {
      return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;  // NOTE:htt, info日志
    reporter.lognum = log;
    // We intentially make log::Reader do checksumming so that
    // corruptions cause entire commits to be skipped instead of
    // propagating bad information (like overly large sequence
    // numbers).
    log::Reader reader(lfile, &reporter, false /*do not checksum*/,
                       0 /*initial_offset*/);  // NOTE:htt, 读取WAL中完整的日志信息, Full或<First,Mid,...,Last>组合

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    MemTable* mem = new MemTable(icmp_);
    mem->Ref();
    int counter = 0;
    while (reader.ReadRecord(&record, &scratch)) {  // NOTE:htt, 读取一条WAL日志
      if (record.size() < 12) {                     // NOTE:htt, 日志个数 ${seq}${count}[{${type}${key}${value}}]
        reporter.Corruption(record.size(), Status::Corruption("log record too small"));
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      status = WriteBatchInternal::InsertInto(&batch, mem);  // NOTE:htt, 将WriteBatch中记录逐条写入到MemTable中
      if (status.ok()) {
        counter += WriteBatchInternal::Count(&batch);  // NOTE:htt, 获取rep缓存的记录个数
      } else {
        Log(options_.info_log, "Log #%llu: ignoring %s", (unsigned long long)log, status.ToString().c_str());
        status = Status::OK();  // Keep going with rest of file
      }
    }
    delete lfile;

    // Do not record a version edit for this conversion to a Table
    // since ExtractMetaData() will also generate edits.
    FileMetaData meta;
    meta.number = next_file_number_++;  // NOTE:htt,下一个文件序号
    Iterator* iter = mem->NewIterator();
    status = BuildTable(dbname_, env_, options_, table_cache_, iter,
                        &meta);  // NOTE:htt, 将iter中的数据内容写入到sst文件中(dbname为前缀),生成元信息到meta
    delete iter;
    mem->Unref();
    mem = NULL;
    if (status.ok()) {
      if (meta.file_size > 0) {
        table_numbers_.push_back(meta.number);  // NOTE:htt, sst或ldb文件num
      }
    }
    Log(options_.info_log, "Log #%llu: %d ops saved to Table #%llu %s", (unsigned long long)log, counter,
        (unsigned long long)meta.number, status.ToString().c_str());
    return status;
  }

  void ExtractMetaData() {  // NOTE:htt, 获取sst/ldb文件元信息
    std::vector<TableInfo> kept;
    for (size_t i = 0; i < table_numbers_.size(); i++) {
      ScanTable(
          table_numbers_
              [i]);  // NOTE:htt,
                     // 扫描ldb/sst文件,获取文件大小,最大值/最小值,以及文件最大的seq,如果文件有异常则尝试修复文件（注意可能会丢数据）
    }
  }

  Iterator* NewTableIterator(const FileMetaData& meta) {  // NOTE:htt, 读取file_number文件,并构建talbe的两层迭代器
    // Same as compaction iterators: if paranoid_checks are on, turn
    // on checksum verification.
    ReadOptions r;
    r.verify_checksums = options_.paranoid_checks;
    return table_cache_->NewIterator(r, meta.number,
                                     meta.file_size);  // NOTE:htt, 读取file_number文件,并构建talbe的两层迭代器
  }

  void ScanTable(
      uint64_t
          number) {  // NOTE:htt,
                     // 扫描ldb/sst文件,获取文件大小,最大值/最小值,以及文件最大的seq,如果文件有异常则尝试修复文件（注意可能会丢数据）
    TableInfo t;
    t.meta.number = number;                                       // NOTE:htt, 扫描文件
    std::string fname = TableFileName(dbname_, number);           // NOTE:htt, 表文件名, ${name}/${number}.ldb
    Status status = env_->GetFileSize(fname, &t.meta.file_size);  // NOTE:htt, 获取sst文件长度
    if (!status.ok()) {
      // Try alternate file name.
      fname = SSTTableFileName(dbname_, number);  // NOTE:htt, 尝试读sst文件名, ${name}/${number}.sst
      Status s2 = env_->GetFileSize(fname, &t.meta.file_size);
      if (s2.ok()) {
        status = Status::OK();
      }
    }
    if (!status.ok()) {  // NOTE:htt, 如果读取失败,则尝试移走 sst或ldb文件
      ArchiveFile(TableFileName(dbname_, number));
      ArchiveFile(SSTTableFileName(dbname_, number));
      Log(options_.info_log, "Table #%llu: dropped: %s", (unsigned long long)t.meta.number, status.ToString().c_str());
      return;
    }

    // Extract metadata by scanning through table.
    int counter = 0;
    Iterator* iter = NewTableIterator(t.meta);  // NOTE:htt, 读取sst文件,并构建table的两层迭代器
    bool empty = true;
    ParsedInternalKey parsed;
    t.max_sequence = 0;  // NOTE:htt, 初始化max_sequence为0
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Slice key = iter->key();  // NOTE:htt, sst 文件中key
      if (!ParseInternalKey(key, &parsed)) {
        Log(options_.info_log, "Table #%llu: unparsable key %s", (unsigned long long)t.meta.number,
            EscapeString(key).c_str());
        continue;
      }

      counter++;
      if (empty) {
        empty = false;
        t.meta.smallest.DecodeFrom(key);  // NOTE:htt, 最小key
      }
      t.meta.largest.DecodeFrom(key);  // NOTE:htt, 最大key
      if (parsed.sequence > t.max_sequence) {
        t.max_sequence = parsed.sequence;  // NOTE:htt, 获取当前的最大的seq,之所以顺序遍历,应该就是为了获取最大的seq
      }
    }
    if (!iter->status().ok()) {
      status = iter->status();
    }
    delete iter;
    Log(options_.info_log, "Table #%llu: %d entries %s", (unsigned long long)t.meta.number, counter,
        status.ToString().c_str());

    if (status.ok()) {
      tables_.push_back(t);  // NOTE:htt, 保存sst文件信息
    } else {
      RepairTable(
          fname,
          t);  // RepairTable archives input file.// NOTE:htt, 读取src文件内容写入到新的copy文件,在重命名为原有src文件
    }
  }

  void RepairTable(const std::string& src,
                   TableInfo t) {  // NOTE:htt, 读取src文件内容写入到新的copy文件,在重命名为原有src文件
    // We will copy src contents to a new table and then rename the
    // new table over the source.

    // Create builder.
    std::string copy = TableFileName(dbname_, next_file_number_++);
    WritableFile* file;
    Status s = env_->NewWritableFile(copy, &file);
    if (!s.ok()) {
      return;
    }
    TableBuilder* builder = new TableBuilder(options_, file);

    // Copy data.
    Iterator* iter = NewTableIterator(t.meta);
    int counter = 0;
    for (iter->SeekToFirst(); iter->Valid();
         iter->Next()) {  // NOTE:htt, 复制有效内容,注意可能会丢失数据,尤其后续内容读取不成功
      builder->Add(iter->key(), iter->value());
      counter++;
    }
    delete iter;

    ArchiveFile(src);  // NOTE:htt,移动文件至废弃lost目录下
    if (counter == 0) {
      builder->Abandon();  // Nothing to save
    } else {
      s = builder->Finish();  // NOTE:htt, 写入整个ldb/sst文件
      if (s.ok()) {
        t.meta.file_size = builder->FileSize();
      }
    }
    delete builder;
    builder = NULL;

    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (counter > 0 && s.ok()) {
      std::string orig = TableFileName(dbname_, t.meta.number);
      s = env_->RenameFile(copy, orig);  // NOTE:htt, 将新的ldb文件重命名为原有sst/ldb文件
      if (s.ok()) {
        Log(options_.info_log, "Table #%llu: %d entries repaired", (unsigned long long)t.meta.number, counter);
        tables_.push_back(t);  // NOTE:htt, 保存sst/ldb文件信息
      }
    }
    if (!s.ok()) {
      env_->DeleteFile(copy);
    }
  }

  Status WriteDescriptor() {  // NOTE:htt, 根据next_file_num/max_seq 以及 ldb/sst文件列表重新生成
                              // mainfest-1文件,并写入到${db}/CURRENT
    std::string tmp = TempFileName(dbname_, 1);  // NOTE:htt, 临时文件名, ${dbname}/1.dbtmp
    WritableFile* file;
    Status status = env_->NewWritableFile(tmp, &file);
    if (!status.ok()) {
      return status;
    }

    SequenceNumber max_sequence = 0;
    for (size_t i = 0; i < tables_.size(); i++) {
      if (max_sequence < tables_[i].max_sequence) {
        max_sequence = tables_[i].max_sequence;
      }
    }

    edit_.SetComparatorName(icmp_.user_comparator()->Name());
    edit_.SetLogNumber(0);
    edit_.SetNextFile(next_file_number_);
    edit_.SetLastSequence(max_sequence);

    for (size_t i = 0; i < tables_.size(); i++) {
      // TODO(opt): separate out into multiple levels
      const TableInfo& t = tables_[i];
      edit_.AddFile(0, t.meta.number, t.meta.file_size, t.meta.smallest,
                    t.meta.largest);  // NOTE:htt, 添加所有的ldb文件信息到VersionEdit中
    }

    // fprintf(stderr, "NewDescriptor:\n%s\n", edit_.DebugString().c_str());
    {
      log::Writer log(file);
      std::string record;
      edit_.EncodeTo(&record);
      status = log.AddRecord(record);  // NOTE:htt, 将VersionEdit信息写入到mainfest文件中
    }
    if (status.ok()) {
      status = file->Close();
    }
    delete file;
    file = NULL;

    if (!status.ok()) {
      env_->DeleteFile(tmp);
    } else {
      // Discard older manifests
      for (size_t i = 0; i < manifests_.size(); i++) {
        ArchiveFile(dbname_ + "/" + manifests_[i]);  // NOTE:htt, 将原有的mainfest文件移动到lost目录下
      }

      // Install new manifest
      status = env_->RenameFile(tmp, DescriptorFileName(dbname_, 1));  // NOTE:htt, 生成新的 ${dbname}/MANIFEST-1文件
      if (status.ok()) {
        status = SetCurrentFile(env_, dbname_, 1);  // NOTE:htt, 将MANIFEST-1值写入到${dbname}/CURRENT
      } else {
        env_->DeleteFile(tmp);
      }
    }
    return status;
  }

  void ArchiveFile(const std::string& fname) {  // NOTE:htt,移动文件至废弃lost目录下
    // Move into another directory.  E.g., for
    //    dir/foo
    // rename to
    //    dir/lost/foo
    const char* slash = strrchr(fname.c_str(), '/');
    std::string new_dir;
    if (slash != NULL) {
      new_dir.assign(fname.data(), slash - fname.data());
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir);  // Ignore error
    std::string new_file = new_dir;
    new_file.append("/");
    new_file.append((slash == NULL) ? fname.c_str() : slash + 1);  // NOTE:htt, ${db}/lost/${number}.log
    Status s = env_->RenameFile(fname, new_file);  // NOTE:htt, 将 ${db}/${number}.log 移动到 ${db}/lost/${number}.log中
    Log(options_.info_log, "Archiving %s: %s\n", fname.c_str(), s.ToString().c_str());
  }
};
}  // namespace

Status RepairDB(
    const std::string& dbname,
    const Options&
        options) {  // NOTE:htt,
                    // 恢复leveldb文件,先将WAL日志数据写入到ldb/sst中,并构建ldb/sst文件列表信息,再重新生成mainfest-1(并写入${db}/CURRENT)
  Repairer repairer(dbname, options);
  return repairer
      .Run();  // NOTE:htt,
               // 恢复leveldb文件,先将WAL日志数据写入到ldb/sst中,并构建ldb/sst文件列表信息,再重新生成mainfest-1(并写入${db}/CURRENT)
}

}  // namespace leveldb
