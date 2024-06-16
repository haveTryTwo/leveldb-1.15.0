// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>
#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {  // NOTE:htt, 写操作,包括批量写操作,以及是否sync和done/*{{{*/
  Status status;
  WriteBatch* batch;  // NOTE:htt, 批量写操作,将记录先编码到rep_,再有WriteBatchInternal实现记录追加到MemTable
  bool sync;  // NOTE:htt, 是否sync
  bool done;  // NOTE:htt, 是否完成
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) {}
}; /*}}}*/

struct DBImpl::CompactionState {  // NOTE:htt, compaction状态,包括compaction后的文件列表
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;  // NOTE:htt, 快照的最小seqNumber

  // Files produced by compaction
  struct Output {     // NOTE:htt, compaction产生文件,包括number, file_size, 最小/最大key
    uint64_t number;  // NOTE:htt, 文件number
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;  // NOTE:htt, compaction文件列表

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;  // NOTE:htt, compact总的文件大小

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c) : compaction(c), outfile(NULL), builder(NULL), total_bytes(0) {}
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {  // NOTE:htt, 调整值在[minvalue, maxvalue]范围之间
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname, const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {  // NOTE:htt, 设置db的options,如果需要生成日志则创建日志
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);  // NOTE:htt, open files范围[64+10, 50000]
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);                // NOTE:htt, write buffer范围[64K, 1G]
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);                        // NOTE:htt, block大小范围[1k, 4M]
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname),
                        OldInfoLogFileName(dbname));  // NOTE:htt,将原有的{dbname}/LOG文件 重命{dbname}/LOG.old
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);  // NOTE: htt, 创建日志文件对象
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);  // NOTE: htt, 生成8M大小的ShardedLRUCache
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(new MemTable(internal_comparator_)),  // NOTE:htt, 可变内存
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL) {
  mem_->Ref();  // NOTE:htt, 增加内存引用
  has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);  // NOTE: htt, 释放锁
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;  // NOTE:htt, 释放info log
  }
  if (owns_cache_) {
    delete options_.block_cache;  // NOTE:htt, 释放block_cache对象
  }
}

Status DBImpl::NewDB() {  // NOTE:htt, 生成 MANIFEST-1 文件, 即DB文件是新创建
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);  // NOTE:htt, 描述文件, ${dbname}/MANIFEST-1
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);  // NOTE:htt, 将VersionEdit内容序列化
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);  // NOTE:htt, 将MANIFEST-1 值写入到${dbname}/CURRENT
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {  // NOTE:htt, 忽略错误
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());  // NOTE:htt, 记录忽略错误
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {  // NOTE:htt, 删除无用的文件
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);  // NOTE:htt, 添加level所有Version下的共同存活的文件

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose // NOTE:htt, 获取dbname_目录下文件列表
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {  // NOTE:htt, 从fname中获取 number 和 文件类型
      bool keep = true;
      switch (type) {
        case kLogFile:  // NOTE:htt, WAL日志
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));  // NOTE:htt, 如果比记录WAL日志number大则保存
          break;
        case kDescriptorFile:  // NOTE:htt, MANIFEST-* 文件
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());  // NOTE:htt, 记录mainfest的version是否比VersionSet大
          break;
        case kTableFile:  // NOTE:htt, sst或ldb文件
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:  // NOTE:htt, dbtmp文件
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:  // NOTE:htt, CURRENT文件,需保留
        case kDBLockFile:   // NOTE:htt, LOCK文件,需保留
        case kInfoLogFile:  // NOTE:htt, LOG文件,需保留
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);  // NOTE:htt, 释放 file_number对应的{file, table}缓存
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", int(type), static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);  // NOTE:htt, 删除对应文件
      }
    }
  }
}

Status DBImpl::Recover(
    VersionEdit* edit) {  // NOTE:htt, 从CURRENT读取mainfest,获取该快照所有文件,在从WAL日志中恢复数据->memtable->sst文件
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);  // NOTE:htt, 创建DB目录
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);  // NOTE:htt, 对${dbname}/LOCK 文件进行加锁
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {  // NOTE:htt, 如果${dbname}/CURRENT 文件不存在
    if (options_.create_if_missing) {                 // NOTE:htt, 如果允许missing时创建DB
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {  // NOTE:htt, 如果不允许missing创建DB则报错
      return Status::InvalidArgument(dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {  // NOTE:htt, 如果存在报错则返回出粗
      return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_
          ->Recover();  // NOTE:htt, 从${dbname}/CURRENT获取mainfest文件名,从mainfest读取VersionEdit列表恢复对应Version
  if (s.ok()) {  // NOTE:htt, 从WAL日志中恢复数据
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);  // NOTE:htt, 获得${dbname}/文件列表
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);  // NOTE:htt, 添加level所有Version下的共同存活的文件
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number);  // NOTE:htt, 移除在Version中保存的文件
        if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
          logs.push_back(number);  // NOTE:htt, 保存WAL日志文件number
      }
    }
    if (!expected.empty()) {  // NOTE:htt, 还有不在Version列表中的文件,则报错
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.", static_cast<int>(expected.size()));
      return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());        // NOTE:htt, 对WAL排序,从小到大恢复
    for (size_t i = 0; i < logs.size(); i++) {  // NOTE:htt, 从所有的WAL日志会数据
      s = RecoverLogFile(logs[i], edit,
                         &max_sequence);  // NOTE:htt, 从WAL日志中恢复数据到memtable,再将memtable生成sst文件

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);  // NOTE:htt, 确保next_file_number值比当前的WAL日志的number大
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);  // NOTE:htt, 更新当前最新的seq值
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(
    uint64_t log_number, VersionEdit* edit,
    SequenceNumber* max_sequence) {  // NOTE:htt, 从WAL日志中恢复数据到memtable,再将memtable生成sst文件
  struct LogReporter : public log::Reader::Reporter {  // NOTE:htt, WAL日志恢复异常记录
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s", (this->status == NULL ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);  // NOTE:htt, WAL日志文件名, ${name}/${number}.log
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);  // NOTE:htt, 忽略错误
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu", (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&  // NOTE:htt, 从WAL中读取一条完整日志;该record为一批<key,value>
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);  // NOTE:htt, 重新设置WriteBatch.rep_为contents

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);  // NOTE:htt, 内存table,采用跳表实现
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);  // NOTE:htt, 将WriteBatch中记录逐条写入到MemTable中
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) + WriteBatchInternal::Count(&batch) -
                                    1;  // NOTE:htt, 最后一条插入记录的seq
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;  // NOTE:htt, 获取当前DB中最大seq
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem, edit, NULL);  // NOTE:htt, 如果memtable大于阈值,则直接将memtable写入到sst文件
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem->Unref();  // NOTE:htt,减少memtable引用
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteLevel0Table(mem, edit, NULL);  // NOTE:htt, 将恢复的memtable生成sst文件
                                                 // Reflect errors immediately so that conditions like full
                                                 // file-systems cause the DB::Open() to fail.
  }

  if (mem != NULL) mem->Unref();
  delete file;
  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {  // NOTE:htt, 将memtable写入到sst文件(选择level层时会进行优化)
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);  // NOTE:htt, 保存新的sst文件number
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started", (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter,
                   &meta);  // NOTE:htt, 将iter中的数据内容写入到sst文件中(dbname为前缀),生成元信息到meta
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s", (unsigned long long)meta.number,
      (unsigned long long)meta.file_size, s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);  // NOTE:htt, 文件写入成功,从compact列表中移除

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();  // NOTE:htt, 最小user_key
    const Slice max_user_key = meta.largest.user_key();   // NOTE:htt, 最大user_key
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(
          min_user_key,
          max_user_key);  // NOTE:htt, 选择MemTable可以直接落入层,范围level [0,1,2]这三层文件,目标避免0->1的compact
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);  // NOTE:htt, 添加保存sst文件元信息,包括<level,number,file_size,smallest,largest>
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;  // NOTE:htt, 此次文件写入时间
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);  // NOTE:htt, 保存此次sst文件生成(memtable->sst也算compact)统计
  return s;
}

void DBImpl::
    CompactMemTable() {  // NOTE:htt,
                         // 将immemtable写入sst,并将原有的文件信息写入到mainfest,同时将新文件信息追加到mainfest中
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);  // NOTE:htt, 将memtable写入到sst文件(选择level层时会进行优化)
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {  // NOTE:htt, 如果生成memtable->sst文件时DB关闭了
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(
        logfile_number_);  // Earlier logs no longer needed // NOTE:htt, 设置当前VersionEdit对应的log number
    s = versions_->LogAndApply(
        &edit,
        &mutex_);  // NOTE:htt,
                   // 根据edit和VersionSet生成新的Version,并保存当前的文件信息到mainfest中,并追加新的edit内容到mainfest
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);  // NOTE:htt, imm_ 内存memtable为NULL
    DeleteObsoleteFiles();         // NOTE:htt, 删除无用的文件
  } else {
    RecordBackgroundError(s);  // NOTE:htt, 通知bg线程出错
  }
}

void DBImpl::CompactRange(
    const Slice* begin,
    const Slice* end) {  // NOTE:htt, 先尝试对immutable进行合并,在对level[0,6]之间[begin,end]进行manual合并
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {  // NOTE:htt, 判断[begin,end]相交的文件层
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap // NOTE:htt,
                           // 测试对immutable进行compaction,合并到level0层
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);  // NOTE:htt, 尝试[level,begin,end]进行合并
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {  // NOTE:htt, 尝试[level,begin,end]进行合并
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;  // NOTE:htt, 手动合并
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;  // NOTE:htt, 构建manual.begin
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;    // NOTE:htt, 设置manual_compaction_
      MaybeScheduleCompaction();       // NOTE:htt, 尝试进行后台段合并操作
    } else {                           // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;  // NOTE:htt, 完成合并,则设置manual_compaction_为NULL
  }
}

Status DBImpl::TEST_CompactMemTable() {  // NOTE:htt, 测试对immutable进行compaction,合并到level0层
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);  // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {  // NOTE:htt, 如果有immutable,则等待immutable compact到level0层
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {  // NOTE:htt, 通知bg线程出错
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {  // NOTE:htt, 尝试进行后台段合并操作
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL && manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {  // NOTE:htt, 判断是否需要compact,compaction_score_>=1
                                               // 或file_to_compact_有值则进行
                                               // No work to be done
  } else {
    bg_compaction_scheduled_ = true;        // NOTE:htt, 启动后台线程
    env_->Schedule(&DBImpl::BGWork, this);  // NOTE:htt, 启动一个线程执行后台线程,并进行compact操作
  }
}

void DBImpl::BGWork(void* db) {  // NOTE:htt, 执行后台任务
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::
    BackgroundCall() {  // NOTE:htt,
                        // 后台执行文件的合并,并生成sst文件,最后会生成新的Version,并写入到mainfest,并清理无用文件
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {  // NOTE:htt, 关闭则停止
                                        // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();  // NOTE:htt,
                             // 后台执行文件的合并,并生成sst文件,最后会生成新的Version,并写入到mainfest,并清理无用文件
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();  // NOTE:htt, 再次执行段合并
  bg_cv_.SignalAll();
}

void DBImpl::
    BackgroundCompaction() {  // NOTE:htt,
                              // 后台执行文件的合并,并生成sst文件,最后会生成新的Version,并写入到mainfest,并清理无用文件
  mutex_.AssertHeld();

  if (imm_ != NULL) {   // NOTE:htt, 如immemtable不为null,则执行immemtable的compaction,即刷盘
    CompactMemTable();  // NOTE:htt,
                        // 将immemtable写入sst,并将原有的文件信息写入到mainfest,同时将新文件信息追加到mainfest中
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {  // NOTE:htt, 确认人工合并下待合并的文件列表(level和level+1层)
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(
        m->level, m->begin,
        m->end);  // NOTE:htt, 选择level层和[begin,end]有交集文件列表作为level层待compact文件列表,并确认level+1文件列表
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;  // NOTE:htt, 待合并第一层最大的key
    }
    Log(options_.info_log, "Manual compaction at level-%d from %s .. %s; will stop at %s\n", m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"), (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_
            ->PickCompaction();  // NOTE:htt, 选择待compact文件,并扩容level层文件列表(前提是扩容后总文件之后小于50M)
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {  // NOTE:htt, 小合并,将level层单个文件直接移动到level+1层
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);  // NOTE:htt, level删除对应文件(内存信息)
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);  // NOTE:htt, level+1层加入对应文件
    status = versions_->LogAndApply(
        c->edit(),
        &mutex_);  // NOTE:htt,
                   // 根据edit和VersionSet生成新的Version,并保存当前的文件信息到mainfest中,并追加新的edit内容到mainfest
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n", static_cast<unsigned long long>(f->number),
        c->level() + 1, static_cast<unsigned long long>(f->file_size), status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {                                              // NOTE:htt, 根据选择文件进行合并
    CompactionState* compact = new CompactionState(c);  // NOTE:htt, compaction状态,包括compaction后的文件列表
    status =
        DoCompactionWork(compact);  // NOTE:htt, 执行文件的合并,并生成sst文件,最后会生成新的Version,并写入到mainfest
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);  // NOTE:htt, 清理本次的compact涉及状态信息
    c->ReleaseInputs();
    DeleteObsoleteFiles();  // NOTE:htt, 删除无用的文件
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {  // NOTE:htt, 如果未完成则继续
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;  // NOTE:htt, 临时记录最大值
      m->begin = &m->tmp_storage;   // NOTE:htt, 记录起始合并值
    }
    manual_compaction_ = NULL;  // NOTE:htt, 下一次不再进行人工合并
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {  // NOTE:htt, 清理本次的compact涉及状态信息
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);  // NOTE:htt, 移除outputs文件
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {  // NOTE:htt, 打开待compact的文件
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();  // NOTE:htt, 生成新的文件Number
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);  // NOTE:htt, 添加本次compact需要push back的文件
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);  // NOTE:htt, 表文件名, ${name}/${number}.ldb
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder =
        new TableBuilder(options_, compact->outfile);  // NOTE:htt, 完成整个sstable写入,包括{data block列表, meta block,
                                                       // meta index block, index block, footer} 写入
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {  // NOTE:htt, 将文件写入sst并进行刷盘处理
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;  // NOTE:htt, 文件number
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();  // NOTE:htt, 完成整个sstable写入, 包括{data block列表, meta block, meta index
                                     // block, index block, footer} 写入
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;  // NOTE:htt, 设置文件大小为新产生sst文件大小
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();  // NOTE: htt, 将目录entry信息刷盘，同时将用户态数据刷入内核，内核态数据刷入磁盘
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(), output_number,
                                               current_bytes);  // NOTE:htt, 读取file_number文件,并构建talbe的两层迭代器
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu: %lld keys, %lld bytes", (unsigned long long)output_number,
          (unsigned long long)current_entries, (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(
    CompactionState*
        compact) {  // NOTE:htt, 记录本次待删除以及待添加的文件(compact之后文件),并生成新的Version和保存到mainfest
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes", compact->compaction->num_input_files(0),
      compact->compaction->level(), compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(
      compact->compaction->edit());  // NOTE:htt, 将本次compaction涉及到 level/level+1 层文件添加到待删除列表
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size, out.smallest,
                                         out.largest);  // NOTE:htt, level+1层增加本次compaction文件
  }
  return versions_->LogAndApply(
      compact->compaction->edit(),
      &mutex_);  // NOTE:htt,
                 // 根据edit和VersionSet生成新的Version,并保存当前的文件信息到mainfest中,并追加新的edit内容到mainfest
}

Status DBImpl::DoCompactionWork(
    CompactionState* compact) {  // NOTE:htt, 执行文件的合并,并生成sst文件,最后会生成新的Version,并写入到mainfest
  const uint64_t start_micros = env_->NowMicros();  // NOTE: htt, 获取系统当前时间对应的微秒值
  int64_t imm_micros = 0;                           // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files", compact->compaction->num_input_files(0),
      compact->compaction->level(), compact->compaction->num_input_files(1), compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot =
        versions_->LastSequence();  // NOTE:htt, 空快照情况下,当前最小的snapshot即当前versions.seq
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;  // NOTE:htt, 快照中最小的即为当前最小快照
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(
      compact->compaction);  // NOTE:htt, 构建compaction中涉及两层需要merge文件的合并的迭代器
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() &&
         !shutting_down_
              .Acquire_Load();) {  // NOTE:htt, MegerIterator 指向下一个值,并重新从所有待compact文件中找到值最小值的位置
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {  // NOTE:htt, immutable直接落sst文件
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();  // NOTE:htt,
                            // 将immemtable写入sst,并将原有的文件信息写入到mainfest,同时将新文件信息追加到mainfest中
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);  // NOTE:htt, 记录时间,imm文件落0层sst时间已专门统计
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) && compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);  // NOTE:htt, 将文件写入sst并进行刷盘处理
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key || user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {  // NOTE:htt, 比compact最小的key小则丢弃
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion && ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(
                     ikey.user_key)) {  // NOTE:htt, 更高层没有该记录,并为删除标记,同时比compaction的sequence小,则可以删
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;  // NOTE:htt, 更新last sequence,判断同一个key多个记录保留情况下使用
    }
#if 0
            Log(options_.info_log,
                    "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                    "%d smallest_snapshot: %d",
                    ikey.user_key.ToString().c_str(),
                    (int)ikey.sequence, ikey.type, kTypeValue, drop,
                    compact->compaction->IsBaseLevelForKey(ikey.user_key),
                    (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {  // NOTE:htt, drop的记录则直接丢弃
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);  // NOTE:htt, 生成新待compact的文件
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);  // NOTE:htt, 首次写入值设置为smallest
      }
      compact->current_output()->largest.DecodeFrom(key);  // NOTE:htt, 每次写入的设置为largest
      compact->builder->Add(key, input->value());  // NOTE:htt, 添加<key,value>,因input每次从所有文件选择的最小的key

      // Close output file if it is big enough
      if (compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);  // NOTE:htt, 将文件写入sst并进行刷盘处理
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();  // NOTE:htt, MegerIterator 指向下一个值,并重新找到值最小的位置
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);  // NOTE:htt, 将文件写入sst并进行刷盘处理
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;  // NOTE:htt, compaction统计
  stats.micros = env_->NowMicros() - start_micros -
                 imm_micros;  // NOTE:htt, compaction消耗时间,imm落0层统计已专门处理,此处不重复统计
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {  // NOTE:htt, 总共读取的数据
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {  // NOTE:htt, 总共写入的数据
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);  // NOTE:htt, compact操作设置文件读取和写入算作 level+1层

  if (status.ok()) {
    status = InstallCompactionResults(
        compact);  // NOTE:htt, 记录本次待删除以及待添加的文件(compact之后文件),并生成新的Version和保存到mainfest
  }
  if (!status.ok()) {
    RecordBackgroundError(status);  // NOTE:htt, 通知bg线程出错
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));  // NOTE:htt, 统计所有level层文件个数
  return status;
}

namespace {
struct IterState {  // NOTE:htt, 迭代状态
  port::Mutex* mu;
  Version* version;  // NOTE:htt, 管理当前版本的所有level层文件
  MemTable* mem;     // NOTE:htt, 可写入mem
  MemTable* imm;     // NOTE:htt, 不可写入imm
};

static void CleanupIteratorState(void* arg1,
                                 void* arg2) {  // NOTE:htt, 清理IterState,包括减少mem/version引用,并删除IterState
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();  // NOTE:htt, 减少mem引用
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;  // NOTE:htt, 删除当前IterState
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options, SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {  // NOTE:htt, 生成{mem, imm, level0-6}文件迭代器,并注册清理函数
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();  // NOTE:htt, 当前最新的sequence

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter = NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;  // NOTE:htt, 引用锁
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();  // NOTE:htt, 引用Version
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {  // NOTE:htt, 测试生成{mem, imm, level0-6}文件迭代器
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t
DBImpl::TEST_MaxNextLevelOverlappingBytes() {  // NOTE:htt,
                                               // 获取所有从level1层开始每个文件和下一层文件交集,并获取交集最大的值
  MutexLock l(&mutex_);
  return versions_
      ->MaxNextLevelOverlappingBytes();  // NOTE:htt, 获取所有从level1层开始每个文件和下一层文件交集,并获取交集最大的值
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  /*{{{*/  // NOTE:htt, 读取key对应value,其中先找mem,再找imm,最后从sst找,从sst查找到会尝试触发读合并
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;  // NOTE:htt,获取最新或Options指定的sequece
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();  // NOTE:htt, 获取last sequence
  }

  // fprintf(stderr, "key:%s, snapshot:%llu\n", key.data(), snapshot); // TODO:htt, delete

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();  // NOTE:htt, 增加引用

  bool have_stat_update = false;
  Version::GetStats stats;  // NOTE:htt, 查询key所在文件信息

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(
            lkey, value,
            &s)) {  // NOTE:htt, 如果mem中找到,
                    // 为KeyComparator(InternalKeyComparator(BytewiseComparatorImpl)),先比较user_key(按递增序),如果相等则按seq递减排序:{key1,10,1},{key1,8,1},{key2,11,1},用户查询时seq为最新({key1,20,1},则比{key1,10,1}小,则会返回{key1,10,1})
                    // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value,
                       &stats);  // NOTE:htt, 从levelDB的7层文件来读取数据,先从0层读取,如果未找到则继续1层读取
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update &&
      current->UpdateStats(stats)) {  // NOTE:htt, 根据空读的次数来减少allowed_seeks,如果为0则记录文件为待compaction,
                                      // 即因读引发的compaction
    MaybeScheduleCompaction();  // NOTE:htt, 尝试进行后台段合并操作
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
} /*}}}*/

Iterator* DBImpl::NewIterator(const ReadOptions& options) {  // NOTE:htt, 根据{mem,imm, level0-6}文件迭代器构建DBIter
  SequenceNumber latest_snapshot;
  uint32_t seed;  // NOTE:htt, 局部变量,未赋值,作为随机种子
  Iterator* iter = NewInternalIterator(options, &latest_snapshot,
                                       &seed);  // NOTE:htt, 生成{mem, imm, level0-6}文件迭代器,并注册清理函数;
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_ : latest_snapshot),
      seed);  // NOTE:htt, 构建DBIter
}

void DBImpl::RecordReadSample(Slice key) {  // NOTE:htt, 读采样,定位读match超过2个文件,则可以尝试合并
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();  // NOTE:htt, 尝试合并
  }
}

const Snapshot* DBImpl::GetSnapshot() {  // NOTE:htt,获取最新seq的snapshot,并插入到snapshotlist的列表中
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());  // NOTE:htt,获取最新seq的snapshot,并插入到snapshotlist的列表中
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {  // NOTE:htt, 删除当前快照列表中一个快照
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key,
                   const Slice& val) {  // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
  return DB::Put(o, key, val);          // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
}

Status DBImpl::Delete(const WriteOptions& options,
                      const Slice& key) {  // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options,
                     WriteBatch* my_batch) {  // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;  // NOTE:htt, 默认为false
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();  // NOTE:htt, 如果不在队首,则等待
  }
  if (w.done) {
    return w.status;  // NOTE:htt, 如果已完成则直接返回状态
  }

  // May temporarily unlock and wait.
  Status status =
      MakeRoomForWrite(my_batch == NULL);  // NOTE:htt, 为写腾空间,主要是将memtable转变为imm,然后生成新的mem来支持写
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;               // NOTE:htt, 此时已经是尾部并且未完成
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(
        &last_writer);  // NOTE:htt, 将writers_中多个 writer请求追加一起直到大小超过限制,这样可以写入更多记录
    WriteBatchInternal::SetSequence(
        updates, last_sequence + 1);  // NOTE:htt, 将最新的 sequence 写入到批量请求中,写入时会递增sequence
    last_sequence += WriteBatchInternal::Count(updates);  // NOTE:htt, 更新last_sequence(+写入记录个数)

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));  // NOTE:htt, 将待写记录 追加到 WAL日志
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();  // NOTE:htt, WAL日志刷盘
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {                                         // NOTE:htt, WAL成功写入后，再写入MemTable
        status = WriteBatchInternal::InsertInto(updates, mem_);  // NOTE:htt, 将WriteBatch中记录逐条写入到MemTable中
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();  // NOTE:htt, 清空 tmp_batch_,为下次整合批量提供准备

    versions_->SetLastSequence(last_sequence);  // NOTE:htt, 更新last_sequence值
  }

  while (true) {  // NOTE:htt, 循环取出已经做的writer,并且不是头部writer,则发起信号通知
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;  // NOTE:htt, 如果ready不是w对应,锁名ready已完成
      ready->cv.Signal();
    }
    if (ready == last_writer) break;  // NOTE:htt, 如果本次批量写入的都已处理则break
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();  // NOTE:htt, 通知队列首部记录处理
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(
    Writer** last_writer) {  // NOTE:htt, 将writers_中多个 writer请求追加一起直到大小超过限制,这样可以写入更多记录
  assert(!writers_.empty());
  Writer* first = writers_.front();  // NOTE:htt, 重新取头部记录
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);  // NOTE:htt, 获取批量写字符串大小

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {  // NOTE:htt, 如果批量记录大小 < 128K, 则调整max size = 128K + size
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;                                   // Advance past "first" // NOTE:htt, 跳过first
  for (; iter != writers_.end(); ++iter) {  // NOTE:htt, 当批量记录累积大小未达到max_size(1M)则持续追加记录
    Writer* w = *iter;                      // NOTE:htt, 取一个writer
    if (w->sync && !first->sync) {  // NOTE:htt, 如果当前请求强制刷盘,但是头部请求未强制刷盘则暂停
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {  // NOTE:htt, 如果累积写请求大小超过 max_size,则暂停新的批量记录添加result中
        // Do not make batch too big
        break;
      }

      // Append to *reuslt
      if (result == first->batch) {  // NOTE:htt, 如果result为first->batch,则将记录追加到缓存 tmp_batch_
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);    // NOTE:htt, 首次写入tmp_batch则为空
        WriteBatchInternal::Append(result, first->batch);  // NOTE:htt, 将first->batch记录追加到result中
      }
      WriteBatchInternal::Append(result, w->batch);  // NOTE:htt, 将w->batch中请求记录追加到result中
    }
    *last_writer = w;  // NOTE:htt, 本次合并取出最后的writer
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {  // NOTE:htt, 为写腾空间,主要是将memtable转变为imm,然后生成新的mem来支持写
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {  // NOTE:htt, 0层sst达到8个,减缓写入,会sleep
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);  // NOTE:htt, sleep 1ms
      allow_delay = false;  // Do not delay a single write more than once // NOTE:htt, 单次写入不sleep多次
      mutex_.Lock();
    } else if (!force && (mem_->ApproximateMemoryUsage() <=
                          options_.write_buffer_size)) {  // NOTE:htt, 如果内存小于4M并不强制则完成
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {  // NOTE:htt, 如果已经有immemtable,则说明内存memtable满了,需等待
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {  // NOTE:htt, 如果level0文件>=12,则等待
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number),
                                &lfile);  // NOTE:htt, 生成WAL ${name}/${number}.log
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);  // NOTE:htt, 回退版本号
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;  // NOTE:htt, 新的WAL文件描述符
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);  // NOTE:htt, WAL处理对象
      imm_ = mem_;                    // NOTE:htt, 将immemtable设置为memtable
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);  // NOTE:htt, 新的memtable
      mem_->Ref();                                // NOTE:htt,增加引用
      force = false;                              // Do not force another compaction if have room
      MaybeScheduleCompaction();                  // NOTE:htt, 尝试进行后台段合并操作
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property,
                         std::string* value) {  // NOTE:htt, 获取统计信息，包括compaction统计或leveldb文件列表统计
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();  // NOTE:htt, property为 leveldb.num-files-at-level2
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d", versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;  // NOTE:htt, 直接获取level的值,如2
      return true;
    }
  } else if (in == "stats") {  // NOTE:htt, level.stats 获取文件compaction统计信息或每层文件信息
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {  // NOTE:htt, 如果compaction有统计或对应层文件大于0则落统计信息
        snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level, files,
                 versions_->NumLevelBytes(level) / 1048576.0,  // NOTE:htt, 某level层文件总大小
                 stats_[level].micros / 1e6,                   // NOTE:htt, compaction操作花费时间
                 stats_[level].bytes_read / 1048576.0,         // NOTE:htt, compaction操作读取数据
                 stats_[level].bytes_written / 1048576.0);     // NOTE:htt, compaction操作写入数据
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {                   // NOTE:htt, leveldb.sstables 获取sst文件列表详细信息
    *value = versions_->current()->DebugString();  // NOTE:htt, 打印当前Version下所有level文件的信息
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {  // NOTE:htt, 统计 [start,limit] 之间的sst文件大小近似值, 通过Range*可以统计多个之间的统计
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber,
                   kValueTypeForSeek);  // NOTE:htt, 最小查询intelnalKey{user_key,seq,t}
    InternalKey k2(range[i].limit, kMaxSequenceNumber,
                   kValueTypeForSeek);                       // NOTE:htt, 最小查询intelnalKey{user_key,seq,t}
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);  // NOTE:htt, 查找k1在leveldb所有文件中近似offset
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);  // NOTE:htt, 查找k2在leveldb所有文件中近似offset
    sizes[i] = (limit >= start ? limit - start : 0);  // NOTE:htt, 统计[k1, k2]之间所有sst文件大小之差
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key,
               const Slice& value) {  // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
  WriteBatch batch;
  batch.Put(key, value);      // NOTE:htt, 添加<key,value> 到rep_
  return Write(opt, &batch);  // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {  // NOTE:htt, 取批量writer记录,并先写WAL,然后写Memtable
  WriteBatch batch;
  batch.Delete(key);  // NOTE:htt, 添加删除key 到rep_
  return Write(opt, &batch);
}

DB::~DB() {}

Status DB::Open(
    const Options& options, const std::string& dbname,
    DB**
        dbptr) {  // NOTE:htt,
                  // 打开DB文件,并从CURRENT读取mainfest,然后获取文件信息,并从WAL恢复mem数据;同时产生新的mainfest,并写入CURRENT;最后删除无用文件,并尝试段合并
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();  // NOTE:htt,启动时加锁
  VersionEdit edit;
  Status s = impl->Recover(&edit);  // Handles create_if_missing, error_if_exists // NOTE:htt,
                                    // 从CURRENT读取mainfest,获取该快照所有文件,在从WAL日志中恢复数据->memtable->sst文件
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);  // NOTE:htt, 打开WAL日志, ${dbname}/${number}.log
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);  // NOTE:htt, 记录当前的WAL日志文件number
      impl->logfile_ = lfile;             // NOTE:htt, WAL底层操作文件写入的文件对象
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);  // NOTE:htt, 将记录写入WAL日志中,如果记录大于块长度,则拆分多个部分写入
      s = impl->versions_->LogAndApply(
          &edit,
          &impl->mutex_);  // NOTE:htt,
                           // 根据edit和VersionSet生成新的Version,并保存当前的文件信息到mainfest中,并追加新的edit内容到mainfest
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles();      // NOTE:htt, 删除无用的文件
      impl->MaybeScheduleCompaction();  // NOTE:htt, 尝试进行后台段合并操作
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {}

Status DestroyDB(const std::string& dbname, const Options& options) {  // NOTE:htt, 删除整个DB,慎用
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);  // NOTE: htt, 打开锁文件(文件加锁成功)
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end // NOTE:htt, 删除所有文件
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
