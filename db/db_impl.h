// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options, const Slice& key, std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;  // NOTE:htt, compaction状态,包括compaction后的文件列表
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&, SequenceNumber* latest_snapshot, uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;  // NOTE:htt, internal key比较器
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;     // options_.comparator == &internal_comparator_
  bool owns_info_log_;        // NOTE:htt, 是否启动了 info log,打印日常日志
  bool owns_cache_;           // NOTE:htt, 是否启动了 block cache
  const std::string dbname_;  // NOTE:htt, db名称

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;  // NOTE:htt, sst文件cache

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;  // NOTE:htt, 文件锁 ${dbname}/LOCK

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  MemTable* mem_;                // NOTE:htt, 可变内存
  MemTable* imm_;                // NOTE:htt, 不可变内存          // Memtable being compacted
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  WritableFile* logfile_;        // NOTE:htt, WAL底层操作文件写入的文件对象
  uint64_t logfile_number_;      // NOTE:htt, WAL日志文件number
  log::Writer* log_;  // NOTE:htt, 将记录写入WAL日志中,如果记录大于块长度,则拆分多个部分写入
  uint32_t seed_;     // For sampling. // NOTE:htt, rand种子

  // Queue of writers.
  std::deque<Writer*> writers_;  // NOTE:htt, 一批写入请求
  WriteBatch* tmp_batch_;        // NOTE:htt, 批量写对象

  SnapshotList snapshots_;  // NOTE:htt, snaphost链表,将snapshot串联起来

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;  // NOTE:htt, 当前正在compactions 文件number

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;  // NOTE:htt, 当前正在进行段合并

  // Information for a manual compaction
  struct ManualCompaction {  // NOTE:htt, 手动compaction
    int level;               // NOTE:htt, compact的层
    bool done;
    const InternalKey* begin;  // NULL means beginning of key range
    const InternalKey* end;    // NULL means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_;  // NOTE:htt, 手动段合并

  VersionSet* versions_;  // NOTE:htt, 当前使用的VersionSet

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;  // NOTE:htt, 后台运行状态

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {  // NOTE:htt, compaction统计
    int64_t micros;         // NOTE:htt, 毫秒
    int64_t bytes_read;     // NOTE:htt, 读取字节数
    int64_t bytes_written;  // NOTE:htt, 写入字节数

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  CompactionStats stats_[config::kNumLevels];  // NOTE:htt, 每层的compaction统计

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {  // NOTE:htt, 获取InternalKey中 user key的比较器
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db, const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy, const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
