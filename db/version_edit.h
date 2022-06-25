// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData { // NOTE:htt, 文件元信息
  int refs; // NOTE:htt, 引用次数, 如果为0则说明没有引用可以删除
  int allowed_seeks;          // Seeks allowed until compaction // NOTE:htt, 运行无效查询次数,用于读触发compaction
  uint64_t number;  // NOTE:htt, file对应的数字
  uint64_t file_size;         // File size in bytes // NOTE:htt, 文件大小
  InternalKey smallest;       // Smallest internal key served by table // NOTE:htt, 内部key, {user_key, seq, t}三者组合
  InternalKey largest;        // Largest internal key served by table // NOTE:htt, 内部key, {user_key, seq, t}三者组合

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

class VersionEdit { // NOTE:htt, 中间状态,记录compaction后增加以及删除文件,并记录下一次compaction的起始位置
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) { // NOTE:htt, 设置比较器名称
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) { // NOTE:htt, 设置日志number
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) { // NOTE:htt, 设置前一个日志number
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) { // NOTE:htt, 设置下一个文件number
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) { // NOTE:htt, 设置最新的序号
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) { // NOTE:htt, 添加某一层文件元信息
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f)); // NOTE:htt, 添加某一层文件元信息{level, FileMetaData}
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file)); // NOTE:htt, 删除文件列表添加删除文件信息{level, file_number}
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  std::string comparator_; // NOTE:htt, 比较器名称
  uint64_t log_number_; // NOTE:htt, 日志number信息,日志文件和Memtable是一一对应
  uint64_t prev_log_number_; // NOTE:htt, 前一个log number
  uint64_t next_file_number_; // NOTE:htt, 下一个文件 number
  SequenceNumber last_sequence_; // NOTE:htt, 下一个sequence,每次写入都会递增
  bool has_comparator_;
  bool has_log_number_; // NOTE:htt, 是否有日志number
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_; // NOTE:htt, 记录每个层级{level, {key, seq, t}}下一次compaction位置
  DeletedFileSet deleted_files_; // NOTE:htt, 每个level compaction之后删除文件列表，对应{level, file_number}
  std::vector< std::pair<int, FileMetaData> > new_files_; // NOTE:htt, 每个层级compaction之后新增文件元信息{level, FileMetaData}
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
