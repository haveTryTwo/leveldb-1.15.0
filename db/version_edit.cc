// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {              // NOTE:htt, 序列化使用的标签，和protobuf类似
  kComparator = 1,      // NOTE:htt, comparator标签
  kLogNumber = 2,       // NOTE:htt, log number标签
  kNextFileNumber = 3,  // NOTE:htt, next file number标签
  kLastSequence = 4,    // NOTE:htt, last sequence标签
  kCompactPointer = 5,  // NOTE:htt, compact pointer标签
  kDeletedFile = 6,     // NOTE:htt, delete file 标签
  kNewFile = 7,         // NOTE:htt, new file标签
  // 8 was used for large value refs
  kPrevLogNumber = 9  // NOTE:htt, prev log number标签
};

void VersionEdit::Clear() {  // NOTE:htt, 清空操作
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {  // NOTE:htt, 将VersionEdit内容序列化
  if (has_comparator_) {
    PutVarint32(dst, kComparator);             // NOTE:htt, 添加kComparator标签
    PutLengthPrefixedSlice(dst, comparator_);  // NOTE:htt, 添加comparator_名称
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);  // NOTE:htt, 添加kLogNumber标签
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);  // NOTE:htt, 添加kPrevLogNumber标签
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);  // NOTE:htt, 添加kNextFileNumber标签
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);  // NOTE:htt, 添加kLastSequence标签
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {  // NOTE:htt, compact指针列表序列化
    PutVarint32(dst, kCompactPointer);             // NOTE:htt, 每个compact_pointer会添加 kCompactPointer标签
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (DeletedFileSet::const_iterator iter = deleted_files_.begin(); iter != deleted_files_.end();
       ++iter) {                     // NOTE:htt, 删除文件列表序列化
    PutVarint32(dst, kDeletedFile);  // NOTE:htt, 每个delete file会添加kDeletedFile标签
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // file number
  }

  for (size_t i = 0; i < new_files_.size(); i++) {  // NOTE:htt, new file列表序列化
    const FileMetaData& f = new_files_[i].second;
    PutVarint32(dst, kNewFile);             // NOTE:htt, 每个new file都会添加 kNewFile标签
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {  // NOTE:htt, 从input中获取 InternalKey
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {  // NOTE:htt, 从input中获取level
  uint32_t v;
  if (GetVarint32(input, &v) && v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {  // NOTE:htt, 从src中解析出 VersionEdit内容
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == NULL && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {  // NOTE:htt, 解析比较器
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {  // NOTE:htt, 解析log number
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {  // NOTE:htt, 解析prev log number
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {  // NOTE:htt, 解析next file number
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {  // NOTE:htt, 解析last sequence
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));  // NOTE:htt, 解析compact_pointers_
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));  // NOTE:htt, 解析 deleted_files_
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &f.number) && GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) && GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(std::make_pair(level, f));  // NOTE:htt, 解析new files
        } else {
          msg = "new-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {  // NOTE:htt, msg 不为NULL,则说明有异常
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {  // NOTE:htt, 打印 VersionEdit Debug信息
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin(); iter != deleted_files_.end(); ++iter) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
