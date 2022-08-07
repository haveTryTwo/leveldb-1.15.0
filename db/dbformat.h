// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_FORMAT_H_
#define STORAGE_LEVELDB_DB_FORMAT_H_

#include <stdio.h>
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7; // NOTE:htt, sstable采用7层

// Level-0 compaction is started when we hit this many files.
static const int kL0_CompactionTrigger = 4; // NOTE:htt, level0层有4个文件即触发compaction

// Soft limit on number of level-0 files.  We slow down writes at this point.
static const int kL0_SlowdownWritesTrigger = 8; // NOTE:htt, level0层达到8个会减缓数据写入

// Maximum number of level-0 files.  We stop writes at this point.
static const int kL0_StopWritesTrigger = 12; // NOTE:htt, level0层达到12文件会停写

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576; // NOTE:htt, 1M

}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum ValueType { // NOTE:htt, 记录类型: 0为删除, 1为新增
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue; // NOTE:htt, 查询序号时构建ParsedInternalKey采用valueType为1

typedef uint64_t SequenceNumber; // NOTE:htt,序号类型为uint64_t

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const SequenceNumber kMaxSequenceNumber =
    ((0x1ull << 56) - 1); // NOTE:htt, 序号值最大为2^56-1,这样底部8位给type(0为删除,1为新增)使用,这样即可以使用64位存储

struct ParsedInternalKey { // NOTE:htt, 解析的内部key, 即{user_key, sequence, type}
  Slice user_key; // NOTE:htt, 用户提供key
  SequenceNumber sequence; // NOTE:htt, 序号
  ValueType type; // NOTE:htt, 写入类型(0:删除,1:新增)

  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) { // NOTE:htt, 内部key编码长度: user_key.size()+8
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result);

// Returns the user key portion of an internal key.
inline Slice ExtractUserKey(const Slice& internal_key) { // NOTE:htt, 从内部key中取出用户key
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8); // NOTE:htt, 从内部key中取出用户key, 末尾8个字节为{seq,t}组合
}

inline ValueType ExtractValueType(const Slice& internal_key) { // NOTE:htt, 从内部key获取写入类型
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8); // NOTE:htt, 末尾8字节为 {seq,t}组合
  unsigned char c = num & 0xff; // NOTE:htt, 取最后8位为type
  return static_cast<ValueType>(c);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator { // NOTE:htt, 内部key采用特殊的比较器comparator
 private:
  const Comparator* user_comparator_; // NOTE:htt, 比较用户 key
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) { }
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy { // NOTE:htt, 内部过滤策略
 private:
  const FilterPolicy* const user_policy_; // NOTE:htt, 过滤器策略
 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) { }
  virtual const char* Name() const;
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const;
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey { // NOTE:htt, 内部key, {user_key, seq, t}三者组合
 private:
  std::string rep_; // NOTE:htt, 内部key, {user_key, seq, t} 三者组合,其中 {seq, t}合并到一个uint64中
 public:
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t)); // NOTE:htt,采用{key, seq, t}构建内部key
  }

  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); } // NOTE:htt, slice已经为{key,seq,t},直接设置
  Slice Encode() const { // NOTE:htt, 内部key的encode, 即组合后的二进制
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); } // NOTE:htt,从内部key中获取 user_key

  void SetFrom(const ParsedInternalKey& p) { // NOTE:htt, 根据p重新生成 rep_
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

inline int InternalKeyComparator::Compare(
    const InternalKey& a, const InternalKey& b) const { // NOTE:htt, 内部key比较
  return Compare(a.Encode(), b.Encode());
}

inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) { // NOTE:htt, 从内部key解析出{user_key, seq, t}
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8); // NOTE:htt, 获取{seq, t}组合
  unsigned char c = num & 0xff; // NOTE:htt, 获取type
  result->sequence = num >> 8; // NOTE:htt, 获取seq
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8); // NOTE:htt, 获取用户key
  return (c <= static_cast<unsigned char>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
class LookupKey { // NOTE:htt, 通过{user_key, seq, ValueType} 构建 LookupKey
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const { return Slice(start_, end_ - start_); } // NOTE:htt, memtable使用key,即 ${key_len}{user_key,seq,t}

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); } // NOTE:htt, 内部key, 即{user_key, seq, t}组合

  // Return the user key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); } // NOTE:htt, 获取user_key

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_; // NOTE:htt, 如上, 字符串起始
  const char* kstart_; // NOTE:htt, 如上, userkey起始
  const char* end_; // NOTE:htt, tag结束位置
  char space_[200];      // Avoid allocation for short keys

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_; // NOTE:htt, 如果start_为堆空间,则释放
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_FORMAT_H_
