// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {  // NOTE:htt, DB迭代器,可以向前或向后查询
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s, uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_counter_(RandomPeriod()) {}
  virtual ~DBIter() {
    delete iter_;  // NOTE:htt, 是否iter_对象
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  virtual Slice value() const {  // NOTE:htt, 获取value
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {  // NOTE:htt, 保存k到dst中
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {  // NOTE:htt, 清空saved_value_
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Pick next gap with average value of config::kReadBytesPeriod.
  ssize_t RandomPeriod() {  // NOTE:htt, 返回[0, 2M-1]之间随机值
    return rnd_.Uniform(2 * config::kReadBytesPeriod);
  }

  DBImpl* db_;                               // NOTE:htt, DB实现
  const Comparator* const user_comparator_;  // NOTE:htt, user比较器
  Iterator* const iter_;                     // NOTE:htt, {mem, imm, level0-6}文件迭代器,并注册清理函数
  SequenceNumber const sequence_;

  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;      // NOTE:htt, 遍历方向,默认 kForward
  bool valid_;

  Random rnd_;
  ssize_t bytes_counter_;  // NOTE:htt,
                           // [0,2M-1]之间随机值,每次读取<key,value>会减去相应值,如果小于0尝试采样并判断是否需要合并

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {  // NOTE:htt, 解析key
  Slice k = iter_->key();
  ssize_t n = k.size() + iter_->value().size();
  bytes_counter_ -= n;
  while (bytes_counter_ < 0) {
    bytes_counter_ += RandomPeriod();
    db_->RecordReadSample(k);  // NOTE:htt, 读采样,可以定位读match超过2个文件,则可以尝试合并
  }
  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {  // NOTE:htt, 指向下一个元素
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions? // NOTE:htt, 如果是Reverse方向,先调整方向
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
  }

  FindNextUserEntry(true, &saved_key_);  // NOTE:htt, 获取下一个有效user entry,会将valid_设置有效
}

void DBIter::FindNextUserEntry(bool skipping,
                               std::string* skip) {  // NOTE:htt, 获取下一个有效user entry,会将valid_设置有效
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);  // NOTE:htt, 保存删除的记录
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {  // NOTE:htt, ikey比skip小或等于,则忽略
            // Entry hidden
          } else {
            valid_ = true;  // NOTE:htt, 设置有效
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;  // NOTE:htt, 删除或比skip小则忽略
}

void DBIter::Prev() {  // NOTE:htt, 向前查找user key
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions? // NOTE:htt, 如果当前是kForward方向,则prev查找需要调整方向
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();  // NOTE:htt, 清空saved_value_
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) < 0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();  // NOTE:htt, 向前查找user key,并将valid_设置有效
}

void DBIter::FindPrevUserEntry() {  // NOTE:htt, 向前查找user key,并将valid_设置有效
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) && user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();  // NOTE:htt, 一直尝试获取没有删除记录
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);       // NOTE:htt, 保存user key
          saved_value_.assign(raw_value.data(), raw_value.size());  // NOTE:htt, 保存value
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {  // NOTE:htt, 如果标记为删除,则说明倒序已读取完,当前调整方向正向查询
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;  // NOTE:htt, 当前key有效
  }
}

void DBIter::Seek(const Slice& target) {  // NOTE:htt, 查找target
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(&saved_key_,
                    ParsedInternalKey(target, sequence_, kValueTypeForSeek));  // NOTE:htt, 构建internal key
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false,
                      &saved_key_ /* temporary storage */);  // NOTE:htt, 获取下一个有效user entry,会将valid_设置有效
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {  // NOTE:htt, 从头部查询开始
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false,
                      &saved_key_ /* temporary storage */);  // NOTE:htt, 获取下一个有效user entry,会将valid_设置有效
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {  // NOTE:htt, 从尾部开始查找
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();  // NOTE:htt, 向前查找user key,并将valid_设置有效
}

}  // anonymous namespace

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator, Iterator* internal_iter,
                        SequenceNumber sequence, uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);  // NOTE:htt, 构建DBIter
}

}  // namespace leveldb
