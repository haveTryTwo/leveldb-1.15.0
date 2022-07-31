// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) { // NOTE:htt, 获取value
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted // NOTE:htt, 获取value长度
  return Slice(p, len); // NOTE:htt, 获取value
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp), // NOTE:htt, comparator_为KeyComparator(InternalKeyComparator(BytewiseComparatorImpl))
      refs_(0),
      table_(comparator_, &arena_) {
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); } // NOTE:htt, 分配的近似内容

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const { // NOTE:htt, 先获取key,再进行大小比对
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr); // NOTE:htt, 获取aptr对应key
  Slice b = GetLengthPrefixedSlice(bptr); // NOTE:htt, 获取bptr对应key
  return comparator.Compare(a, b); // NOTE:htt, key比较
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) { // NOTE:htt, 编码 key
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator { // NOTE:htt, memTable迭代器
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); } // NOTE:htt, 查找编码后的key
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); } // NOTE:htt, 获取key
  virtual Slice value() const { // NOTE:htt, 获取value
    Slice key_slice = GetLengthPrefixedSlice(iter_.key()); // NOTE:htt, 获取key
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size()); // NOTE:htt,获取value
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_; // NOTE:htt, 跳表迭代器
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() { // NOTE:htt, 构建内存memTable迭代器
  return new MemTableIterator(&table_);
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) { // NOTE:htt, 跳表中插入{{key,seq,t}, value}
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8; // NOTE:htt, 编码采用{key, seq, t}做为内部key
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size; // NOTE:htt, 获取<key,value>组装字符串编码长度
  char* buf = arena_.Allocate(encoded_len); // NOTE:htt, 分配内存
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size); // NOTE:htt, 赋值 user_key
  p += key_size;
  EncodeFixed64(p, (s << 8) | type); // NOTE:htt, 追加 {seq, type}组合值
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size); // NOTE:htt, 追加 value
  assert((p + val_size) - buf == encoded_len);
  table_.Insert(buf); // NOTE:htt, 跳表中写入{key, value}
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) { // NOTE:htt, 查找{user_key, seq, type}组合key
  Slice memkey = key.memtable_key(); // NOTE:htt, mem对应key
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data()); // NOTE:htt, 跳表中查找key
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key(); // NOTE:htt, entry格式  ${key_len}${user_key, seq, type}
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) { // NOTE:htt, 仅比较 user_key
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8); // NOTE:htt, 获取{seq, type}组合
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: { // NOTE:htt, 写入情况返回数据
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length); // NOTE:htt, 获取value的值
          value->assign(v.data(), v.size()); // NOTE:htt, 获取value
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice()); // NOTE:htt, 删除情况返回未找到
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
