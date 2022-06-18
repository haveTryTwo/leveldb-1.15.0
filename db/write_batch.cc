// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() { // NOTE:htt, 清空rep
  rep_.clear();
  rep_.resize(kHeader); // NOTE:htt, 设置头部长度: 序号+count
}

Status WriteBatch::Iterate(Handler* handler) const { // NOTE:htt, 遍历rep中的<key,value>逐条写入Memtable,根据操作类型Put()或Delete()
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader); // NOTE:htt, input跳过头部
  Slice key, value;
  int found = 0;
  while (!input.empty()) { // NOTE:htt, 遍历input字符串中 <key,value>
    found++;
    char tag = input[0]; // NOTE:htt, 操作类型
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue: // NOTE;htt,写入类型则插入<key,value>
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value); // NOTE:htt, 执行写入
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion: // NOTE;htt, 删除类型则删除数据
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key); // NOTE:htt, 执行删除
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) { // NOTE:htt, 判断遍历得到的统计和缓存的中统计是否一致
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) { // NOTE:htt, 获取rep缓存的记录个数
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) { // NOTE:htt, 设置rep的缓存的记录个数
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) { // NOTE:htt, 获取rep缓存的sequence number
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) { // NOTE:htt, 设置rep缓存的sequence number
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) { // NOTE:htt, 添加<key,value> 到rep_
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1); // NOTE:htt, 增加记录count
  rep_.push_back(static_cast<char>(kTypeValue)); // NOTE:htt, 添加增加的类型
  PutLengthPrefixedSlice(&rep_, key); // NOTE:htt,保存长度+字符串(key),其中长度采用可变整数
  PutLengthPrefixedSlice(&rep_, value);// NOTE:htt,保存长度+字符串(value),其中长度采用可变整数
}

void WriteBatch::Delete(const Slice& key) { // NOTE:htt, 添加删除key 到rep_
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1); // NOTE:htt, 增加记录count
  rep_.push_back(static_cast<char>(kTypeDeletion)); // NOTE:htt, 设置删除类型
  PutLengthPrefixedSlice(&rep_, key); // NOTE:htt,保存长度+字符串(key),其中长度采用可变整数
}

namespace {
class MemTableInserter : public WriteBatch::Handler { // NOTE:htt, 内存Memtable写入
 public:
  SequenceNumber sequence_; // NOTE:htt, 当前seq值,每插入记录都会递增
  MemTable* mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value); // NOTE:htt, 跳表中插入{{key,seq,t}, value}
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice()); // NOTE:htt, 跳表中插入删除{{key,seq,t}, {}}
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) { // NOTE:htt, 将WriteBatch中记录逐条写入到MemTable中
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b); // NOTE:htt, 获取rep缓存的sequence number
  inserter.mem_ = memtable;
  return b->Iterate(&inserter); // NOTE:htt, 将WriteBatch中值写入到Memtable中,写入时每条记录会增加seq
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) { // NOTE:htt, 重新设置WriteBatch.rep_为contents,格式为 ${seq}${count}[{${type}${key}${value}]...
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) { // NOTE:htt, 将src记录追加到dst中
  SetCount(dst, Count(dst) + Count(src)); // NOTE:htt, 设置记录条数为 count(dst)+count(src)
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader); // NOTE:htt, 将src记录追加到dst中
}

}  // namespace leveldb
