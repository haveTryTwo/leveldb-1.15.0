// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/iterator.h"

namespace leveldb {

Iterator::Iterator() {
  cleanup_.function = NULL; // NOTE: htt, cleanup_.参数设置为null，其中 arg1和arg2由于和function绑定未进行赋值(推荐赋值)
  cleanup_.next = NULL;
}

Iterator::~Iterator() {
  if (cleanup_.function != NULL) {
    (*cleanup_.function)(cleanup_.arg1, cleanup_.arg2); // NOTE: htt, 析构时执行 cleanup
    for (Cleanup* c = cleanup_.next; c != NULL; ) { // NOTE: htt, 遍历 cleanup.next，并执行清理
      (*c->function)(c->arg1, c->arg2);
      Cleanup* next = c->next;
      delete c; // NOTE: htt, 非首部的Cleanup为new对象，所以需要释放
      c = next;
    }
  }
}

void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) { // NOTE: htt, 注册新的 <func,arg1,arg2> Cleanup处理
  assert(func != NULL);
  Cleanup* c;
  if (cleanup_.function == NULL) {
    c = &cleanup_; // NOTE: htt, 如果cleanup_.function为空，则直接将<func,arg1,arg2>赋值到cleanup中
  } else {
    c = new Cleanup; // NOTE: htt, 创建新的Cleanup对象，并插入到 cleanup_.next 
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

namespace {
class EmptyIterator : public Iterator { // NOTE: htt, 空的Iterator, Valid()等操作为false
 public:
  EmptyIterator(const Status& s) : status_(s) { }
  virtual bool Valid() const { return false; }
  virtual void Seek(const Slice& target) { }
  virtual void SeekToFirst() { }
  virtual void SeekToLast() { }
  virtual void Next() { assert(false); }
  virtual void Prev() { assert(false); }
  Slice key() const { assert(false); return Slice(); }
  Slice value() const { assert(false); return Slice(); }
  virtual Status status() const { return status_; } // NOTE: htt, 仅status()函数有效
 private:
  Status status_;
};
}  // namespace

Iterator* NewEmptyIterator() { // NOTE: htt, 创建空的正常状态Iterator
  return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) { // NOTE: htt, 创建空的异常状态的Iterator
  return new EmptyIterator(status);
}

}  // namespace leveldb
