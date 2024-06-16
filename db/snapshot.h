// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "leveldb/db.h"

namespace leveldb {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
class SnapshotImpl : public Snapshot {  // NOTE:htt, 具体的快照,保存SequenceNumber,并通过next_/prev_将对象串联起来
 public:
  SequenceNumber number_;  // const after creation // NOTE:htt, 当前seqNumber

 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list
  SnapshotImpl* prev_;  // NOTE:htt, prev快照
  SnapshotImpl* next_;  // NOTE:htt, next快照

  SnapshotList* list_;  // just for sanity checks // NOTE:htt, 保存当前的snapshotList对象,用于验证
};

class SnapshotList {  // NOTE:htt, snaphost链表,将snapshot串联起来
 public:
  SnapshotList() {  // NOTE:htt, 快照初始化指向自己
    list_.prev_ = &list_;
    list_.next_ = &list_;
  }

  bool empty() const { return list_.next_ == &list_; }
  SnapshotImpl* oldest() const {
    assert(!empty());
    return list_.next_;
  }
  SnapshotImpl* newest() const {
    assert(!empty());
    return list_.prev_;
  }

  const SnapshotImpl* New(SequenceNumber seq) {  // NOTE:htt,插入新节点,其中new node的为list.prev_,原有新节点的next_
    SnapshotImpl* s = new SnapshotImpl;
    s->number_ = seq;
    s->list_ = this;  // NOTE:htt, 保存SnapshotList对象到当前的snaphost中
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    return s;
  }

  void Delete(const SnapshotImpl* s) {  // NOTE:htt, 删除当前快照列表中一个快照
    assert(s->list_ == this);           // NOTE:htt, 必须是当前的snapshotList中的快照,才允许删除
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    delete s;
  }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl list_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SNAPSHOT_H_
