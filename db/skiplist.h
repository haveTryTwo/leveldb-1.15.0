// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;  // NOTE:htt, 提供内存分配机制,如果小内存则先缓存一部分,后续申请直接从缓存取

template <typename Key, class Comparator>
class SkipList {  // NOTE:htt, 跳表
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {  // NOTE:htt, 跳表的迭代器
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;  // NOTE:htt, 跳表
    Node* node_;            // NOTE:htt, 当前节点
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };  // NOTE:htt, 最高12层

  // Immutable after construction
  Comparator const
      compare_;  // NOTE:htt,key大小比对,为KeyComparator(InternalKeyComparator(BytewiseComparatorImpl)),先比较user_key(按递增序),如果相等则按seq递减排序:{key1,10,1},{key1,8,1},{key2,11,1},用户查询时seq为最新({key1,20,1},则比{key1,10,1}小,则会返回{key1,10,1})
  Arena* const arena_;  // Arena used for allocations of nodes

  Node* const head_;  // NOTE:htt, 头结点<key=0, height=12>

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;  // Height of the entire list // NOTE:htt, 当前最大的层高,初始化为1

  inline int GetMaxHeight() const {  // NOTE:htt, 获取最高层高
    return static_cast<int>(reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  Random rnd_;  // NOTE: htt, 随机值生成器, 注意顶多 2^31 次会轮回(算法导致)

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {  // NOTE:htt, 跳表中节点
  explicit Node(const Key& k) : key(k) {}

  Key const key;  // NOTE:htt, 存储的key

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {  // NOTE:htt, 带内存屏障获取当前节点第n层的下一个节点
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  void SetNext(int n, Node* x) {  // NOTE:htt, 带内存屏障设置当前节点第n层指向x节点
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);  // NOTE:htt, 带内存屏障设置 <n, x> 即当前节点第n层指向x节点
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {  // NOTE:htt, 不带内存屏障方式获取当前节点第n层的下一个节点
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {  // NOTE:htt, 不带内存屏障设置当前节点第n层指向x节点
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  port::AtomicPointer next_[1];  // NOTE:htt, 当前节点的层高指针数组,第0层为底层链接
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {  // NOTE:htt, 分配包含key的Node,并且层高为height
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));  // NOTE:htt, 分配sizeof(Node)+(n-1)*指针数组空间
  return new (mem) Node(key);                                      // NOTE:htt, 本地分配
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {  // NOTE:htt, skiplist iterator初始化
  list_ = list;
  node_ = NULL;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {  // NOTE:htt, 当前节点是否有效
  return node_ != NULL;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {  // NOTE:htt, 获取节点key
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {  // NOTE:htt, 下一个节点
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {  // NOTE:htt, 获取前一个节点
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);  // NOTE:htt, 查找比key小的node
  if (node_ == list_->head_) {              // NOTE:htt, 已到头部
    node_ = NULL;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {  // NOTE:htt, 查找和key相等或大于的node
  node_ = list_->FindGreaterOrEqual(target, NULL);  // NOTE:htt, 查找和key相等或大于的node
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {  // NOTE:htt, 指向跳表中第一个node
  node_ = list_->head_->Next(0);                                  // NOTE:htt, 指向第一个node
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {  // NOTE:htt, 指向跳表中最后一个node
  node_ = list_->FindLast();                                     // NOTE:htt, 找到最后一个node
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {  // NOTE:htt, 获取随机height
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {  // NOTE:htt, 1/4概率会增加height
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key,
                                               Node* n) const {  // NOTE:htt, 判断n->key是否比key小
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindGreaterOrEqual(
    const Key& key, Node** prev) const {  // NOTE:htt, 查找和当前key大或相等的节点 /*{{{*/
  Node* x = head_;
  int level = GetMaxHeight() - 1;  // NOTE:htt, 获取当前跳表的最高height
  while (true) {
    Node* next = x->Next(level);

    //    if (next != NULL) {/*{{{*/
    //        uint32_t len;
    //        const char* p = next->key;
    //        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted //
    //        NOTE:htt, 获取跳表中key的长度 std::string next_key(p, len); // NOTE:htt, 获取key
    //
    //        fprintf(stderr, "next key:");
    //        for (size_t i = 0; i < len; ++i) {
    //            fprintf(stderr, "%02x", (uint8_t)p[i]);
    //        }
    //
    //        fprintf(stderr, "  key:");
    //        p = key;
    //        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted //
    //        NOTE:htt, 获取key的长度 for (size_t i = 0; i < len; ++i) {
    //            fprintf(stderr, "%02x", (uint8_t)p[i]);
    //        }
    //        fprintf(stderr, "\n");
    //    }/*}}}*/
    if (KeyIsAfterNode(key, next)) {  // NOTE:htt, key在node之后
      // Keep searching in this list
      x = next;
    } else {  // NOTE:htt, key在node之前
      if (prev != NULL) prev[level] = x;
      if (level == 0) {  // NOTE:htt, 到底
        return next;     // NOTE:htt, 返回下一个node,和key相等或大于key
      } else {
        // Switch to next list
        level--;
      }
    }
  }
} /*}}}*/

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLessThan(
    const Key& key) const {  // NOTE:htt, 找比key小的node /*{{{*/
  Node* x = head_;
  int level = GetMaxHeight() - 1;  // NOTE:htt, 获取当前跳表的最高height
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {  // NOTE:htt, key在next node前
      if (level == 0) {                                   // NOTE:htt, 到底
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {  // NOTE:htt, key在next node之后
      x = next;
    }
  }
} /*}}}*/

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {  // NOTE:htt, 查找最后一个node /*{{{*/
  Node* x = head_;
  int level = GetMaxHeight() - 1;  // NOTE:htt, 获取当前跳表的最高height
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {  // NOTE:htt, 到底
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
} /*}}}*/

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),  // NOTE:htt, 头结点<key=0, height=12>
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) { /*{{{*/
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);  // NOTE:htt, 头节点每层的指向为NULL
  }
} /*}}}*/

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {  // NOTE:htt, 查询新的key /*{{{*/
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);  // NOTE:htt, 查找和当前key相同或大于的node

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));  // NOTE:htt, 不允许相同key插入

  int height = RandomHeight();  // NOTE:htt, 获取随机height
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;  // NOTE:htt, 调整[current max_height_, height]层的指针为 head_
    }
    // fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));  // NOTE:htt, 设置新的层高
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));  // NOTE:htt, 调整新node的i层指向prev[i]节点i的对应节点
    prev[i]->SetNext(i, x);                               // NOTE:htt, prev[i]节点指向新节点
  }
} /*}}}*/

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {  // NOTE:htt, 判断是否包含key /*{{{*/
  Node* x = FindGreaterOrEqual(key, NULL);
  if (x != NULL && Equal(key, x->key)) {  // NOTE:htt, 相等则返回true
    return true;
  } else {
    return false;
  }
} /*}}}*/

}  // namespace leveldb
