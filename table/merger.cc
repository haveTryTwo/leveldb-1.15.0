// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator { // NOTE:htt, 处理一组iterator,然后进行查找, 向下一个next()或前一个prev()推进
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(NULL),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]); // NOTE:htt, 保存children[]到对应iterator
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != NULL);
  }

  virtual void SeekToFirst() { // NOTE:htt, 一组iterator都调整指向头部
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest(); // NOTE:htt, 找到最小的key对应的iterator
    direction_ = kForward;
  }

  virtual void SeekToLast() { // NOTE: htt, 一组iterator都调整指向尾部
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest(); // NOTE:htt, 找到最大的key对应的iterator
    direction_ = kReverse; // NOTE:htt, 反向
  }

  virtual void Seek(const Slice& target) { // NOTE: htt, 一组iterator中都查找对应key
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest(); // NOTE:htt, 找到最小的key对应的iterator
    direction_ = kForward; // NOTE:htt, 正常查找后,方向会设置为向前
  }

  virtual void Next() { // NOTE:htt, 指向下一个值,并重新找到值最小的位置
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) { // NOTE:htt, next需要向前,如果不是该方向,找到对应key指向next,并调整方向
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next(); // NOTE:htt,找到key之后，指向下一个key
          }
        }
      }
      direction_ = kForward; // NOTE:htt,调整方向为next
    }

    current_->Next(); // NOTE:htt, 将current指向next值
    FindSmallest(); // NOTE:htt, 重新从iterator列表中找到当前新的最小的iter
  }

  virtual void Prev() { // NOTE:htt, 将迭代器指向前一个值,并重新找到当前最大的位置
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) { // NOTE:htt, 如果不是反方向,调整查询方向为reverse
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key()); // NOTE:htt, 找到当前的key
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse; // NOTE:htt, 调整查询方向为reverse
    }

    current_->Prev(); // NOTE:htt, current向前调整
    FindLargest(); // NOTE:htt, 找到当前最大值位置
  }

  virtual Slice key() const { // NOTE:htt, 获取iterator中key
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const { // NOTE:htt, 获取当前的值
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const { // NOTE:htt, 找到非ok的情况
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) { // NOTE:htt, 找到非ok的情况
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_; // NOTE:htt, 比较器
  IteratorWrapper* children_; // NOTE:htt, children迭代器
  int n_;
  IteratorWrapper* current_; // NOTE:htt, 当前迭代器列表中使用的具体哪个

  // Which direction is the iterator moving?
  enum Direction {
    kForward, // NOTE:htt, 正向
    kReverse // NOTE:htt, 反向
  };
  Direction direction_; // NOTE:htt, iterator迭代方向
};

void MergingIterator::FindSmallest() { // NOTE:htt, 找到所有IteratorWrapper中最小的key对应的iterator
  IteratorWrapper* smallest = NULL;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == NULL) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) { // NOTE:htt, 判断iteratorWrapper中缓存key大小
        smallest = child;
      }
    }
  }
  current_ = smallest; // NOTE:htt, current选择最小的
}

void MergingIterator::FindLargest() { // NOTE:htt, 找到所有IteratorWrapper中最大的key对应的iterator
  IteratorWrapper* largest = NULL;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == NULL) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {// NOTE:htt, 判断iteratorWrapper中缓存key大小
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) { // NOTE:htt, 构建多个iterator的合并处理
  assert(n >= 0);
  if (n == 0) { // NOTE:htt, 个数为0,则返回空迭代器
    return NewEmptyIterator();
  } else if (n == 1) { // NOTE:htt, 个数为1,返回第一个,简化处理
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n); // NOTE:htt, 个数为多个,返回对应iterator的合并处理
  }
}

}  // namespace leveldb
