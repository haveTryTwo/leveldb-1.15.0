// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static const int kTargetFileSize = 2 * 1048576; // NOTE:htt, 目标文件大小2M

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static const int64_t kMaxGrandParentOverlapBytes = 10 * kTargetFileSize; // NOTE:htt, 20M, 更上一层文件重叠的大小

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static const int64_t kExpandedCompactionByteSizeLimit = 25 * kTargetFileSize; // NOTE:htt, compact 扩展文件时最大文件为50M

static double MaxBytesForLevel(int level) { // NOTE:htt, 每层最大容量,0/1层10M, 2层100M, 3层1G, 4层10G, 5层100G, 6层1T
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = 10 * 1048576.0;  // Result for both level-0 and level-1 // NOTE:htt, 第0/1层,最大为10M
  while (level > 1) { // NOTE:htt, 第2层往上,每层容量扩大10倍
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(int level) { // NOTE:htt, 文件最大为2M
  return kTargetFileSize;  // We could vary per level to reduce number of files?
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) { // NOTE:htt, 所有文件的大小之和
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

namespace {
std::string IntSetToString(const std::set<uint64_t>& s) { // NOTE:htt, 数字集合转string, {1,2,3}
  std::string result = "{";
  for (std::set<uint64_t>::const_iterator it = s.begin();
       it != s.end();
       ++it) {
    result += (result.size() > 1) ? "," : "";
    result += NumberToString(*it); // NOTE:htt, 数字转字符串
  }
  result += "}";
  return result;
}
}  // namespace

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_; // NOTE:htt, 调整链表指针
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) { // NOTE:htt, 遍历当前Version保存的所有level的文件,减少文件引用
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) { // NOTE:htt, 文件没有引用则可以删除
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) { // NOTE:htt, 二分查找key所在的files列表的index
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) { // NOTE:htt, 判断user key是否在文件之后
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) { // NOTE:htt, 判断user key是否在文件之前
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) { // NOTE:htt, 判断 smallest_user_key,largest_user_key 和 files文件列表是否有交集
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {  // NOTE:htt, 文件相交,则逐个文件判断, level0层会相交,其他层不会相交
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) { // NOTE:htt, 没有交集
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != NULL) { // NOTE:htt, 文件不相交,则先用smallest_user_key找打对应文件,在判断largest_user_key是否在其中
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator { // NOTE:htt, level文件的迭代器
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target); // NOTE:htt, 查找 target在 flist_ 文件列表中的index
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() { // NOTE:htt, 设置index_为文件列表最大的下标
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() { // NOTE:htt, 增加index
    assert(Valid());
    index_++;
  }
  virtual void Prev() { // NOTE:htt, 减少index
    assert(Valid());
    if (index_ == 0) { // NOTE:htt, 如果index已经为0,则调整index为文件大小
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const { // NOTE:htt, 获取当前index对应文件的最大的InternalKey
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const { // NOTE:htt, 获取index的value,为对应文件的 {number, file_size}组合
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number); // NOTE:htt, 文件number
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size); // NOTE:htt, 文件大小
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_; // NOTE:htt, 内部key采用特殊的比较器comparator
  const std::vector<FileMetaData*>* const flist_; // NOTE:htt, 文件列表
  uint32_t index_; // NOTE:htt, 当前迭代的index

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16]; // NOTE:htt, 当前index对应的value值,保存为对应文件的{number, file_size}
};

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) { // NOTE:htt, 构建 file_value{file number, file_size}的文件缓存迭代器
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) { // NOTE:htt, file_value需要为 {number, file_size}
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8)); // NOTE:htt, 读取file_number文件,并构建talbe的两层迭代器
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const { // NOTE:htt, 为level N层生成迭代器,先查找文件,然后生成文件的迭代器,包含index迭代器和data迭代器的双层迭代器
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), // NOTE:htt, level层文件迭代器
      &GetFileIterator, vset_->table_cache_, options); // NOTE:htt, 生成文件迭代器,包含index迭代器和data迭代器的双层迭代器
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) { // NOTE:htt, 针对level N 每层都生成文件迭代器,以便文件查找;其中level 0层是每个文件生成迭代器, >= level 1层,则每层一个迭代器
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) { // NOTE:htt, level 0层文件有可能重叠,则每个文件都构建一个迭代器
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size)); // NOTE:htt, 读取level 0层number文件,构建talbe的两层迭代器
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) { // NOTE:htt, >= level 1层文件无重叠，则可以单层构建一个迭代器
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));// NOTE:htt, 为level N层生成迭代器,先查找文件,然后生成文件的迭代器,包含index迭代器和data迭代器的双层迭代器
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState { // NOTE:htt, 状态
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver { // NOTE:htt, 保存当前 用户key 的查找状态,以及对应值
  SaverState state; // NOTE:htt, 状态
  const Comparator* ucmp; // NOTE:htt, 用户key比较器
  Slice user_key; // NOTE:htt, 用户key
  std::string* value; // NOTE:htt, 保存的valude
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) { // NOTE:htt, 通过internalKey来确认用户key是否存在,如果存在则保存对应的value
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) { // NOTE:htt, 用户key相同
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) { // NOTE:htt, 如果查找到,则设置value
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) { // NOTE:htt, 判断哪个文件更新
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) { // NOTE:htt, 保存首次查找到 user_key的文件
  // TODO(sanjay): Change Version::Get() to use this function.
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) { // NOTE:htt, 遍历 level0层的所有文件,如果 user_key在文件范围内则保存下来
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst); // NOTE:htt, 更新的文件排在前面
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) { // NOTE:htt, 非首次匹配则退出
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) { // NOTE:htt, 从level1开始,每层仅需要定位一个文件查找
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) { // NOTE:htt, 非首次匹配则退出
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) { // NOTE:htt, 从levelDB的7层文件来读取数据,先从0层读取,如果未找到则继续1层查找,直到找到或未找打
  Slice ikey = k.internal_key(); // NOTE:htt, 内部key, 即{user_key, seq, t}组合
  Slice user_key = k.user_key(); // NOTE:htt, 获取 user_key
  const Comparator* ucmp = vset_->icmp_.user_comparator(); // NOTE:htt, user_key比较器
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  for (int level = 0; level < config::kNumLevels; level++) { // NOTE:ht, 如果0没找到则继续1层,如果1层没找到继续2层
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    if (level == 0) { // NOTE:htt, 如果是level0, 则找出所有和 user_key 有交集的文件
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      tmp.reserve(num_files); // NOTE:htt, tmp至少可以存储 num_files 个对象
      for (uint32_t i = 0; i < num_files; i++) {
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) { // NOTE:htt, 如果user_key在 [f->smallest, f->larget]之间
          tmp.push_back(f);
        }
      }
      if (tmp.empty()) continue;

      std::sort(tmp.begin(), tmp.end(), NewestFirst); // NOTE:htt, 新文件排在前面
      files = &tmp[0];
      num_files = tmp.size();
    } else { // NOTE:htt, 如果是 >= 1的level层
      // Binary search to find earliest index whose largest key >= ikey.
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);// NOTE:htt, 二分查找key所在的files列表的index
      if (index >= num_files) {
        files = NULL;
        num_files = 0;
      } else {
        tmp2 = files[index];
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = NULL;
          num_files = 0;
        } else {
          files = &tmp2;
          num_files = 1;
        }
      }
    }

    for (uint32_t i = 0; i < num_files; ++i) { // NOTE:htt, 遍历获取到文件,查找ikey
      if (last_file_read != NULL && stats->seek_file == NULL) {
        // We have had more than one seek for this read.  Charge the 1st file.
        stats->seek_file = last_file_read; // NOTE:htt, 上一次读取的文件,即上一次未找打,如果此次找到,则对上一次文件做合并操作
        stats->seek_file_level = last_file_read_level; // NOTE:htt, 上一次读取文件level
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp; // NOTE:htt, 保存user_key比较器
      saver.user_key = user_key;
      saver.value = value; // NOTE:htt, 设置为value的指针
      s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                   ikey, &saver, SaveValue); // NOTE:htt, 查找ikey,并且如果找到则保存value到 saver.value
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          break;      // Keep searching in other files
        case kFound:
          return s; // NOTE:htt, 找到则返回saver,对应saver.value为值
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) { // NOTE:htt, 根据空读的次数来减少allowed_seeks,如果为0则记录文件为待compaction
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--; // NOTE:htt, 减少allowed_seeks值, 如果为0则将将下一次compact的文件设置为当前空读次数减为0的文件
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f; // NOTE:htt, 如果读文件超过个数则设置为下一次待合并文件
      file_to_compact_level_ = stats.seek_file_level; // NOTE:htt, 记录对应待合并level
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) { // NOTE:htt, 读采样,保存首次match的文件,如果match超过2个文件则尝试更新待合并文件
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State { // NOTE:htt, 状态
    GetStats stats;  // Holds first matching file // NOTE:htt, 首次match的文件
    int matches; // NOTE:htt, match的个数

    static bool Match(void* arg, int level, FileMetaData* f) { // NOTE:htt, Match后设置对应的首次match的文件
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) { // NOTE:htt, 如果为1则保存第一次match的文件
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0; // NOTE:htt, 设置matches为0
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match); // NOTE:htt, 保存首次查找到 user_key的文件

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) { // NOTE:htt, 如果match的文件超过2个,则认为第一个文件allowed_seeks需减1,然后可以作为compact文件
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { // NOTE:htt, 增加Version引用
  ++refs_;
}

void Version::Unref() { // NOTE:htt, 减少version引用
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_; // NOTE:htt, 减少version引用
  if (refs_ == 0) { // NOTE:htt, 如果引用为0,则可以删除
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) { // NOTE:htt, 判断[smallest_user_key, largest_user_key]和level n层文件是否相交
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key); // NOTE:htt, user_key 是否和level n层文件是否相交
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) { // NOTE:htt, 选择MemTable可以直接落入层,范围level [0,1,2]这三层文件,目标避免0->1的compact
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) { // NOTE:htt, 如MemTable和level0层没交集,继续尝试从[0,1,2]考虑
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) { // NOTE:htt, 考虑[0,1,2]做为和MemTable compact的层
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) { // NOTE:htt, 如level+1层和user_key有交集,选择level层
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps); // NOTE:htt, 获取level+2层和 [begin, end]有交集的文件
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > kMaxGrandParentOverlapBytes) { // NOTE:htt, 如果level+2层和[start,limit]交集文件大于20M,则不考虑level2层,选择level层开始
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) { // NOTE:htt, 获取 level 层和 [begin, end]有交集的文件,其中level0层需考虑文件交集范围扩大
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear(); // NOTE:htt, 每次会清空
  Slice user_begin, user_end;
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator(); // NOTE:htt, user_key比较器
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key(); // NOTE:htt, 文件中最小user_key
    const Slice file_limit = f->largest.user_key(); // NOTE:htt, 文件中最大user_key
    if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) { // NOTE:htt, level 0层文件之间并没有严格有序,需要根据加入的文件重新判定[user_begin, user_end]
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start; // NOTE:htt, 如果文件start比user_begin小,则说明检查的开始范围要变小,并需要重新计算交集文件
          inputs->clear(); // NOTE:htt, 文件重新计算
          i = 0;
        } else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit; // NOTE:htt, 如果文件end比user_end大,则说明检查的结束范围要变大,并需要重新计算交集文件
          inputs->clear();
          i = 0; // NOTE:htt, 文件重新计算
        }
      }
    }
  }
}

std::string Version::DebugString() const { // NOTE:htt, 打印当前Version下所有level文件的信息
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder { // NOTE:htt, 根据当前base_->files_[level]和新增文件(levels_[level].added_files)生成新的Version->files_[level]文件
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey { // NOTE:htt, 比较文件的smallest值,优先选择更大的值
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const { // NOTE:htt,比较smallet大小,优先选择更大值
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0); // NOTE:htt, 反向排序,选择大的在前
      } else {
        // Break ties by file number
        return (f1->number < f2->number); // NOTE:htt, 反向排序,选择大的在前
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet; // NOTE:htt, 将更大的smallest放在前面
  struct LevelState { // NOTE:htt, 某level层文件状态
    std::set<uint64_t> deleted_files; // NOTE:htt, 本level层删除文件number列表
    FileSet* added_files; // NOTE:htt, 本level层增加的文件
  };

  VersionSet* vset_; // NOTE:htt, 当前的versionSet
  Version* base_; // NOTE:htt, 当前基于处理的version
  LevelState levels_[config::kNumLevels]; // NOTE:htt, 七层level的增加或删除文件统计

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref(); // NOTE:htt, 增加Version引用
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_; // NOTE:htt, 设置比较器
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp); // NOTE:htt, 初始化每层的增加文件
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added; // NOTE:htt, 删除level层 added Set
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--; // NOTE:htt, 减少文件的引用
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref(); // NOTE:htt, 减少Version的引用
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) { // NOTE:htt, 将VersionEdit中删除和添加的文件加入到各level层文件状态,包括删除和添加文件信息
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] = // NOTE:htt, 设置VersionEdit level 记录即将compact的InternalKey
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) { // NOTE:htt, 保存level层待删除文件
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number); // NOTE:htt, 从VersionEdit获取 level 层需要删除的文件 number
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1; // NOTE:htt, 设置新增文件引用为1

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100; // NOTE:htt, 在读失效场景,最小seek的空读4M(把16K看出40K),对应compect为160K

      levels_[level].deleted_files.erase(f->number); // NOTE:htt, level移除待添加的文件
      levels_[level].added_files->insert(f); // NOTE:htt, level层文件加入待添加的文件
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) { // NOTE:htt, 将当前最新状态保存到新的version中,合并base_ version文件和 edit中新增文件,并去除edit删除文件
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) { // NOTE:htt, 将level层原有文件(base_->files)和新增文件(levels_[level].added_files)归并到新的 version->files_[level]
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) { // NOTE:htt, 归并levels_[level].added_files和base_->files_[level]中文件,并追加新的v->files_[level]
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter); // NOTE:htt, 将文件f加入到 v->files_[level]层
        }

        MaybeAddFile(v, level, *added_iter); // NOTE:htt, 将 levels_[level].added_files中迭代的文件加入到新的v->files_[level]
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) { // NOTE:htt,将base_->files_[level]剩余文件追加到新的v->files_[level]
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) { // NOTE:htt, 验证文件之间有序
          const InternalKey& prev_end = v->files_[level][i-1]->largest; // NOTE:htt, 前一个文件中最大的InternalKey
          const InternalKey& this_begin = v->files_[level][i]->smallest; /// NOTE:htt, 当前文件中最小的InternalKey
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) { // NOTE:htt, 如果前一个文件中key当前当前文件key则出错
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) { // NOTE:htt, 将文件f加入到 v->files_[level]层
    if (levels_[level].deleted_files.count(f->number) > 0) { // NOTE:htt, 如果删除列表包含文件则不加入文件
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) { // NOTE:htt, 校验 > 0 层的文件必须有序
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++; // NOTE:htt, 增加文件的引用
      files->push_back(f); // NOTE:htt, 将文件添加到末尾
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache), // NOTE:htt, 设置TableCache
      icmp_(*cmp), // NOTE:htt, 设置InternalKeyComparator
      next_file_number_(2), // NOTE:htt, 下一个文件number, 初始为2
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) {
  AppendVersion(new Version(this)); // NOTE:htt, 添加version并设置为current_
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) { // NOTE:htt, 添加version并设置为current
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v; // NOTE:htt, 赋值初始化Version
  v->Ref(); // NOTE:htt, 增加version引用

  // Append to linked list
  v->prev_ = dummy_versions_.prev_; // NOTE:htt, 设置current version的前置引用为 dummy_versions_
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) { // NOTE:htt, 根据edit和VersionSet生成新的Version,并保存当前的文件信息到mainfest中,并追加新的edit内容到mainfest
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_); // NOTE:htt, 设置日志number
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit); // NOTE:htt, 将VersionEdit中删除和添加的文件加入到各level层文件状态,包括删除和添加文件信息
    builder.SaveTo(v); // NOTE:htt, 将当前最新状态保存到新的version中,合并base_ version文件和 edit中新增文件,并去除edit删除文件
  }
  Finalize(v); // NOTE:htt, 设置当前Version最需要compact的<level, score>值

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file; // NOTE:htt, 用于确认是否产生了新的mainfest文件
  Status s;
  if (descriptor_log_ == NULL) { // NOTE:htt, 首次生成新的mainfest,并将现有文件compact和所有文件信息保存
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_); // NOTE:htt, 描述文件, ${dbname}/MANIFEST-${number}
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_); // NOTE:htt, 生成mainfest文件实际写入的文件描述符
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_); // NOTE:htt, 封装mainfest描述符,采用WAL日志块方式分内容,一旦创建除非重新加载则当前会一直使用该 mainfest来存储新增的元信息,风险是如果重启则恢复时间会比较长,因为需要处理这个过程中的文件列表
      s = WriteSnapshot(descriptor_log_); // NOTE:htt, 保存当前compact值以及所有文件信息到mainfest(采用WAL日志格式写入)
    }
  }

  // Unlock during expensive MANIFEST log write
  { // NOTE:htt, 将新的文件信息(包括compact和文件信息)写入到mainfest,同时将MANIFEST-${number}值写入${dbname}/CURRENT
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) { // NOTE: htt, 将新的文件信息(包括compact和文件信息)写入到mainfest
      std::string record;
      edit->EncodeTo(&record); // NOTE:htt, 将新的VersionEdit内容序列化
      s = descriptor_log_->AddRecord(record); // NOTE:htt, 保存增量的VersionEdit内容到mainfest
      if (s.ok()) {
        s = descriptor_file_->Sync(); // NOTE: htt, 将目录entry信息刷盘，同时将用户态数据刷入内核，内核态数据刷入磁盘
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_); // NOTE:htt, 将MANIFEST-${number}值写入到${dbname}/CURRENT
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v); // NOTE:htt, 添加version并设置为current
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover() { // NOTE:htt, 从${dbname}/CURRENT获取mainfest文件名,从mainfest读取VersionEdit列表恢复对应Version
  struct LogReporter : public log::Reader::Reporter { // NOTE:htt, 保存status
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s; // NOTE:htt, 保存status
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current); // NOTE:htt, 从${dbname}/CURRENT读取mainfest文件名称
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1); // NOTE:htt, 去除\n

  std::string dscname = dbname_ + "/" + current; // NOTE:htt, mainfest路径${dbname}/MANIFEST-${number}
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  { // NOTE:htt, 读取mainfest文件中保存的所有Edit信息,用来生成当前的Version(包括所有的文件信息)
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) { // NOTE:htt, 读取一条完整的日志,Full或<First,Mid,...,Last>组合
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) { // NOTE:htt, Edit和当前VersionSet比较器需一致
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit); // NOTE:htt, 将VersionEdit中删除和添加的文件加入到各level层文件状态,包括删除和添加文件信息
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file; // NOTE:htt, 删除mainfest读取文件内存对象,析构时会进行文件关闭
  file = NULL;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number); // NOTE:htt, 确认next_file_number_比prev_log_number新
    MarkFileNumberUsed(log_number); // NOTE:htt, 确认next_file_number_比log_number新
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v); // NOTE:htt, 将当前最新状态保存到新的version中,合并base_version文件和 edit中新增文件,并去除edit删除文件
    // Install recovered version
    Finalize(v);// NOTE:htt, 设置当前Version 需要compact(合并)的<level, score>值
    AppendVersion(v); // NOTE:htt, 添加version并设置为current
    manifest_file_number_ = next_file; // NOTE:htt, 设置当前 mainfest file number 值
    next_file_number_ = next_file + 1; // NOTE:htt, 更新 next_file_number_值
    last_sequence_ = last_sequence; // NOTE:htt, 更新last_sequence
    log_number_ = log_number; // NOTE:htt, 设置当前的 log_number
    prev_log_number_ = prev_log_number;
  }

  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) { // NOTE:htt, 确保next_file_number_新值
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) { // NOTE:htt, 设置当前Version 需要compact(合并)的<level, score>值
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger); // NOTE:htt, level0层有4个文件即触发compaction
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]); // NOTE:htt, 所有文件的大小之和
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level); // NOTE:htt, 每层最大容量,0/1层10M, 2层100M, 3层1G, 4层10G, 5层100G, 6层1T, level>1层按照该层文件大小之和除以该层对应默认大小之和,如果超过并且越大则越先触发compaction
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level; // NOTE:htt, 设置最需要compact的level
  v->compaction_score_ = best_score; // NOTE:htt, 设置最需要compact的score
}

Status VersionSet::WriteSnapshot(log::Writer* log) { // NOTE:htt, 保存当前compact值以及所有文件信息到mainfest(采用WAL日志格式写入)
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit; // NOTE:htt, 临时VersionEdit,用于保存当前compact和所有文件信息
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key); // NOTE:htt, 保存每层的<level, 下一个compact key>的值
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) { // NOTE:htt, 对于新的mainfest,首次写入会保存所有的文件信息
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest); // NOTE:htt, 保存每层<leve, file>到edit中
    }
  }

  std::string record;
  edit.EncodeTo(&record); // NOTE:htt, 将VersionEdit内容序列化
  return log->AddRecord(record); // NOTE:htt, 将slice写入到WAL日志中,如果长度大于块,则分多个部分写入
}

int VersionSet::NumLevelFiles(int level) const { // NOTE:htt, level每层文件个数
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const { // NOTE:htt, 统计所有level层文件个数
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) { // NOTE:htt, 查找key在leveldb所有文件中近似offset
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) { // NOTE:htt, 比ikey小文件都统计
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size; // NOTE:htt, 加上 file size,计算全局的offset
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else { // NOTE:htt, 如果恰好在某个sst文件内则进一步统计在文件中block的偏移
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());// NOTE:htt, 找到key所在block偏移,若key不存在,返回metablockindex偏移
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) { // NOTE:htt, 添加level所有Version下的共同存活的文件
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const { // NOTE:htt, 计算当前Version下level层所有文件大小
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() { // NOTE:htt, 获取所有从level1层开始每个文件和下一层文件交集,并获取交集最大的值
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) { // NOE:htt, 计算每个level层中每个文件和下一层文件交集
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps); // NOTE:htt, 获取当前文件和level+1有交集文件列表
      const int64_t sum = TotalFileSize(overlaps); // NOTE:htt, 获取当前交集的大小
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) { // NOTE:htt, 获取inputs文件列表中最小的key和最大的key
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) { // NOTE:htt, 获取inputs1和inputs2所有文件列表中最小的key和最大的key
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) { // NOTE:htt, 构建compaction中涉及两层需要merge文件的合并的迭代器
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2); // NOTE:htt,如果有level0,则需要 level0.size()+1,否则2个iter即够
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(
              options, files[i]->number, files[i]->file_size); // NOTE:htt, level0层直接构建文件迭代器(包括index iter和data iter)
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]), // NOTE:htt, 先查找level层对应文件
            &GetFileIterator, table_cache_, options); // NOTE:htt, 再构建文件迭代器(包括index iter和data iter)
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);// NOTE:htt, 构建多个iterator的合并处理
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() { // NOTE:htt, 选择待compact文件,并扩容level层文件列表(前提是扩容后总文件之后小于50M)
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) { // NOTE:htt, 按文件大小compact,查找level需要compact的文件,当前level层继续compact也需要满足条件
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels); // NOTE:htt, 选择合并层<=5
    c = new Compaction(level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) { // NOTE:htt, 获取level层需要compact的文件
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) { // NOTE:htt, 选择的文件需要比compact_pointer_大
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) { // NOTE:htt, 如果input[0]为空,则从level层第一个文件开始
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]); // NOTE:htt, level层重新从第一个文件选择,则会重新计算largest key,并更新compact_pointer_[level]
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(level);
    c->inputs_[0].push_back(current_->file_to_compact_); // NOTE:htt, 因seek触发的下一个compact文件,直接设置对应文件
  } else {
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) { // NOTE:htt, level0层则选择有交集的文件列表
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]); // NOTE:htt, level0层和其他有交集sst文件则也加入compact
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);// NOTE:htt,计算input1待合并文件, 并扩展input0文件列表,如果扩展后和input1文件之和小于50M

  return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) { // NOTE:htt,计算input1待合并文件, 并扩展input0文件列表,如果扩展后和input1文件之和小于50M
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);// NOTE:htt, 获取inputs文件列表中最小的key和最大的key

  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]); // NOTE:htt, 获取level+1层有交集文件列表

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit); // NOTE:htt, 获取待compact最小key和最大key

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0); // NOTE:htt, 扩大level层和<all_star,all_limit>交集文件
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size < kExpandedCompactionByteSizeLimit) { // NOTE:htt,如果level+1和level expend文件小于50M,则扩容待compact文件列表
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) { // NOTE:htt, 如扩展level层文件后inputs_[1]文件交集文件没有变化,则调整inputs_[0]
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0; // NOTE:htt, inputs_[0]调整为expanded0
        c->inputs_[1] = expanded1; // NOTE:htt, inputs_[1]调整为expanded1
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_); // NOTE:htt, 获取 level+2层和<all_start,all_limit>相交的文件列表
  }

  if (false) {
    Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        level,
        smallest.DebugString().c_str(),
        largest.DebugString().c_str());
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString(); // NOTE:htt, 调整level层待compact最大key {user_key,seq,t}为largest
  c->edit_.SetCompactPointer(level, largest); // NOTE:htt, 设置edit的levelcent待compact最大key为largest
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) { // NOTE:htt, 选择level层和[begin,end]有交集文件列表作为level层待compact文件列表,并确认level+1文件列表
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs); // NOTE:htt, 获取 level 层和 [begin, end]有交集的文件,其中level0层需考虑文件交集范围扩大
  if (inputs.empty()) {
    return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) { // NOTE:htt, 如果已有文件之后超过2M,则只compact已有文件列表
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs; // NOTE:htt, 确认inputs[0]文件列表
  SetupOtherInputs(c); // NOTE:htt, 计算input1待comppact文件,并扩容inputs0待compact文件列表,如果扩展后和input1文件之和小于50M
  return c;
}

Compaction::Compaction(int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(level)),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref(); // NOTE:htt, 减少version的引用
  }
}

bool Compaction::IsTrivialMove() const { // NOTE:htt, 是否为小合并
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && // NOTE:htt, level层合并只有一个文件
          num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <= kMaxGrandParentOverlapBytes);
}

void Compaction::AddInputDeletions(VersionEdit* edit) { // NOTE:htt, 将本次compaction涉及到 level/level+1 层文件添加到待删除列表
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number); // NOTE:htt, 将 <level+0/1, 文件number> 添加到待删除列表
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) { // NOTE:htt, 确认level+1层的key在更高层不存在
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator(); // NOTE:htt, 获取user key比较器
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) { // NOTE:htt, 确认 level+2 往上的层, 当前 user_key是否在其中文件范围之内
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break; // NOTE:htt, 继续下一层的判断
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) { // NOTE:htt, 当前是否需要停止,如果和levle+2层太多重叠则停止
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > kMaxGrandParentOverlapBytes) { // NOTE:htt, 重叠超过20M,则太多重叠,启动新的处理
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() { // NOTE:htt, 释放input version
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

}  // namespace leveldb
