//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

  template <typename K, typename V>
  ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
    auto bucket = std::make_shared<Bucket>(bucket_size,0);
    dir_.push_back(bucket);
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
    int mask = (1 << global_depth_) - 1;
    return std::hash<K>()(key) & mask;
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
    std::scoped_lock<std::mutex> lock(latch_);
    return GetGlobalDepthInternal();
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
    return global_depth_;
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
    std::scoped_lock<std::mutex> lock(latch_);
    return GetLocalDepthInternal(dir_index);
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
    return dir_[dir_index]->GetDepth();
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
    std::scoped_lock<std::mutex> lock(latch_);
    return GetNumBucketsInternal();
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
    return num_buckets_;
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);

    auto bucket = dir_[IndexOf(key)];

    return bucket->Find(key,value);
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);

    auto bucket = dir_[IndexOf(key)];

    return bucket->Remove(key);
  }

  template <typename K, typename V>
  void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
    std::scoped_lock<std::mutex> lock(latch_);

    while (dir_[IndexOf(key)]->IsFull()) {
      auto index = IndexOf(key);
      auto bucket = dir_[index];

      if (bucket->GetDepth() == global_depth_) {
        global_depth_++;
        int dir_num = dir_.size();
        dir_.resize(dir_num*2);
        for (int i = 0; i < dir_num; i++) {
          dir_[i + dir_num] = dir_[i];
        }
      }

      int mask = 1 << bucket->GetDepth();

      bucket->IncrementDepth();
      auto new_bucket1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
      auto new_bucket2 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
      num_buckets_++;

      for (const auto &item : bucket->GetItems()) {
        size_t hash_key = std::hash<K>()(item.first) & mask;
        if (hash_key == 0U) {
          new_bucket1->Insert(item.first, item.second);
        } else {
          new_bucket2->Insert(item.first, item.second);
        }
      }

      for (size_t i = 0; i < dir_.size(); i++) {
        if (dir_[i] == bucket) {
          if ((i & mask) == 0U) {
            dir_[i] = new_bucket1;
          } else {
            dir_[i] = new_bucket2;
          }
        }
      }
    }

    auto bucket = dir_[IndexOf(key)];

    for (auto &item : bucket->GetItems()) {
      if (item.first == key) {
        item.second = value;
        return;
      }
    }

    bucket->Insert(key, value);
  }

  //===--------------------------------------------------------------------===//
  // Bucket
  //===--------------------------------------------------------------------===//
  template <typename K, typename V>
  ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
    for(std::pair<K,V> pair : list_){
      if(pair.first==key){
        value=pair.second;
        return true;
      }
    }
    return false;
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
    for(std::pair<K,V> pair : list_){
      if(pair.first==key){
        list_.remove(pair);
        return true;
      }
    }
    return false;
  }

  template <typename K, typename V>
  auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
    if (IsFull()) {
      return false;
    }
    list_.emplace_back(key, value);
    return true;
  }

  template class ExtendibleHashTable<page_id_t, Page *>;
  template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
  template class ExtendibleHashTable<int, int>;
  // test purpose
  template class ExtendibleHashTable<int, std::string>;
  template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
