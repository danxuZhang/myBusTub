//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include "common/exception.h"

namespace bustub {
static const size_t INF_TIMESTAMP = std::numeric_limits<size_t>::max();

LRUKNode::LRUKNode(frame_id_t fid, size_t k) : k_(k), fid_(fid) {}

auto LRUKNode::GetFrameId() const -> frame_id_t { return fid_; }

auto LRUKNode::IsEvictable() const -> bool { return is_evictable_; }

auto LRUKNode::SetEvictable(bool set_evictable) -> void { is_evictable_ = set_evictable; }

auto LRUKNode::RecordAccess(size_t timestamp) -> void {
  if (history_.size() == k_) {
    history_.pop_front();
  }
  history_.push_back(timestamp);
}

auto LRUKNode::GetEarliestTimestamp() const -> size_t { return history_.front(); }

auto LRUKNode::GetKBackDist(size_t current_timestamp) const -> size_t {
  if (history_.size() < k_) {
    return INF_TIMESTAMP;
  }

  return current_timestamp - history_.front();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (node_store_.empty() || curr_size_ == 0) {
    return false;
  }

  std::lock_guard<std::mutex> guard{latch_};
  std::vector<std::unique_ptr<LRUKNode>> inf_nodes;
  size_t max_k_dist = 0;
  std::unique_ptr<LRUKNode> max_k_dist_node;

  for (auto &p : node_store_) {
    auto &node = p.second;
    if (!node.IsEvictable()) {
      continue;
    }

    size_t k_dist = node.GetKBackDist(current_timestamp_);
    if (k_dist == INF_TIMESTAMP) {
      inf_nodes.push_back(std::make_unique<LRUKNode>(node));
      continue;
    }

    if (k_dist > max_k_dist) {
      max_k_dist = k_dist;
      max_k_dist_node = std::make_unique<LRUKNode>(node);
    }
  }

  std::unique_ptr<LRUKNode> victim;
  if (!inf_nodes.empty()) {
    victim = std::move(*std::min_element(inf_nodes.begin(), inf_nodes.end(), [](const auto &a, const auto &b) {
      return a->GetEarliestTimestamp() < b->GetEarliestTimestamp();
    }));

  } else {
    victim = std::move(max_k_dist_node);
  }

  *frame_id = victim->GetFrameId();
  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument{"invalid frame id"};
  }

  std::lock_guard<std::mutex> guard{latch_};

  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    LRUKNode new_node{frame_id, k_};
    new_node.RecordAccess(current_timestamp_);
    node_store_.emplace(frame_id, new_node);
  } else {
    it->second.RecordAccess(current_timestamp_);
  }

  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard{latch_};
  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    throw std::invalid_argument{"cannot find frame_id"};
  }

  if (set_evictable && !it->second.IsEvictable()) {
    it->second.SetEvictable(true);
    curr_size_++;
  } else if (!set_evictable && it->second.IsEvictable()) {
    it->second.SetEvictable(false);
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard{latch_};
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!it->second.IsEvictable()) {
    throw std::invalid_argument{"frame is not evictable"};
  }

  curr_size_--;
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
