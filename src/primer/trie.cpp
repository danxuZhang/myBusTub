#include "primer/trie.h"
#include <memory>
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto node = root_;

  for (char ch : key) {
    auto child_it = node->children_.find(ch);
    if (child_it == node->children_.end()) {
      return nullptr;
    }
    node = child_it->second;
  }

  auto val_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(node);
  return val_node ? val_node->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (!root_) {
    // Handle the empty trie case by creating a new trie with a single key-value pair.
    std::shared_ptr<TrieNode> current_node =
        std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    for (auto it = key.rbegin(); it != key.rend(); ++it) {
      auto new_node =
          std::make_shared<TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>({{*it, current_node}}));
      current_node = std::move(new_node);
    }
    return Trie{current_node};
  }

  std::stack<std::shared_ptr<const TrieNode>> node_stack;

  auto node = root_;
  auto it = key.begin();
  while (it != key.end()) {
    node_stack.push(node);
    auto child = node->children_.find(*it);
    if (child == node->children_.end()) {
      break;
    }
    node = child->second;
    ++it;
  }

  if (it == key.end()) {
    std::shared_ptr<const TrieNode> current_node =
        std::make_shared<const TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)));

    for (auto rit = key.rbegin(); rit != key.rend(); ++rit) {
      auto last_parent = node_stack.top();
      auto new_children = last_parent->children_;
      new_children[*rit] = current_node;
      std::shared_ptr<const TrieNode> new_parent;
      auto val_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(last_parent);
      if (val_node) {
        new_parent = std::make_shared<const TrieNodeWithValue<T>>(new_children, val_node->value_);
      } else {
        new_parent = std::make_shared<const TrieNode>(new_children);
      }
      node_stack.pop();
      current_node = std::move(new_parent);
    }

    return Trie{current_node};
  }

  auto rit = key.rbegin();

  std::shared_ptr<const TrieNode> current_node =
      std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  while (std::distance(it, rit.base()) > 1) {
    auto new_node = std::make_shared<TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>({{*rit, current_node}}));
    current_node = new_node;
    rit++;
  }

  while (!node_stack.empty()) {
    char ch = *rit;
    auto old_parent = node_stack.top();
    auto new_children = old_parent->children_;
    new_children[ch] = current_node;
    auto old_val_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(old_parent);
    std::shared_ptr<const TrieNode> new_node;
    if (old_val_node) {
      new_node = std::make_shared<const TrieNodeWithValue<T>>(new_children, old_val_node->value_);
    } else {
      new_node = std::make_shared<const TrieNode>(new_children);
    }
    rit++;
    node_stack.pop();
    current_node = new_node;
  }

  return Trie{current_node};
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
