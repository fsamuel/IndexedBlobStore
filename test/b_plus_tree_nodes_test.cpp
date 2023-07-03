#include "gtest/gtest.h"
#include "b_plus_tree_nodes.h"

using namespace b_plus_tree;

TEST(BPlusTreeNodesTest, CastToBaseNodeFromLeafNode) {
  LeafNode<4> leaf_node(2);
  BaseNode<4>* base_node = reinterpret_cast<BaseNode<4>*>(&leaf_node);

  EXPECT_EQ(base_node->is_leaf(), true);
  EXPECT_EQ(base_node->is_internal(), false);
  EXPECT_EQ(base_node->num_keys(), 2);

  // Update the keys in leaf_node and verify they're visible in base
  // node.
  leaf_node.set_key(0, 1);
  leaf_node.set_key(1, 2);
  EXPECT_EQ(base_node->keys[0], 1);
  EXPECT_EQ(base_node->keys[1], 2);
}

TEST(BPlusTreeNodesTest, CastToBaseNodeFromInternalNode) {
  InternalNode<4> internal_node(2);
  BaseNode<4>* base_node = reinterpret_cast<BaseNode<4>*>(&internal_node);

  EXPECT_EQ(base_node->is_leaf(), false);
  EXPECT_EQ(base_node->is_internal(), true);
  EXPECT_EQ(base_node->num_keys(), 2);

  // Update the keys in internal_node and verify they're visible in base
  // node.
  internal_node.set_key(0, 1);
  internal_node.set_key(1, 2);
  EXPECT_EQ(base_node->keys[0], 1);
  EXPECT_EQ(base_node->keys[1], 2);
}
