#include "b_plus_tree_nodes.h"

namespace b_plus_tree {
void PrintNode(BlobStoreObject<const HeadNode> node) {
  if (node == nullptr) {
    std::cout << "NULL head" << std::endl;
    return;
  }
  std::cout << "Head (Index = " << node.Index()
            << ", root = " << node->root_index
            << ", version = " << node->version << ")" << std::endl;
}
}  // namespace b_plus_tree