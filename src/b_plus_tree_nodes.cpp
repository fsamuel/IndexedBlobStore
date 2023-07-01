#include "b_plus_tree_nodes.h"

void PrintNode(BlobStoreObject<const HeadNode> node) {
  if (node == nullptr) {
    std::cout << "NULL head" << std::endl;
    return;
  }
  std::cout << "head (Index = " << node.Index()
            << ", root = " << node->root_index
            << ", version = " << node->get_version() << ")" << std::endl;
}