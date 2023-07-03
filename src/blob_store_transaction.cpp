#include "blob_store_transaction.h"

namespace blob_store {

void PrintNode(BlobStoreObject<const HeadNode> node) {
  if (node == nullptr) {
    std::cout << "NULL head" << std::endl;
    return;
  }
  std::cout << "Head (Index = " << node.Index()
            << ", root = " << node->root_index
            << ", version = " << node->version << ")" << std::endl;
}

}  // namespace blob_store