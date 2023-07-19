#ifndef SERIALIZE_TRAITS_H_
#define SERIALIZE_TRAITS_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

template <typename T>
struct SerializeTraits {
  static size_t Size(const T&) { return sizeof(T); }

  static void Serialize(char* buffer, const T& t) {
    memcpy(buffer, &t, sizeof(T));
  }

  static void Deserialize(const char* buffer, T* t) {
    memcpy(t, buffer, sizeof(T));
  }
};

// SerializeTraits for std::string
template <>
struct SerializeTraits<std::string> {
  static size_t Size(const std::string& s) { return sizeof(size_t) + s.size(); }

  static void Serialize(char* buffer, const std::string& s) {
    size_t size = s.size();
    memcpy(buffer, &size, sizeof(size_t));
    memcpy(buffer + sizeof(size_t), s.c_str(), size);
  }

  static void Deserialize(const char* buffer, std::string* s) {
    size_t size;
    memcpy(&size, buffer, sizeof(size_t));
    s->resize(size);
    memcpy(&(*s)[0], buffer + sizeof(size_t), size);
  }
};

// SerializeTraits for std::vector
template <typename T>
struct SerializeTraits<std::vector<T>> {
  static size_t Size(const std::vector<T>& v) {
    size_t size = sizeof(size_t);
    for (const auto& t : v) {
      size += SerializeTraits<T>::Size(t);
    }
    return size;
  }

  static void Serialize(char* buffer, const std::vector<T>& v) {
    size_t size = v.size();
    memcpy(buffer, &size, sizeof(size_t));
    buffer += sizeof(size_t);
    for (const auto& t : v) {
      SerializeTraits<T>::Serialize(buffer, t);
      buffer += SerializeTraits<T>::Size(t);
    }
  }

  static void Deserialize(const char* buffer, std::vector<T>* v) {
    size_t size;
    memcpy(&size, buffer, sizeof(size_t));
    buffer += sizeof(size_t);
    v->resize(size);
    for (auto& t : *v) {
      SerializeTraits<T>::Deserialize(buffer, &t);
      buffer += SerializeTraits<T>::Size(t);
    }
  }
};

// SerializeTraits for std::unordered_set.
template <typename T>
struct SerializeTraits<std::unordered_set<T>> {
  static size_t Size(const std::unordered_set<T>& s) {
    size_t size = sizeof(size_t);
    for (const auto& t : s) {
      size += SerializeTraits<T>::Size(t);
    }
    return size;
  }

  static void Serialize(char* buffer, const std::unordered_set<T>& s) {
    size_t size = s.size();
    memcpy(buffer, &size, sizeof(size_t));
    buffer += sizeof(size_t);
    for (const auto& t : s) {
      SerializeTraits<T>::Serialize(buffer, t);
      buffer += SerializeTraits<T>::Size(t);
    }
  }

  static void Deserialize(const char* buffer, std::unordered_set<T>* s) {
    size_t size;
    memcpy(&size, buffer, sizeof(size_t));
    buffer += sizeof(size_t);
    for (size_t i = 0; i < size; ++i) {
      T t;
      SerializeTraits<T>::Deserialize(buffer, &t);
      buffer += SerializeTraits<T>::Size(t);
      s->insert(std::move(t));
    }
  }
};

// SerializeTraits for std::unordered_map.
template <typename K, typename V>
struct SerializeTraits<std::unordered_map<K, V>> {
  static size_t Size(const std::unordered_map<K, V>& m) {
    size_t size = sizeof(size_t);
    for (const auto& p : m) {
      size += SerializeTraits<K>::Size(p.first);
      size += SerializeTraits<V>::Size(p.second);
    }
    return size;
  }

  static void Serialize(char* buffer, const std::unordered_map<K, V>& m) {
    size_t size = m.size();
    memcpy(buffer, &size, sizeof(size_t));
    buffer += sizeof(size_t);
    for (const auto& p : m) {
      SerializeTraits<K>::Serialize(buffer, p.first);
      buffer += SerializeTraits<K>::Size(p.first);
      SerializeTraits<V>::Serialize(buffer, p.second);
      buffer += SerializeTraits<V>::Size(p.second);
    }
  }

  static void Deserialize(const char* buffer, std::unordered_map<K, V>* m) {
    size_t size;
    memcpy(&size, buffer, sizeof(size_t));
    buffer += sizeof(size_t);
    for (size_t i = 0; i < size; ++i) {
      K k;
      SerializeTraits<K>::Deserialize(buffer, &k);
      buffer += SerializeTraits<K>::Size(k);
      V v;
      SerializeTraits<V>::Deserialize(buffer, &v);
      buffer += SerializeTraits<V>::Size(v);
      m->insert(std::make_pair(std::move(k), std::move(v)));
    }
  }
};

#endif  // SERIALIZE_TRAITS_H_