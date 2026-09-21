#include "include/CNameMatcher.h"
#include <stdlib.h>
#include <string.h>

struct segment {
  size_t offset;
  size_t length;
};

struct scanner_name_pattern {
  uint8_t *bytes;
  size_t *failure;
  struct segment *segments;
  size_t segment_count;
  size_t prefix_length;
  size_t suffix_offset;
  size_t suffix_length;
  size_t minimum_length;
  bool wildcard;
};

void scanner_name_pattern_destroy(scanner_name_pattern *p) {
  if (!p)
    return;
  free(p->bytes);
  free(p->failure);
  free(p->segments);
  free(p);
}

scanner_name_pattern *scanner_name_pattern_create(const uint8_t *bytes,
                                                  size_t length) {
  scanner_name_pattern *p = calloc(1, sizeof(*p));
  if (!p)
    return NULL;
  p->bytes = malloc(length ? length : 1);
  if (!p->bytes)
    goto fail;
  memcpy(p->bytes, bytes, length);
  size_t first = length, last = length;
  for (size_t i = 0; i < length; ++i) {
    if (bytes[i] == '*') {
      if (first == length)
        first = i;
      last = i;
    } else {
      ++p->minimum_length;
    }
  }
  p->wildcard = first != length;
  p->prefix_length = first;
  if (!p->wildcard)
    return p;
  p->suffix_offset = last + 1;
  p->suffix_length = length - last - 1;
  // Common prefix*, *suffix, and prefix*suffix need no search tables.
  for (size_t i = first; i < last;) {
    if (bytes[i] == '*') {
      ++i;
      continue;
    }
    ++p->segment_count;
    while (i < last && bytes[i] != '*')
      ++i;
  }
  if (!p->segment_count)
    return p;
  p->segments = calloc(p->segment_count, sizeof(*p->segments));
  p->failure = calloc(length, sizeof(*p->failure));
  if (!p->segments || !p->failure)
    goto fail;
  size_t count = 0;
  for (size_t i = first; i < last;) {
    if (bytes[i] == '*') {
      ++i;
      continue;
    }
    size_t start = i;
    while (i < last && bytes[i] != '*')
      ++i;
    p->segments[count++] = (struct segment){start, i - start};
    size_t matched = 0;
    for (size_t j = start + 1; j < i; ++j) {
      while (matched && bytes[j] != bytes[start + matched])
        matched = p->failure[start + matched - 1];
      if (bytes[j] == bytes[start + matched])
        ++matched;
      p->failure[j] = matched;
    }
  }
  return p;
fail:
  scanner_name_pattern_destroy(p);
  return NULL;
}

bool scanner_name_pattern_matches(const scanner_name_pattern *p,
                                  const uint8_t *name, size_t length) {
  if (length < p->minimum_length)
    return false;
  if (!p->wildcard && length != p->minimum_length)
    return false;
  if (p->prefix_length && memcmp(name, p->bytes, p->prefix_length))
    return false;
  size_t end = length - p->suffix_length;
  if (p->suffix_length &&
      memcmp(name + end, p->bytes + p->suffix_offset, p->suffix_length))
    return false;
  size_t position = p->prefix_length;
  for (size_t s = 0; s < p->segment_count; ++s) {
    struct segment segment = p->segments[s];
    if (end - position < segment.length)
      return false;
    const uint8_t *literal = p->bytes + segment.offset;
    const uint8_t *first = memchr(name + position, literal[0], end - position);
    if (!first)
      return false;
    position = (size_t)(first - name);
    size_t matched = 0;
    while (position < end) {
      uint8_t byte = name[position++];
      while (matched && byte != literal[matched])
        matched = p->failure[segment.offset + matched - 1];
      if (byte == literal[matched])
        ++matched;
      if (matched == segment.length)
        break;
    }
    if (matched != segment.length)
      return false;
  }
  return true;
}

// Word-at-a-time ASCII detection without alignment assumptions or overreads.
bool scanner_name_is_ascii(const uint8_t *name, size_t length) {
  while (length >= sizeof(uint64_t)) {
    uint64_t word;
    memcpy(&word, name, sizeof(word));
    if (word & UINT64_C(0x8080808080808080))
      return false;
    name += sizeof(word);
    length -= sizeof(word);
  }
  while (length--) {
    if (*name++ & 0x80)
      return false;
  }
  return true;
}
