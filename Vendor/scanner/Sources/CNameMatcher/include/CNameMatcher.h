#ifndef C_NAME_MATCHER_H
#define C_NAME_MATCHER_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct scanner_name_pattern scanner_name_pattern;
// The returned immutable pattern can be shared by all scan workers.
scanner_name_pattern *scanner_name_pattern_create(const uint8_t *bytes, size_t length);
void scanner_name_pattern_destroy(scanner_name_pattern *pattern);
bool scanner_name_pattern_matches(const scanner_name_pattern *pattern,
                                  const uint8_t *name, size_t length);
bool scanner_name_is_ascii(const uint8_t *name, size_t length);
#endif
