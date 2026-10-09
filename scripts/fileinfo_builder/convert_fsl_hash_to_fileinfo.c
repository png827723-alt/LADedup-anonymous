#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../../tools/fsl/fs-hasher-0.9.5/libhashfile.h"

struct file_entry {
    char *path;
    uint64_t file_size;
    double heat;
    double heat_dsize;
    uint64_t num_chunks;
    uint64_t cap_chunks;
    uint32_t *chunk_ids;
    uint32_t *chunk_sizes;
};

struct unique_chunk {
    char *fp;
    uint32_t size;
};

struct chunk_bucket_entry {
    size_t idx;
    struct chunk_bucket_entry *next;
};

struct unique_chunk_map {
    struct unique_chunk *items;
    size_t len;
    size_t cap;
    struct chunk_bucket_entry **buckets;
    size_t bucket_count;
};

static void die(const char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(1);
}

static void die_errno(const char *msg) {
    fprintf(stderr, "%s: %s\n", msg, strerror(errno));
    exit(1);
}

static void *xmalloc(size_t sz) {
    void *p = malloc(sz);
    if (!p) {
        die("out of memory");
    }
    return p;
}

static void *xrealloc(void *ptr, size_t sz) {
    void *p = realloc(ptr, sz);
    if (!p) {
        die("out of memory");
    }
    return p;
}

static char *xstrdup(const char *s) {
    size_t n = strlen(s);
    char *p = xmalloc(n + 1);
    memcpy(p, s, n + 1);
    return p;
}

static uint64_t hash_string64(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)(*s++);
        h *= 1099511628211ULL;
    }
    return h;
}

static void unique_chunk_map_init(struct unique_chunk_map *m) {
    m->bucket_count = 1u << 22;
    m->buckets = calloc(m->bucket_count, sizeof(struct chunk_bucket_entry *));
    if (!m->buckets) {
        die("out of memory");
    }
}

static void ensure_file_chunk_cap(struct file_entry *fe, uint64_t need) {
    if (fe->cap_chunks >= need) {
        return;
    }
    uint64_t new_cap = fe->cap_chunks ? fe->cap_chunks : 16;
    while (new_cap < need) {
        new_cap *= 2;
    }
    fe->chunk_ids = xrealloc(fe->chunk_ids, (size_t)new_cap * sizeof(uint32_t));
    fe->chunk_sizes = xrealloc(fe->chunk_sizes, (size_t)new_cap * sizeof(uint32_t));
    fe->cap_chunks = new_cap;
}

static int cmp_file_path(const void *a, const void *b) {
    const struct file_entry *fa = a;
    const struct file_entry *fb = b;
    return strcmp(fa->path, fb->path);
}

static void fp_to_hex(const uint8_t *hash, int hash_size_bytes, char *out_hex) {
    static const char hexdigits[] = "0123456789abcdef";
    int i;
    for (i = 0; i < hash_size_bytes; ++i) {
        out_hex[2 * i] = hexdigits[(hash[i] >> 4) & 0xF];
        out_hex[2 * i + 1] = hexdigits[hash[i] & 0xF];
    }
    out_hex[2 * hash_size_bytes] = '\0';
}

static uint32_t unique_chunk_id(struct unique_chunk_map *m, const char *fp, uint32_t size) {
    uint64_t hv = hash_string64(fp);
    size_t bucket = (size_t)(hv % m->bucket_count);
    struct chunk_bucket_entry *it = m->buckets[bucket];
    while (it) {
        if (strcmp(m->items[it->idx].fp, fp) == 0) {
            if (m->items[it->idx].size != size) {
                fprintf(stderr, "chunk fingerprint size mismatch for %s: %u vs %u\n",
                        fp, m->items[it->idx].size, size);
                exit(1);
            }
            return (uint32_t)(it->idx + 1);
        }
        it = it->next;
    }
    if (m->len == m->cap) {
        size_t new_cap = m->cap ? m->cap * 2 : 1024;
        m->items = xrealloc(m->items, new_cap * sizeof(struct unique_chunk));
        m->cap = new_cap;
    }
    m->items[m->len].fp = xstrdup(fp);
    m->items[m->len].size = size;
    {
        struct chunk_bucket_entry *new_entry = xmalloc(sizeof(struct chunk_bucket_entry));
        new_entry->idx = m->len;
        new_entry->next = m->buckets[bucket];
        m->buckets[bucket] = new_entry;
    }
    m->len += 1;
    return (uint32_t)m->len;
}

static char *make_rel_path(const char *root, const char *full) {
    size_t root_len = strlen(root);
    if (strncmp(root, full, root_len) == 0) {
        const char *rel = full + root_len;
        while (*rel == '/') {
            rel++;
        }
        return xstrdup(rel);
    }
    return xstrdup(full);
}

static void json_escape(FILE *out, const char *s) {
    const unsigned char *p = (const unsigned char *)s;
    fputc('"', out);
    while (*p) {
        switch (*p) {
        case '\\':
            fputs("\\\\", out);
            break;
        case '"':
            fputs("\\\"", out);
            break;
        case '\b':
            fputs("\\b", out);
            break;
        case '\f':
            fputs("\\f", out);
            break;
        case '\n':
            fputs("\\n", out);
            break;
        case '\r':
            fputs("\\r", out);
            break;
        case '\t':
            fputs("\\t", out);
            break;
        default:
            if (*p < 0x20) {
                fprintf(out, "\\u%04x", *p);
            } else {
                fputc(*p, out);
            }
            break;
        }
        p++;
    }
    fputc('"', out);
}

static uint32_t infer_chunk_bytes(struct hashfile_handle *hf) {
    char method_buf[1024];
    struct fixed_chnking_params fixed_params;
    struct var_chnking_params var_params;
    if (hashfile_chunking_method(hf) == FIXED) {
        if (hashfile_fxd_chunking_params(hf, &fixed_params) == 0) {
            return fixed_params.chunk_size;
        }
    } else if (hashfile_chunking_method(hf) == VARIABLE) {
        if (hashfile_var_chunking_params(hf, &var_params) == 0) {
            if (var_params.algo == RABIN || var_params.algo == SIMPLE_MATCH) {
                if (var_params.algo_params.rabin_params.bits_to_compare > 0 &&
                    var_params.algo_params.rabin_params.bits_to_compare < 31) {
                    return (uint32_t)(1u << var_params.algo_params.rabin_params.bits_to_compare);
                }
            }
            if (var_params.min_csize > 0 && var_params.max_csize >= var_params.min_csize) {
                return (uint32_t)((var_params.min_csize + var_params.max_csize) / 2);
            }
        }
    }
    if (hashfile_chunking_method_str(hf, method_buf, (int)sizeof(method_buf)) == 0) {
        char *bits = strstr(method_buf, "bits=");
        if (bits) {
            int n = atoi(bits + 5);
            if (n > 0 && n < 31) {
                return (uint32_t)(1u << n);
            }
        }
    }
    return 8192;
}

static void usage(const char *argv0) {
    fprintf(stderr,
            "Usage: %s --input-hash <file.hash[.anon]> --output-json <file.json> [--zipf-s <s>] [--pretty]\n",
            argv0);
}

int main(int argc, char **argv) {
    const char *input_hash = NULL;
    const char *output_json = NULL;
    double zipf_s = 0.7;
    int pretty = 0;
    int i;
    struct hashfile_handle *hf;
    const struct chunk_info *ci;
    struct unique_chunk_map uniq = {0};
    struct file_entry *files = NULL;
    size_t files_len = 0;
    size_t files_cap = 0;
    uint64_t total_size = 0;
    uint32_t chunk_bytes;
    const char *root_path;
    int hash_size_bytes;
    FILE *out;

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--input-hash") == 0 && i + 1 < argc) {
            input_hash = argv[++i];
        } else if (strcmp(argv[i], "--output-json") == 0 && i + 1 < argc) {
            output_json = argv[++i];
        } else if (strcmp(argv[i], "--zipf-s") == 0 && i + 1 < argc) {
            zipf_s = atof(argv[++i]);
        } else if (strcmp(argv[i], "--pretty") == 0) {
            pretty = 1;
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (!input_hash || !output_json) {
        usage(argv[0]);
        return 2;
    }
    if (zipf_s <= 0.0) {
        die("--zipf-s must be positive");
    }

    hf = hashfile_open((char *)input_hash);
    if (!hf) {
        die_errno("open hash file");
    }
    unique_chunk_map_init(&uniq);
    root_path = hashfile_rootpath(hf);
    hash_size_bytes = (int)(hashfile_hash_size(hf) / 8);
    if (hash_size_bytes <= 0 || hash_size_bytes > 64) {
        die("unsupported hash size");
    }
    chunk_bytes = infer_chunk_bytes(hf);

    while (1) {
        int ret = hashfile_next_file(hf);
        struct file_entry fe;
        uint64_t chunk_count = 0;
        if (ret < 0) {
            die_errno("iterate hash file");
        }
        if (ret == 0) {
            break;
        }
        memset(&fe, 0, sizeof(fe));
        fe.path = make_rel_path(root_path, hashfile_curfile_path(hf));
        fe.file_size = hashfile_curfile_size(hf);
        total_size += fe.file_size;

        while ((ci = hashfile_next_chunk(hf)) != NULL) {
            char fp_hex[129];
            uint32_t cid;
            uint32_t csize = (uint32_t)ci->size;
            fp_to_hex(ci->hash, hash_size_bytes, fp_hex);
            cid = unique_chunk_id(&uniq, fp_hex, csize);
            ensure_file_chunk_cap(&fe, chunk_count + 1);
            fe.chunk_ids[chunk_count] = cid;
            fe.chunk_sizes[chunk_count] = csize;
            chunk_count++;
        }
        fe.num_chunks = chunk_count;

        if (files_len == files_cap) {
            size_t new_cap = files_cap ? files_cap * 2 : 1024;
            files = xrealloc(files, new_cap * sizeof(struct file_entry));
            files_cap = new_cap;
        }
        files[files_len++] = fe;
    }
    hashfile_close(hf);

    qsort(files, files_len, sizeof(struct file_entry), cmp_file_path);
    for (i = 0; i < (int)files_len; ++i) {
        double rank = (double)(i + 1);
        files[i].heat = 1.0 / pow(rank, zipf_s);
        files[i].heat_dsize = files[i].file_size > 0 ? files[i].heat / (double)files[i].file_size : 0.0;
    }

    out = fopen(output_json, "w");
    if (!out) {
        die_errno("open output json");
    }

    fprintf(out, "{%s", pretty ? "\n" : "");
    fprintf(out, pretty ? "  \"format_version\": \"mean_go_v1\",\n" : "\"format_version\":\"mean_go_v1\",");
    fprintf(out, pretty ? "  \"chunk_bytes\": %u,\n" : "\"chunk_bytes\":%u,", chunk_bytes);
    fprintf(out, pretty ? "  \"total_size\": %" PRIu64 ",\n" : "\"total_size\":%" PRIu64 ",", total_size);

    fprintf(out, pretty ? "  \"uni_fingerprint\": [\n" : "\"uni_fingerprint\":[");
    for (i = 0; i < (int)uniq.len; ++i) {
        if (pretty) {
            fprintf(out, "    ");
        }
        json_escape(out, uniq.items[i].fp);
        if ((size_t)(i + 1) != uniq.len) {
            fprintf(out, ",");
        }
        fprintf(out, "%s", pretty ? "\n" : "");
    }
    fprintf(out, pretty ? "  ],\n" : "],");

    fprintf(out, pretty ? "  \"uni_size\": [\n" : "\"uni_size\":[");
    for (i = 0; i < (int)uniq.len; ++i) {
        if (pretty) {
            fprintf(out, "    ");
        }
        fprintf(out, "%u", uniq.items[i].size);
        if ((size_t)(i + 1) != uniq.len) {
            fprintf(out, ",");
        }
        fprintf(out, "%s", pretty ? "\n" : "");
    }
    fprintf(out, pretty ? "  ],\n" : "],");

    fprintf(out, pretty ? "  \"files\": [\n" : "\"files\":[");
    for (i = 0; i < (int)files_len; ++i) {
        uint64_t j;
        if (pretty) {
            fprintf(out, "    {\n");
            fprintf(out, "      \"path\": ");
        } else {
            fprintf(out, "{");
            fprintf(out, "\"path\":");
        }
        json_escape(out, files[i].path);
        fprintf(out, pretty ? ",\n      \"chunk_ids\": [" : ",\"chunk_ids\":[");
        for (j = 0; j < files[i].num_chunks; ++j) {
            if (j > 0) {
                fprintf(out, ",");
            }
            fprintf(out, "%u", files[i].chunk_ids[j]);
        }
        fprintf(out, pretty ? "],\n      \"chunk_sizes\": [" : "],\"chunk_sizes\":[");
        for (j = 0; j < files[i].num_chunks; ++j) {
            if (j > 0) {
                fprintf(out, ",");
            }
            fprintf(out, "%u", files[i].chunk_sizes[j]);
        }
        fprintf(out,
                pretty ? "],\n      \"heat\": %.17g,\n      \"heat_dsize\": %.17g\n    }"
                       : "],\"heat\":%.17g,\"heat_dsize\":%.17g}",
                files[i].heat, files[i].heat_dsize);
        if ((size_t)(i + 1) != files_len) {
            fprintf(out, ",");
        }
        fprintf(out, "%s", pretty ? "\n" : "");
    }
    fprintf(out, pretty ? "  ]\n}\n" : "]}");

    fclose(out);
    fprintf(stderr,
            "saved %s (files=%zu, unique_chunks=%zu, total_size=%" PRIu64 ", chunk_bytes=%u)\n",
            output_json, files_len, uniq.len, total_size, chunk_bytes);
    return 0;
}
