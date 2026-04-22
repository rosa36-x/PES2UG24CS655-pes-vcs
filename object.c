// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <errno.h>


// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Convert type enum → string
    const char *type_str;
    switch (type) {
        case OBJ_BLOB: type_str = "blob"; break;
        case OBJ_TREE: type_str = "tree"; break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }

    // Step 2: Build header "<type> <size>\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    // Step 3: Allocate full object = header + data
    size_t total_size = header_len + len;
    unsigned char *full = malloc(total_size);
    if (!full) return -1;

    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // Step 4: Compute hash of full object
    compute_hash(full, total_size, id_out);

    // Step 5: Deduplication check
    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // Step 6: Build object path
    char path[512];
    object_path(id_out, path, sizeof(path));

    // Extract directory path (.pes/objects/XX)
    char dir[512];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (!slash) {
        free(full);
        return -1;
    }
    *slash = '\0';

    // Step 7: Ensure directories exist
    if (mkdir(".pes", 0755) < 0 && errno != EEXIST) {
        free(full);
        return -1;
    }
    if (mkdir(OBJECTS_DIR, 0755) < 0 && errno != EEXIST) {
        free(full);
        return -1;
    }
    if (mkdir(dir, 0755) < 0 && errno != EEXIST) {
        free(full);
        return -1;
    }

    // Step 8: Write to temp file
    char tmp_path[512];
    if (snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path) >= (int)sizeof(tmp_path)) {
        free(full);
        return -1;
    }
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full);
        return -1;
    }

    // Handle partial writes
    ssize_t total_written = 0;
    while (total_written < (ssize_t)total_size) {
        ssize_t w = write(fd, full + total_written, total_size - total_written);
        if (w <= 0) {
            close(fd);
            free(full);
            return -1;
        }
        total_written += w;
    }

    // Step 9: fsync temp file
    if (fsync(fd) < 0) {
        close(fd);
        free(full);
        return -1;
    }

    close(fd);

    // Step 10: Atomic rename
    if (rename(tmp_path, path) < 0) {
        free(full);
        return -1;
    }

    // Step 11: fsync directory
    int dir_fd = open(dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    free(full);
    return 0;
}


int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Build file path
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Open file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // Step 3: Get file size
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    rewind(f);

    // Step 4: Read full file
    unsigned char *buf = malloc(size);
    if (!buf) {
        fclose(f);
        return -1;
    }

    if (fread(buf, 1, size, f) != (size_t)size) {
        fclose(f);
        free(buf);
        return -1;
    }
    fclose(f);

    // Step 5: Verify hash (integrity check)
    ObjectID computed;
    compute_hash(buf, size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(buf);
        return -1;
    }

    // Step 6: Find header-data separator
    char *null_pos = memchr(buf, '\0', size);
    if (!null_pos) {
        free(buf);
        return -1;
    }

    // Step 7: Parse header
    char type_str[16];
    size_t data_size;

    if (sscanf((char *)buf, "%15s %zu", type_str, &data_size) != 2) {
        free(buf);
        return -1;
    }

    // Step 8: Set type
    if (strcmp(type_str, "blob") == 0) {
        *type_out = OBJ_BLOB;
    } else if (strcmp(type_str, "tree") == 0) {
        *type_out = OBJ_TREE;
    } else if (strcmp(type_str, "commit") == 0) {
        *type_out = OBJ_COMMIT;
    } else {
        free(buf);
        return -1;
    }

    // Step 9: Extract data
    unsigned char *data_start = (unsigned char *)null_pos + 1;

    // Validate size
    if ((size_t)(buf + size - data_start) != data_size) {
        free(buf);
        return -1;
    }

    void *data = malloc(data_size);
    if (!data) {
        free(buf);
        return -1;
    }

    memcpy(data, data_start, data_size);

    *data_out = data;
    *len_out = data_size;

    free(buf);
    return 0;
}

