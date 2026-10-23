/* Compatibility GID queries, independent of extended libibverbs APIs. */
#ifndef RUAPC_GID_COMPAT_H
#define RUAPC_GID_COMPAT_H

#include <infiniband/verbs.h>

int ruapc_ibv_query_gid(struct ibv_context *context, uint8_t port_num,
                        int index, union ibv_gid *gid);

/* Direct kernel queries return zero or a positive errno.
 * The driver ID query is implemented in driver.c. */
int ruapc_ibv_query_gid_driver_id(struct ibv_context *context,
                                uint32_t *driver_id);

int ruapc_ibv_query_gid_entry(struct ibv_context *context, uint32_t driver_id,
                            uint8_t port_num, uint32_t index,
                            union ibv_gid *gid, uint32_t *gid_type);

/* `entries` points to `max_entries` writable entries of 32 bytes each. */
int ruapc_ibv_query_gid_table(struct ibv_context *context, uint32_t driver_id,
                            void *entries, uint32_t max_entries,
                            uint32_t *num_entries);

#endif /* RUAPC_GID_COMPAT_H */
