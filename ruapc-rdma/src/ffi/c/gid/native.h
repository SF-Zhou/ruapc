/* GID APIs from the installed libibverbs; selected by ibv-query-gid-ex. */
#ifndef RUAPC_GID_NATIVE_H
#define RUAPC_GID_NATIVE_H

#include <infiniband/verbs.h>

/* Return zero on success or a positive errno on failure. */
int ruapc_ibv_query_gid_ex(struct ibv_context *context, uint32_t port_num,
                         uint32_t index, struct ibv_gid_entry *entry);

/* Return the populated entry count on success or a negative errno on failure. */
ssize_t ruapc_ibv_query_gid_table_native(struct ibv_context *context,
                                       struct ibv_gid_entry *entries,
                                       size_t max_entries);

#endif /* RUAPC_GID_NATIVE_H */
