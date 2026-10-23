/* Compile through verbs.h so its inline entry-size ABI handling is honored. */
#include "native.h"

int ruapc_ibv_query_gid_ex(struct ibv_context *context, uint32_t port_num,
                         uint32_t index, struct ibv_gid_entry *entry)
{
	return ibv_query_gid_ex(context, port_num, index, entry, 0);
}

ssize_t ruapc_ibv_query_gid_table_native(struct ibv_context *context,
                                       struct ibv_gid_entry *entries,
                                       size_t max_entries)
{
	return ibv_query_gid_table(context, entries, max_entries, 0);
}
