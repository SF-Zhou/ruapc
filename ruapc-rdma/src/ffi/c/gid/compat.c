#include "compat.h"
#include "ioctl.h"

int ruapc_ibv_query_gid(struct ibv_context *context, uint8_t port_num,
                        int index, union ibv_gid *gid)
{
	return ibv_query_gid(context, port_num, index, gid);
}

int ruapc_ibv_query_gid_entry(struct ibv_context *context, uint32_t driver_id,
			    uint8_t port_num, uint32_t index,
			    union ibv_gid *gid, uint32_t *gid_type)
{
	return ruapc_query_gid_entry(context->cmd_fd, driver_id, port_num, index,
				    gid->raw, gid_type);
}

int ruapc_ibv_query_gid_table(struct ibv_context *context, uint32_t driver_id,
			    void *entries, uint32_t max_entries,
			    uint32_t *num_entries)
{
	return ruapc_query_gid_table(context->cmd_fd, driver_id, entries,
				     max_entries, num_entries);
}
