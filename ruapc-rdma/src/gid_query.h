#ifndef RUAPC_GID_QUERY_H
#define RUAPC_GID_QUERY_H

#include <stdint.h>

/* Kept separate from verbs.h: some vendor headers and distro kernel headers
 * define conflicting versions of the rdma_driver_id enum. */
int ruapc_query_gid_entry(int cmd_fd, uint32_t driver_id, uint8_t port_num,
                         uint32_t index, unsigned char gid[16],
                         uint32_t *gid_type);

#endif
