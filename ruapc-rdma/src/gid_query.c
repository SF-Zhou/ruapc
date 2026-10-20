/* Query GIDs without requiring the libibverbs ibv_query_gid_ex symbol.
 *
 * The ioctl envelope predates Ubuntu 20.04. Both GID methods and their
 * responses were added in Linux 5.10, so keep their small, stable UAPI
 * definition private instead of requiring newer installed kernel headers.
 *
 * Reference: rdma-core b63b7bfe5d5b0adb97b973c71723adf2323afa2c,
 * libibverbs/cmd_device.c and kernel-headers/rdma/ib_user_ioctl_{cmds,verbs}.h.
 */
#include "gid_query.h"

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/ioctl.h>
#include <rdma/rdma_user_ioctl_cmds.h>

enum {
	RUAPC_UVERBS_OBJECT_DEVICE = 0,
	RUAPC_UVERBS_METHOD_QUERY_GID_TABLE = 5,
	RUAPC_UVERBS_METHOD_QUERY_GID_ENTRY = 6,
	RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_PORT = 0,
	RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_GID_INDEX = 1,
	RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_FLAGS = 2,
	RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_RESP_ENTRY = 3,
	RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_ENTRY_SIZE = 0,
	RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_FLAGS = 1,
	RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_RESP_ENTRIES = 2,
	RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_RESP_NUM_ENTRIES = 3,
};

struct ruapc_uverbs_gid_entry {
	__aligned_u64 gid[2];
	__u32 gid_index;
	__u32 port_num;
	__u32 gid_type;
	__u32 netdev_ifindex;
};

struct ruapc_uverbs_command {
	/* Newer headers end ib_uverbs_ioctl_hdr with attrs[], so it cannot
	 * precede another member. Spell out its fixed prefix and verify the
	 * layout below; this also works with older headers using attrs[0]. */
	__u16 length;
	__u16 object_id;
	__u16 method_id;
	__u16 num_attrs;
	__aligned_u64 reserved1;
	__u32 driver_id;
	__u32 reserved2;
	struct ib_uverbs_attr attrs[4];
};

#define CHECK_HEADER_OFFSET(field) \
	_Static_assert(offsetof(struct ruapc_uverbs_command, field) == \
		       offsetof(struct ib_uverbs_ioctl_hdr, field), \
		       "Unexpected ioctl header offset: " #field)
CHECK_HEADER_OFFSET(length);
CHECK_HEADER_OFFSET(object_id);
CHECK_HEADER_OFFSET(method_id);
CHECK_HEADER_OFFSET(num_attrs);
CHECK_HEADER_OFFSET(reserved1);
CHECK_HEADER_OFFSET(driver_id);
CHECK_HEADER_OFFSET(reserved2);
CHECK_HEADER_OFFSET(attrs);
#undef CHECK_HEADER_OFFSET

_Static_assert(sizeof(struct ruapc_uverbs_gid_entry) == 32,
	       "GID response must match the kernel UAPI");
_Static_assert(offsetof(struct ruapc_uverbs_gid_entry, gid_type) == 24,
	       "GID type must match the kernel UAPI");
_Static_assert(sizeof(struct ib_uverbs_ioctl_hdr) == 24,
	       "Unexpected ioctl header layout");
_Static_assert(sizeof(struct ib_uverbs_attr) == 16,
	       "Unexpected ioctl attribute layout");
_Static_assert(offsetof(struct ruapc_uverbs_command, attrs) == 24,
	       "Attributes must immediately follow the ioctl header");
_Static_assert(sizeof(struct ruapc_uverbs_command) ==
	       sizeof(struct ib_uverbs_ioctl_hdr) + 4 * sizeof(struct ib_uverbs_attr),
	       "Command must contain exactly one header and four attributes");

int ruapc_query_gid_entry(int cmd_fd, uint32_t driver_id, uint8_t port_num,
			 uint32_t index, unsigned char gid[16],
			 uint32_t *gid_type)
{
	struct ruapc_uverbs_gid_entry entry = {0};
	struct ruapc_uverbs_command cmd = {
		.length = sizeof(cmd),
		.object_id = RUAPC_UVERBS_OBJECT_DEVICE,
		.method_id = RUAPC_UVERBS_METHOD_QUERY_GID_ENTRY,
		.num_attrs = 4,
		.driver_id = driver_id,
		.attrs = {
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_PORT,
				.len = sizeof(__u64),
				.flags = UVERBS_ATTR_F_MANDATORY,
				.data = port_num,
			},
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_GID_INDEX,
				.len = sizeof(__u64),
				.flags = UVERBS_ATTR_F_MANDATORY,
				.data = index,
			},
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_FLAGS,
				.len = sizeof(__u32),
				.flags = UVERBS_ATTR_F_MANDATORY,
				/* Zero flags, including on big-endian hosts. */
				.data = 0,
			},
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_ENTRY_RESP_ENTRY,
				.len = sizeof(entry),
				.flags = UVERBS_ATTR_F_MANDATORY,
				.data = (uintptr_t)&entry,
			},
		},
	};

	/* A generic core command still requires the actual driver's ID.
	 * All storage remains live until this synchronous ioctl completes. */
	if (ioctl(cmd_fd, RDMA_VERBS_IOCTL, &cmd) < 0)
		return errno;

	memcpy(gid, entry.gid, sizeof(entry.gid));
	*gid_type = entry.gid_type;
	return 0;
}

int ruapc_query_gid_table(int cmd_fd, uint32_t driver_id, void *entries,
			  uint32_t max_entries, uint32_t *num_entries)
{
	uint64_t count = 0;
	struct ruapc_uverbs_command cmd;

	/* `len` is a `__u16` in the UAPI attribute; a larger array would be
	 * silently truncated into an invalid request. */
	if (!entries || !max_entries ||
	    max_entries > UINT16_MAX / sizeof(struct ruapc_uverbs_gid_entry))
		return EINVAL;

	cmd = (struct ruapc_uverbs_command){
		.length = sizeof(cmd),
		.object_id = RUAPC_UVERBS_OBJECT_DEVICE,
		.method_id = RUAPC_UVERBS_METHOD_QUERY_GID_TABLE,
		.num_attrs = 4,
		.driver_id = driver_id,
		.attrs = {
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_ENTRY_SIZE,
				.len = sizeof(__u64),
				.flags = UVERBS_ATTR_F_MANDATORY,
				.data = sizeof(struct ruapc_uverbs_gid_entry),
			},
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_FLAGS,
				.len = sizeof(__u32),
				.flags = UVERBS_ATTR_F_MANDATORY,
				/* Zero flags, including on big-endian hosts. */
				.data = 0,
			},
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_RESP_ENTRIES,
				/*
				 * The kernel writes into the caller's array
				 * directly; keep the whole array live until this
				 * synchronous ioctl completes.
				 */
				.len = max_entries *
				       sizeof(struct ruapc_uverbs_gid_entry),
				.flags = UVERBS_ATTR_F_MANDATORY,
				.data = (uintptr_t)entries,
			},
			{
				.attr_id = RUAPC_UVERBS_ATTR_QUERY_GID_TABLE_RESP_NUM_ENTRIES,
				.len = sizeof(__u64),
				.flags = UVERBS_ATTR_F_MANDATORY,
				.data = (uintptr_t)&count,
			},
		},
	};

	if (ioctl(cmd_fd, RDMA_VERBS_IOCTL, &cmd) < 0)
		return errno;

	if (count > UINT32_MAX)
		return EOVERFLOW;
	*num_entries = (uint32_t)count;
	return 0;
}
