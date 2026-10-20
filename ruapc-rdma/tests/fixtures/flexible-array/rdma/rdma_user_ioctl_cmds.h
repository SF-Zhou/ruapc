/* Keep the installed UAPI except for the modern flexible-array header layout.
 * Embedding this header before more fields is rejected by Clang under -Werror.
 */
#ifndef RUAPC_TEST_FLEXIBLE_ARRAY_IOCTL_CMDS_H
#define RUAPC_TEST_FLEXIBLE_ARRAY_IOCTL_CMDS_H

#define ib_uverbs_ioctl_hdr ruapc_test_installed_ioctl_hdr
#include_next <rdma/rdma_user_ioctl_cmds.h>
#undef ib_uverbs_ioctl_hdr

struct ib_uverbs_ioctl_hdr {
	__u16 length;
	__u16 object_id;
	__u16 method_id;
	__u16 num_attrs;
	__aligned_u64 reserved1;
	__u32 driver_id;
	__u32 reserved2;
	struct ib_uverbs_attr attrs[];
};

#endif
