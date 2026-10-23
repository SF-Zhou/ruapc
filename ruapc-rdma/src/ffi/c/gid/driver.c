/* Resolve the ioctl driver ID through the public RDMA netlink ABI.
 *
 * verbs_context::priv is not a public ABI, and a zero ioctl driver_id is
 * not a wildcard. GET_CHARDEV supplies the ID without depending on sysfs.
 * See Linux drivers/infiniband/core/{nldev,uverbs_main}.c.
 */
#include "compat.h"

#include <errno.h>
#include <limits.h>
#include <linux/netlink.h>
#include <poll.h>
#include <rdma/rdma_netlink.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <time.h>
#include <unistd.h>

#define RUAPC_NL_BUFFER_SIZE 65536
#define RUAPC_NL_TIMEOUT_MS 1000

struct ruapc_nl_result {
	const char *name;
	size_t name_len;
	uint32_t index;
	uint32_t driver_id;
	uint64_t chardev;
	bool has_index;
	bool has_driver_id;
	bool has_chardev;
};

static int ruapc_nl_parse_attrs(const struct nlmsghdr *msg,
			       struct ruapc_nl_result *result)
{
	const unsigned char *cursor = NLMSG_DATA(msg);
	size_t remaining = msg->nlmsg_len - NLMSG_HDRLEN;

	memset(result, 0, sizeof(*result));
	while (remaining) {
		struct nlattr attr;
		const void *data;
		size_t size;
		size_t step;

		if (remaining < NLA_HDRLEN)
			return EPROTO;
		memcpy(&attr, cursor, sizeof(attr));
		if (attr.nla_len < NLA_HDRLEN || attr.nla_len > remaining)
			return EPROTO;
		size = attr.nla_len - NLA_HDRLEN;
		step = NLA_ALIGN(attr.nla_len);
		if (step > remaining)
			return EPROTO;
		data = cursor + NLA_HDRLEN;

		switch (attr.nla_type & NLA_TYPE_MASK) {
		case RDMA_NLDEV_ATTR_DEV_NAME:
			if (result->name || !size ||
			    ((const char *)data)[size - 1] != '\0')
				return EPROTO;
			result->name = data;
			result->name_len = size;
			break;
		case RDMA_NLDEV_ATTR_DEV_INDEX:
			if (result->has_index || size != sizeof(result->index))
				return EPROTO;
			memcpy(&result->index, data, sizeof(result->index));
			result->has_index = true;
			break;
		case RDMA_NLDEV_ATTR_UVERBS_DRIVER_ID:
			/* The UAPI header comment says u64, but the kernel
			 * emits this attribute with nla_put_u32().
			 */
			if (result->has_driver_id ||
			    size != sizeof(result->driver_id))
				return EPROTO;
			memcpy(&result->driver_id, data, sizeof(result->driver_id));
			result->has_driver_id = true;
			break;
		case RDMA_NLDEV_ATTR_CHARDEV:
			if (result->has_chardev || size != sizeof(result->chardev))
				return EPROTO;
			memcpy(&result->chardev, data, sizeof(result->chardev));
			result->has_chardev = true;
			break;
		default:
			break;
		}
		cursor += step;
		remaining -= step;
	}
	return 0;
}

static int64_t ruapc_nl_now_ms(void)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now))
		return -1;
	return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static int ruapc_nl_receive(int fd, void *buffer, size_t *received,
			  int64_t deadline)
{
	for (;;) {
		struct sockaddr_nl sender = {};
		struct iovec iov = {
			.iov_base = buffer,
			.iov_len = RUAPC_NL_BUFFER_SIZE,
		};
		struct msghdr msg = {
			.msg_name = &sender,
			.msg_namelen = sizeof(sender),
			.msg_iov = &iov,
			.msg_iovlen = 1,
		};
		struct pollfd poll_fd = { .fd = fd, .events = POLLIN };
		int64_t now = ruapc_nl_now_ms();
		ssize_t len;
		int ret;

		if (now < 0)
			return errno;
		if (now >= deadline)
			return ETIMEDOUT;
		ret = poll(&poll_fd, 1, (int)(deadline - now));
		if (ret < 0) {
			if (errno == EINTR)
				continue;
			return errno;
		}
		if (!ret)
			return ETIMEDOUT;
		len = recvmsg(fd, &msg, MSG_DONTWAIT);
		if (len < 0) {
			if (errno == EINTR || errno == EAGAIN)
				continue;
			return errno;
		}
		if (msg.msg_namelen != sizeof(sender) ||
		    sender.nl_family != AF_NETLINK || sender.nl_pid != 0)
			return EPROTO;
		if (msg.msg_flags & MSG_TRUNC)
			return EMSGSIZE;
		if (!len)
			return EPROTO;
		*received = (size_t)len;
		return 0;
	}
}

static int ruapc_nl_query(int fd, struct nlmsghdr *request, const char *name,
			 struct ruapc_nl_result *result, int64_t deadline)
{
	struct sockaddr_nl kernel = { .nl_family = AF_NETLINK };
	void *buffer;
	int ret = 0;
	bool found = false;
	ssize_t sent;

	sent = sendto(fd, request, request->nlmsg_len, 0,
		      (struct sockaddr *)&kernel, sizeof(kernel));
	if (sent < 0)
		return errno;
	if ((size_t)sent != request->nlmsg_len)
		return EIO;
	buffer = malloc(RUAPC_NL_BUFFER_SIZE);
	if (!buffer)
		return ENOMEM;

	for (;;) {
		struct nlmsghdr *msg;
		size_t remaining = 0;

		ret = ruapc_nl_receive(fd, buffer, &remaining, deadline);
		if (ret)
			break;
		msg = buffer;
		while (remaining) {
			struct ruapc_nl_result entry;
			size_t step;

			if (remaining < NLMSG_HDRLEN || msg->nlmsg_len < NLMSG_HDRLEN ||
			    msg->nlmsg_len > remaining) {
				ret = EPROTO;
				goto out;
			}
			step = NLMSG_ALIGN(msg->nlmsg_len);
			if (step > remaining || msg->nlmsg_seq != request->nlmsg_seq) {
				ret = EPROTO;
				goto out;
			}
			if (msg->nlmsg_flags & NLM_F_DUMP_INTR) {
				ret = EAGAIN;
				goto out;
			}
			if (msg->nlmsg_type == NLMSG_ERROR) {
				struct nlmsgerr error;

				if (msg->nlmsg_len < NLMSG_LENGTH(sizeof(error))) {
					ret = EPROTO;
					goto out;
				}
				memcpy(&error, NLMSG_DATA(msg), sizeof(error));
				if (error.error) {
					ret = error.error < 0 && error.error != INT_MIN
						? -error.error : EPROTO;
					goto out;
				}
			} else if (msg->nlmsg_type == NLMSG_DONE) {
				int error = 0;
				size_t len = msg->nlmsg_len - NLMSG_HDRLEN;

				if (len && len < sizeof(error)) {
					ret = EPROTO;
					goto out;
				}
				if (len)
					memcpy(&error, NLMSG_DATA(msg), sizeof(error));
				ret = error < 0 && error != INT_MIN ? -error
					: error ? EPROTO : found ? 0 : ENODEV;
				goto out;
			} else if (msg->nlmsg_type == request->nlmsg_type) {
				ret = ruapc_nl_parse_attrs(msg, &entry);
				if (ret)
					goto out;
				if (!name || (entry.name &&
				    entry.name_len == strlen(name) + 1 &&
				    !memcmp(entry.name, name, entry.name_len))) {
					if (found) {
						ret = EPROTO;
						goto out;
					}
					*result = entry;
					/* Names point into the receive buffer and
					 * must not escape this function.
					 */
					result->name = NULL;
					result->name_len = 0;
					found = true;
					if (!(request->nlmsg_flags & NLM_F_DUMP))
						goto out;
				}
			} else {
				ret = EPROTO;
				goto out;
			}
			remaining -= step;
			msg = (struct nlmsghdr *)((unsigned char *)msg + step);
		}
	}
out:
	free(buffer);
	return ret;
}

int ruapc_ibv_query_gid_driver_id(struct ibv_context *context, uint32_t *driver_id)
{
	struct sockaddr_nl local = { .nl_family = AF_NETLINK };
	struct nlmsghdr request = {
		.nlmsg_len = NLMSG_HDRLEN,
		.nlmsg_type = RDMA_NL_GET_TYPE(RDMA_NL_NLDEV, RDMA_NLDEV_CMD_GET),
		.nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP,
		.nlmsg_seq = 1,
	};
	struct {
		struct nlmsghdr hdr;
		struct nlattr index_attr;
		uint32_t index;
		struct nlattr type_attr;
		char type[NLA_ALIGN(sizeof("uverbs"))];
	} chardev_request = {
		.hdr = {
			.nlmsg_len = sizeof(chardev_request),
			.nlmsg_type = RDMA_NL_GET_TYPE(RDMA_NL_NLDEV,
						     RDMA_NLDEV_CMD_GET_CHARDEV),
			.nlmsg_flags = NLM_F_REQUEST,
			.nlmsg_seq = 2,
		},
		.index_attr = {
			.nla_len = NLA_HDRLEN + sizeof(uint32_t),
			.nla_type = RDMA_NLDEV_ATTR_DEV_INDEX,
		},
		.type_attr = {
			.nla_len = NLA_HDRLEN + sizeof("uverbs"),
			.nla_type = RDMA_NLDEV_ATTR_CHARDEV_TYPE,
		},
		.type = "uverbs",
	};
	struct ruapc_nl_result result = {};
	struct stat status;
	int64_t deadline;
	int fd;
	int ret;

	if (fstat(context->cmd_fd, &status))
		return errno;
	if (!S_ISCHR(status.st_mode))
		return ENODEV;
	deadline = ruapc_nl_now_ms();
	if (deadline < 0)
		return errno;
	deadline += RUAPC_NL_TIMEOUT_MS;
	fd = socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_RDMA);
	if (fd < 0)
		return errno;
	if (bind(fd, (struct sockaddr *)&local, sizeof(local))) {
		ret = errno;
		goto out;
	}
	ret = ruapc_nl_query(fd, &request, context->device->name, &result, deadline);
	if (ret)
		goto out;
	if (!result.has_index) {
		ret = EPROTO;
		goto out;
	}
	chardev_request.index = result.index;
	ret = ruapc_nl_query(fd, &chardev_request.hdr, NULL, &result, deadline);
	if (ret)
		goto out;
	if (!result.has_driver_id || !result.has_chardev) {
		ret = EOPNOTSUPP;
		goto out;
	}
	/* The kernel uses huge_encode_dev(), whose layout differs from libc's
	 * dev_t for large major numbers. Match decoded device identity.
	 */
	if (result.chardev > UINT32_MAX ||
	    ((result.chardev >> 8) & 0xfff) != major(status.st_rdev) ||
	    ((result.chardev & 0xff) | ((result.chardev >> 12) & 0xfff00)) !=
		    minor(status.st_rdev)) {
		ret = ENODEV;
		goto out;
	}
	*driver_id = result.driver_id;
out:
	close(fd);
	return ret;
}
