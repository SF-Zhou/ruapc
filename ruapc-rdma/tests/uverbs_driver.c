/* Exercise the netlink parser without requiring a device or kernel support. */
#include "../src/uverbs_driver.c"

#include <assert.h>

union message_buffer {
	struct nlmsghdr hdr;
	unsigned char bytes[256];
};

static void reset_message(union message_buffer *buffer)
{
	memset(buffer, 0, sizeof(*buffer));
	buffer->hdr.nlmsg_len = NLMSG_HDRLEN;
}

static void append_attr(union message_buffer *buffer, uint16_t type,
			const void *data, size_t size)
{
	struct nlattr attr = {
		.nla_len = NLA_HDRLEN + size,
		.nla_type = type,
	};
	unsigned char *cursor = buffer->bytes + buffer->hdr.nlmsg_len;

	assert(buffer->hdr.nlmsg_len + NLA_ALIGN(attr.nla_len) <= sizeof(*buffer));
	memcpy(cursor, &attr, sizeof(attr));
	memcpy(cursor + NLA_HDRLEN, data, size);
	buffer->hdr.nlmsg_len += NLA_ALIGN(attr.nla_len);
}

static void test_valid_attributes(void)
{
	union message_buffer buffer;
	struct ruapc_nl_result result;
	const char name[] = "mlx5_0";
	uint32_t index = 123;
	uint32_t driver_id = 1;
	uint64_t chardev = 0x12345678;
	uint32_t unknown = 17;

	reset_message(&buffer);
	append_attr(&buffer, RDMA_NLDEV_ATTR_DEV_NAME, name, sizeof(name));
	append_attr(&buffer, RDMA_NLDEV_ATTR_DEV_INDEX, &index, sizeof(index));
	append_attr(&buffer, RDMA_NLDEV_ATTR_UVERBS_DRIVER_ID,
		    &driver_id, sizeof(driver_id));
	append_attr(&buffer, RDMA_NLDEV_ATTR_CHARDEV, &chardev, sizeof(chardev));
	append_attr(&buffer, 0x3fff, &unknown, sizeof(unknown));
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == 0);
	assert(result.has_index && result.index == index);
	assert(result.has_driver_id && result.driver_id == driver_id);
	assert(result.has_chardev && result.chardev == chardev);
	assert(result.name_len == sizeof(name) && !strcmp(result.name, name));

	/* Old drivers may omit DRIVER_ID without producing a malformed reply. */
	reset_message(&buffer);
	append_attr(&buffer, RDMA_NLDEV_ATTR_DEV_INDEX, &index, sizeof(index));
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == 0);
	assert(result.has_index && !result.has_driver_id);
	assert(!result.has_chardev && !result.name);
}

static void test_malformed_attributes(void)
{
	union message_buffer buffer;
	struct ruapc_nl_result result;
	struct nlattr *attr = (struct nlattr *)(buffer.bytes + NLMSG_HDRLEN);
	uint32_t value = 37;
	uint64_t wrong_width = 37;

	reset_message(&buffer);
	buffer.hdr.nlmsg_len++;
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);
	reset_message(&buffer);
	append_attr(&buffer, RDMA_NLDEV_ATTR_UVERBS_DRIVER_ID, &value, sizeof(value));
	attr->nla_len = NLA_HDRLEN - 1;
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);
	attr->nla_len = 9;
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);
	attr->nla_len = 7;
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);
	attr->nla_len = 8;
	append_attr(&buffer, RDMA_NLDEV_ATTR_UVERBS_DRIVER_ID, &value, sizeof(value));
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);

	/* Despite the UAPI header's comment, DRIVER_ID really is a u32. */
	reset_message(&buffer);
	append_attr(&buffer, RDMA_NLDEV_ATTR_UVERBS_DRIVER_ID,
		    &wrong_width, sizeof(wrong_width));
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);
	reset_message(&buffer);
	append_attr(&buffer, RDMA_NLDEV_ATTR_DEV_NAME, "bad", 3);
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);
	reset_message(&buffer);
	append_attr(&buffer, RDMA_NLDEV_ATTR_DEV_NAME, "good", 5);
	buffer.hdr.nlmsg_len--; /* Missing attribute alignment padding. */
	assert(ruapc_nl_parse_attrs(&buffer.hdr, &result) == EPROTO);
}

static uint32_t next_random(uint32_t *state)
{
	*state = *state * UINT32_C(1664525) + UINT32_C(1013904223);
	return *state;
}

static void test_arbitrary_payloads(void)
{
	union message_buffer buffer;
	struct ruapc_nl_result result;
	uint32_t state = 1;
	unsigned i;

	/* Deterministic bounded input coverage, also useful under ASan/UBSan. */
	for (i = 0; i < 200000; i++) {
		size_t len = next_random(&state) %
			     (sizeof(buffer.bytes) - NLMSG_HDRLEN + 1);
		size_t j;

		reset_message(&buffer);
		for (j = 0; j < len; j++)
			buffer.bytes[NLMSG_HDRLEN + j] = next_random(&state) >> 24;
		buffer.hdr.nlmsg_len += len;
		(void)ruapc_nl_parse_attrs(&buffer.hdr, &result);
	}
}

int main(void)
{
	test_valid_attributes();
	test_malformed_attributes();
	test_arbitrary_payloads();
	return 0;
}
