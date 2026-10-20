/* Check the actual ioctl wire requests and simulate success/error replies. */
#include <assert.h>
#include <stdarg.h>
#include <sys/ioctl.h>

static int mock_ioctl(int fd, unsigned long request, ...);

#define ioctl mock_ioctl
#include "../src/gid_query.c"
#undef ioctl

static int response_error;
static unsigned call_count;
static unsigned table_capacity;
static unsigned table_entries;
static const unsigned char expected_gid[16] = {
	0xfe, 0x80, 0, 0, 0, 0, 0, 0,
	0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
};

static void fill_entry(struct ruapc_uverbs_gid_entry *entry, unsigned index,
		       unsigned port_num, unsigned gid_type)
{
	memcpy(entry->gid, expected_gid, sizeof(entry->gid));
	entry->gid_index = index;
	entry->port_num = port_num;
	entry->gid_type = gid_type;
	entry->netdev_ifindex = 0;
}

static int mock_ioctl(int fd, unsigned long request, ...)
{
	struct ib_uverbs_ioctl_hdr header;
	struct ib_uverbs_attr attrs[4];
	const unsigned char *command;
	va_list args;
	unsigned i;

	va_start(args, request);
	command = (const unsigned char *)va_arg(args, struct ruapc_uverbs_command *);
	va_end(args);
	/* Decode the wire bytes using the installed UAPI types without
	 * aliasing the private command as a different struct type.
	 */
	memcpy(&header, command, sizeof(header));
	memcpy(attrs, command + sizeof(header), sizeof(attrs));
	call_count++;
	assert(fd == 123);
	assert(_IOC_TYPE(request) == 0x1b && _IOC_NR(request) == 1);
	assert(_IOC_SIZE(request) == 24);
	assert(_IOC_DIR(request) == (_IOC_READ | _IOC_WRITE));
	assert(header.length == 88);
	assert(header.object_id == 0);
	assert(header.num_attrs == 4 && header.driver_id == 7);
	assert(header.reserved1 == 0 && header.reserved2 == 0);
	for (i = 0; i < 4; i++) {
		assert(attrs[i].attr_id == i);
		assert(attrs[i].flags == 1);
		assert(attrs[i].attr_data.reserved == 0);
	}

	if (header.method_id == 6) {
		unsigned char *response;
		uint32_t type = 2;

		assert(attrs[0].len == 8 && attrs[0].data == 2);
		assert(attrs[1].len == 8 && attrs[1].data == 513);
		assert(attrs[2].len == 4 && attrs[2].data == 0);
		assert(attrs[3].len == 32 && attrs[3].data);
		assert(attrs[3].data % 8 == 0);
		response = (void *)(uintptr_t)attrs[3].data;
		memcpy(response, expected_gid, 16);
		memcpy(response + 24, &type, 4);
	} else {
		struct ruapc_uverbs_gid_entry *entries;
		uint64_t count = table_entries;

		assert(header.method_id == 5);
		assert(attrs[0].len == 8 &&
		       attrs[0].data == sizeof(struct ruapc_uverbs_gid_entry));
		assert(attrs[1].len == 4 && attrs[1].data == 0);
		assert(attrs[2].len == table_capacity * sizeof(*entries) &&
		       attrs[2].data);
		assert(attrs[3].len == 8 && attrs[3].data);
		entries = (void *)(uintptr_t)attrs[2].data;
		for (i = 0; i < table_entries; i++)
			fill_entry(&entries[i], 10 + i, 1 + i, i);
		memcpy((void *)(uintptr_t)attrs[3].data, &count, sizeof(count));
	}
	if (response_error) {
		/* An error after a partial write must not publish outputs. */
		errno = response_error;
		return -1;
	}
	return 0;
}

int main(void)
{
	struct ruapc_uverbs_gid_entry entries[4];
	unsigned char gid[16] = {};
	uint32_t type = UINT32_MAX;
	uint32_t count;
	int errors[] = { ENOTTY, EPROTONOSUPPORT, ENODATA, EINVAL, EIO };
	unsigned i;

	assert(ruapc_query_gid_entry(123, 7, 2, 513, gid, &type) == 0);
	assert(!memcmp(gid, expected_gid, sizeof(gid)) && type == 2);
	for (i = 0; i < sizeof(errors) / sizeof(errors[0]); i++) {
		unsigned char unchanged[16];

		memset(gid, 0xab, sizeof(gid));
		memcpy(unchanged, gid, sizeof(gid));
		type = UINT32_MAX;
		response_error = errors[i];
		assert(ruapc_query_gid_entry(123, 7, 2, 513, gid, &type) == errors[i]);
		assert(!memcmp(gid, unchanged, sizeof(gid)) && type == UINT32_MAX);
	}
	assert(call_count == 1 + sizeof(errors) / sizeof(errors[0]));
	response_error = 0;

	/* A table response carries multiple entries and its count. */
	table_capacity = 4;
	table_entries = 3;
	memset(entries, 0, sizeof(entries));
	count = UINT32_MAX;
	assert(ruapc_query_gid_table(123, 7, entries, table_capacity, &count) == 0);
	assert(count == table_entries);
	for (i = 0; i < table_entries; i++) {
		assert(entries[i].gid_index == 10 + i);
		assert(entries[i].port_num == 1 + i);
		assert(entries[i].gid_type == i);
		assert(!memcmp(entries[i].gid, expected_gid, sizeof(entries[i].gid)));
	}

	/* A failed table query must not publish its partial count. */
	response_error = ENODATA;
	count = UINT32_MAX;
	assert(ruapc_query_gid_table(123, 7, entries, table_capacity, &count) == ENODATA);
	assert(count == UINT32_MAX);
	response_error = 0;

	/* Invalid buffers are rejected without issuing an ioctl. */
	{
		unsigned issued = call_count;

		assert(ruapc_query_gid_table(123, 7, entries, 0, &count) == EINVAL);
		assert(ruapc_query_gid_table(123, 7, NULL, table_capacity, &count) ==
		       EINVAL);
		assert(ruapc_query_gid_table(123, 7, entries,
					     UINT16_MAX / sizeof(entries[0]) + 1,
					     &count) == EINVAL);
		assert(call_count == issued);
	}
	return 0;
}
