/* Check the installed verbs.h inline ABI without loading a device or library. */
#include "../../src/ffi/c/gid/native.c"

#include <assert.h>
#include <errno.h>
#include <string.h>

static struct ibv_context *expected_context;
static struct ibv_gid_entry *expected_entries;
static size_t expected_capacity;
static int query_result;
static ssize_t table_result;
static unsigned query_calls;
static unsigned table_calls;
static const struct ibv_gid_entry expected_entry = {
	.gid.raw = {0xfe, 0x80, 0, 0, 0, 0, 0, 0,
		    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
	.gid_index = 0xf0000123,
	.port_num = 0x102,
	.gid_type = IBV_GID_TYPE_ROCE_V2,
	.ndev_ifindex = 17,
};

/* Mock the exported entry-size ABI, leaving the public inline wrappers intact. */
int _ibv_query_gid_ex(struct ibv_context *context, uint32_t port_num,
		      uint32_t gid_index, struct ibv_gid_entry *entry,
		      uint32_t flags, size_t entry_size)
{
	query_calls++;
	assert(context == expected_context);
	assert(port_num == expected_entry.port_num);
	assert(gid_index == expected_entry.gid_index);
	assert(entry == expected_entries);
	assert(flags == 0);
	assert(entry_size == sizeof(*entry));
	if (!query_result)
		*entry = expected_entry;
	return query_result;
}

ssize_t _ibv_query_gid_table(struct ibv_context *context,
			    struct ibv_gid_entry *entries, size_t max_entries,
			    uint32_t flags, size_t entry_size)
{
	table_calls++;
	assert(context == expected_context);
	assert(entries == expected_entries);
	assert(max_entries == expected_capacity);
	assert(flags == 0);
	assert(entry_size == sizeof(*entries));
	for (ssize_t i = 0; i < table_result; i++) {
		assert((size_t)i < max_entries);
		entries[i] = expected_entry;
		entries[i].gid_index = (uint32_t)i;
	}
	return table_result;
}

static void test_query(void)
{
	struct ibv_context context = {0};
	struct ibv_gid_entry entry = {0};
	const int results[] = {0, ENODATA, EOPNOTSUPP, EINVAL};

	expected_context = &context;
	expected_entries = &entry;
	for (size_t i = 0; i < sizeof(results) / sizeof(results[0]); i++) {
		query_result = results[i];
		errno = EDOM;
		assert(ruapc_ibv_query_gid_ex(&context, expected_entry.port_num,
					     expected_entry.gid_index, &entry) ==
		       query_result);
		assert(errno == EDOM);
		assert(query_calls == i + 1);
	}
	assert(memcmp(entry.gid.raw, expected_entry.gid.raw,
		      sizeof(entry.gid.raw)) == 0);
	assert(entry.gid_index == expected_entry.gid_index);
	assert(entry.port_num == expected_entry.port_num);
	assert(entry.gid_type == expected_entry.gid_type);
	assert(entry.ndev_ifindex == expected_entry.ndev_ifindex);
	/* Release the mock's borrowed pointers before the fixtures leave scope. */
	expected_context = NULL;
	expected_entries = NULL;
}

static void test_table(void)
{
	struct ibv_context context = {0};
	struct ibv_gid_entry entries[7] = {0};
	const ssize_t results[] = {2, 0, -ENOSPC, -EOPNOTSUPP, -EINVAL};

	expected_context = &context;
	expected_entries = entries;
	expected_capacity = sizeof(entries) / sizeof(entries[0]);
	for (size_t i = 0; i < sizeof(results) / sizeof(results[0]); i++) {
		table_result = results[i];
		errno = EDOM;
		assert(ruapc_ibv_query_gid_table_native(&context, entries,
						       expected_capacity) ==
		       table_result);
		assert(errno == EDOM);
		assert(table_calls == i + 1);
	}
	assert(entries[0].gid_index == 0 && entries[1].gid_index == 1);
	assert(entries[0].gid_type == expected_entry.gid_type);
	assert(entries[1].ndev_ifindex == expected_entry.ndev_ifindex);
	/* Release the mock's borrowed pointers before the fixtures leave scope. */
	expected_context = NULL;
	expected_entries = NULL;
}

int main(void)
{
	test_query();
	test_table();
	return 0;
}
