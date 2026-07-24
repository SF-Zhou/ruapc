/* C shim wrapping every libibverbs entry point used by this crate.
 *
 * Why wrap everything, not just the problematic ones?
 *
 * rdma-core's standard ABI-evolution technique is to keep the old exported
 * symbol for already-compiled binaries and redirect newly compiled code to
 * new semantics via a function-like macro or static inline wrapper in
 * <infiniband/verbs.h> (ibv_query_port and ibv_reg_mr both started life as
 * plain functions and were later macro-wrapped this way). bindgen binds
 * exported symbols directly, so it silently keeps the frozen legacy
 * semantics forever -- no compile error, just subtly wrong behavior (e.g.
 * the legacy ibv_query_port only fills the compat-sized prefix of
 * ibv_port_attr, leaving newer fields uninitialized).
 *
 * Routing every call through this C translation unit guarantees "freshly
 * compiled against the installed header" semantics for each entry point,
 * on any platform and any rdma-core version, present or future. The C
 * compiler also type-checks each wrapper body against the real header
 * prototypes. The cost is one direct call per invocation, which is not
 * measurable even on the hottest path (empty ibv_poll_cq on mlx5:
 * 9.79 ns/op both with and without the shim).
 *
 * Rust code must never bind an ibv_* symbol directly; add a wrapper here
 * instead.
 */
#ifndef RUAPC_RDMA_SHIM_H
#define RUAPC_RDMA_SHIM_H

#include <infiniband/verbs.h>

/* Device management */

struct ibv_device **ruapc_ibv_get_device_list(int *num_devices);

void ruapc_ibv_free_device_list(struct ibv_device **list);

__be64 ruapc_ibv_get_device_guid(struct ibv_device *device);

struct ibv_context *ruapc_ibv_open_device(struct ibv_device *device);

int ruapc_ibv_close_device(struct ibv_context *context);

/* Queries */

int ruapc_ibv_query_device(struct ibv_context *context,
                           struct ibv_device_attr *device_attr);

int ruapc_ibv_query_port(struct ibv_context *context, uint8_t port_num,
                         struct ibv_port_attr *port_attr);

int ruapc_ibv_query_gid(struct ibv_context *context, uint8_t port_num,
                        int index, union ibv_gid *gid);

/* Protection domain */

struct ibv_pd *ruapc_ibv_alloc_pd(struct ibv_context *context);

int ruapc_ibv_dealloc_pd(struct ibv_pd *pd);

/* Memory region */

struct ibv_mr *ruapc_ibv_reg_mr(struct ibv_pd *pd, void *addr, size_t length,
                                int access);

int ruapc_ibv_dereg_mr(struct ibv_mr *mr);

/* Completion channel */

struct ibv_comp_channel *ruapc_ibv_create_comp_channel(
	struct ibv_context *context);

int ruapc_ibv_destroy_comp_channel(struct ibv_comp_channel *channel);

int ruapc_ibv_get_cq_event(struct ibv_comp_channel *channel,
                           struct ibv_cq **cq, void **cq_context);

/* Completion queue */

struct ibv_cq *ruapc_ibv_create_cq(struct ibv_context *context, int cqe,
                                   void *cq_context,
                                   struct ibv_comp_channel *channel,
                                   int comp_vector);

int ruapc_ibv_destroy_cq(struct ibv_cq *cq);

int ruapc_ibv_req_notify_cq(struct ibv_cq *cq, int solicited_only);

void ruapc_ibv_ack_cq_events(struct ibv_cq *cq, unsigned int nevents);

int ruapc_ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);

/* Queue pair */

struct ibv_qp *ruapc_ibv_create_qp(struct ibv_pd *pd,
                                   struct ibv_qp_init_attr *qp_init_attr);

int ruapc_ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr,
                        int attr_mask);

int ruapc_ibv_destroy_qp(struct ibv_qp *qp);

int ruapc_ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                        struct ibv_send_wr **bad_wr);

int ruapc_ibv_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr,
                        struct ibv_recv_wr **bad_wr);

#endif /* RUAPC_RDMA_SHIM_H */
