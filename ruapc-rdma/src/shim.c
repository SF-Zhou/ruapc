/* See shim.h for why these wrappers exist. */
#include "shim.h"

/* Device management */

struct ibv_device **ruapc_ibv_get_device_list(int *num_devices)
{
	return ibv_get_device_list(num_devices);
}

void ruapc_ibv_free_device_list(struct ibv_device **list)
{
	ibv_free_device_list(list);
}

__be64 ruapc_ibv_get_device_guid(struct ibv_device *device)
{
	return ibv_get_device_guid(device);
}

struct ibv_context *ruapc_ibv_open_device(struct ibv_device *device)
{
	return ibv_open_device(device);
}

int ruapc_ibv_close_device(struct ibv_context *context)
{
	return ibv_close_device(context);
}

/* Queries */

int ruapc_ibv_query_device(struct ibv_context *context,
                           struct ibv_device_attr *device_attr)
{
	return ibv_query_device(context, device_attr);
}

int ruapc_ibv_query_port(struct ibv_context *context, uint8_t port_num,
                         struct ibv_port_attr *port_attr)
{
	return ibv_query_port(context, port_num, port_attr);
}

int ruapc_ibv_query_gid(struct ibv_context *context, uint8_t port_num,
                        int index, union ibv_gid *gid)
{
	return ibv_query_gid(context, port_num, index, gid);
}

/* Protection domain */

struct ibv_pd *ruapc_ibv_alloc_pd(struct ibv_context *context)
{
	return ibv_alloc_pd(context);
}

int ruapc_ibv_dealloc_pd(struct ibv_pd *pd)
{
	return ibv_dealloc_pd(pd);
}

/* Memory region */

struct ibv_mr *ruapc_ibv_reg_mr(struct ibv_pd *pd, void *addr, size_t length,
                                int access)
{
	/* `access` is not a compile-time constant here, so on modern
	 * rdma-core the ibv_reg_mr macro conservatively routes through
	 * ibv_reg_mr_iova2, which accepts optional access flags. */
	return ibv_reg_mr(pd, addr, length, access);
}

int ruapc_ibv_dereg_mr(struct ibv_mr *mr)
{
	return ibv_dereg_mr(mr);
}

/* Completion channel */

struct ibv_comp_channel *ruapc_ibv_create_comp_channel(
	struct ibv_context *context)
{
	return ibv_create_comp_channel(context);
}

int ruapc_ibv_destroy_comp_channel(struct ibv_comp_channel *channel)
{
	return ibv_destroy_comp_channel(channel);
}

int ruapc_ibv_get_cq_event(struct ibv_comp_channel *channel,
                           struct ibv_cq **cq, void **cq_context)
{
	return ibv_get_cq_event(channel, cq, cq_context);
}

/* Completion queue */

struct ibv_cq *ruapc_ibv_create_cq(struct ibv_context *context, int cqe,
                                   void *cq_context,
                                   struct ibv_comp_channel *channel,
                                   int comp_vector)
{
	return ibv_create_cq(context, cqe, cq_context, channel, comp_vector);
}

int ruapc_ibv_destroy_cq(struct ibv_cq *cq)
{
	return ibv_destroy_cq(cq);
}

int ruapc_ibv_req_notify_cq(struct ibv_cq *cq, int solicited_only)
{
	return ibv_req_notify_cq(cq, solicited_only);
}

void ruapc_ibv_ack_cq_events(struct ibv_cq *cq, unsigned int nevents)
{
	ibv_ack_cq_events(cq, nevents);
}

int ruapc_ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc)
{
	return ibv_poll_cq(cq, num_entries, wc);
}

/* Queue pair */

struct ibv_qp *ruapc_ibv_create_qp(struct ibv_pd *pd,
                                   struct ibv_qp_init_attr *qp_init_attr)
{
	return ibv_create_qp(pd, qp_init_attr);
}

int ruapc_ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr,
                        int attr_mask)
{
	return ibv_modify_qp(qp, attr, attr_mask);
}

int ruapc_ibv_destroy_qp(struct ibv_qp *qp)
{
	return ibv_destroy_qp(qp);
}

int ruapc_ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                        struct ibv_send_wr **bad_wr)
{
	return ibv_post_send(qp, wr, bad_wr);
}

int ruapc_ibv_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr,
                        struct ibv_recv_wr **bad_wr)
{
	return ibv_post_recv(qp, wr, bad_wr);
}
