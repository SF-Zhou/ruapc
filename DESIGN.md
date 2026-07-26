# DESIGN.md — Remote Read/Write 设计方案

## 设计目标

### 1. 支持 Remote Read/Write

在常规双边 Request/Response RPC 模式的基础上，Server 端支持 Remote Read/Write 操作。Client 提供 Buffer 地址、长度和 MemoryKey 后，Server 可以直接读写 Client 端的内存，无需 Client 参与数据拷贝。

### 2. TCP 模拟 Remote Read/Write

Remote Read/Write 功能不限于 RDMA 设备，同样扩展到 TCP 网络之上。TCP 上通过反向 RPC 模拟 Remote Read/Write 操作：Server 发送 Remote Read/Write 命令给 Client，Client 执行相应操作并返回结果。模拟时需要提前完成内存注册以保证访问安全。应用可以完全在 TCP 环境下开发测试，最后直接在 RDMA 网络上部署和使用。

### 3. RDMA Write 通过 Client-side RDMA Read 模拟

RDMA Write 在实践中不被推荐使用（并发控制困难、易造成网络拥塞）。当 Server 端需要执行 RDMA Write 时，改为发消息通知 Client，由 Client 执行 RDMA Read 从 Server 端读取数据，完成后通知 Server。所有数据流动都通过"本地"的 RDMA Read 完成，更易于并发控制和避免网络拥塞。这是一个对称设计——无论 Remote Read 还是 Remote Write，双方都需要注册内存。

## 核心类型

### AlignedMemory

有所有权的、对齐的内存对象，是系统内存分配和回收的最小单位。

```rust
pub struct AlignedMemory {
    ptr: NonNull<u8>,
    size: usize,
}
```

- 分配时使用固定的对齐值（如 2MiB，适配 huge page）
- 在 `Drop` 时自动调用 `std::alloc::dealloc` 释放内存
- 提供 `as_ptr()` / `as_mut_ptr()` / `as_slice()` / `as_mut_slice()` 接口
- 不使用 `dyn Allocator` 抽象，直接使用固定对齐分配实现，符合 enum 多态原则

**设计考量：** `ruapc-bufpool` 中的 `DefaultAllocator` 已在 64 位平台上使用 2MiB 对齐分配，新设计将此逻辑固化到 `AlignedMemory` 中，去掉不必要的 trait 抽象。

### MemoryKey (enum)

注册完成后的内存在特定设备上的 key，用于传播给远端进行远程内存访问。遵循 enum dispatch 原则，按 Device 类型划分不同的 variant。

```rust
pub enum MemoryKey {
    Tcp {
        id: u64,
    },
    #[cfg(feature = "rdma")]
    Rdma {
        lkey: u32,
        rkey: u32,
    },
}
```

- **TCP variant：** 存储内存在 TCP 设备上注册的唯一 ID
- **RDMA variant：** 存储 lkey（本地访问）和 rkey（远端访问），对应 ibverbs 的 `ibv_mr.lkey` / `ibv_mr.rkey`
- RDMA variant 通过 `#[cfg(feature = "rdma")]` feature flag 控制

### MemoryRegistration (enum)

表达 `AlignedMemory` 在某个 Device 上的注册状态。持有 `Arc<Device>` 以保证设备生命周期。

```rust
pub enum MemoryRegistration {
    Tcp {
        device: Arc<Device>,
        id: u64,
    },
    #[cfg(feature = "rdma")]
    Rdma {
        device: Arc<Device>,
        mr: RawMemoryRegion, // *mut ibv_mr, Drop 时调用 ibv_dereg_mr
    },
}
```

关键方法：

```rust
impl MemoryRegistration {
    /// 获取该注册对应的 MemoryKey
    fn memory_key(&self) -> MemoryKey;

    /// 反注册内存。由 Memory::drop 调用
    fn unregister(&mut self, mem: &AlignedMemory);
}
```

- **TCP：** 反注册时从 TCP Device 的注册表中移除该 ID
- **RDMA：** 反注册时调用 `ibv_dereg_mr` 释放 memory region

**设计考量：** 参考 `ruapc-rdma` 中 `RawMemoryRegion` 的 RAII 设计。注意 `ruapc-rdma` 现有的 `RegisteredBuffer::rkey()` 方法存在 bug（返回了 lkey），新设计中需要修正。

### Memory

包含一个 `AlignedMemory` 和一组 `MemoryRegistration`，表示内存在一组设备上完成了注册。

```rust
pub struct Memory {
    aligned_memory: AlignedMemory,
    registrations: Vec<Option<MemoryRegistration>>, // 按 device.index() 索引
}
```

关键方法：

```rust
impl Memory {
    /// 通过 device.index() O(1) 查找该设备的 MemoryKey
    fn get_memory_key(&self, device: &Device) -> Result<MemoryKey>;

    /// 插入一个新的 MemoryRegistration，由 Device::register_memory 调用
    fn insert_registration(&mut self, index: usize, reg: MemoryRegistration);
}

impl Drop for Memory {
    /// 依次调用每个 MemoryRegistration 的 unregister 方法完成反注册
    fn drop(&mut self);
}
```

- `registrations` 使用固定长度 `Vec<Option<MemoryRegistration>>`，按 `device.index()` 索引，实现 O(1) 查找
- 由于 `Devices` 在 `BufferPool` 创建前就固定下来，`registrations` 的长度在初始化时就已确定
- `get_memory_key` 通过 `device.index()` 直接索引到对应的 `MemoryRegistration`

### Device (enum)

对网卡设备的抽象。遵循 enum dispatch 原则，使用 enum 而非 dyn trait。

```rust
pub enum Device {
    Tcp(TcpDevice),
    #[cfg(feature = "rdma")]
    Rdma(RdmaDevice),
}
```

关键方法和字段：

```rust
impl Device {
    /// 在此设备上注册内存，成功则将 MemoryRegistration 插入 Memory
    fn register_memory(&self, mem: &mut Memory) -> Result<()>;

    /// 获取设备的唯一编号（由 Devices::add_device 分配）
    fn index(&self) -> usize;
}
```

#### TcpDevice

```rust
pub struct TcpDevice {
    index: usize,
    /// 注册表：ID → (内存起始地址, 长度)，用于安全校验（使用 DashMap 支持并发访问）
    registry: DashMap<u64, (usize, usize)>,
    next_id: AtomicU64,
}
```

- `register_memory`：分配新的 u64 ID，记录内存地址和长度到 registry
- `unregister`：从 registry 中移除该 ID
- `validate_access(id, addr, len)`：校验 ID 存在性、addr 范围不越界

#### RdmaDevice

```rust
#[cfg(feature = "rdma")]
pub struct RdmaDevice {
    index: usize,
    device: Arc<ruapc_rdma::Device>, // 封装 ruapc-rdma 的 Device
}
```

- `register_memory`：调用 `ibv_reg_mr` 注册内存到 RDMA 设备的 protection domain
- `unregister`：调用 `ibv_dereg_mr` 释放 memory region

**设计考量：** RDMA variant 在 ruapc 核心 crate 中定义，通过 feature flag 控制编译。在 ruapc crate 内重新封装 `RdmaDevice`，隔离对 `ruapc-rdma` 底层接口的直接依赖。

### Devices

一组 Device 的集合，在 BufferPool 创建前固定。

```rust
pub struct Devices {
    devices: Vec<Arc<Device>>,
}
```

关键方法：

```rust
impl Devices {
    /// 添加设备并分配单调递增的唯一编号
    fn add_device(&mut self, device: Device) -> Arc<Device>;

    /// 获取设备数量
    fn len(&self) -> usize;

    /// 遍历所有设备
    fn iter(&self) -> impl Iterator<Item = &Arc<Device>>;
}
```

- `add_device` 在添加时设置 `device.index()`，编号从 0 开始单调递增
- 一旦创建 `BufferPool`，`Devices` 通过 `Arc<Devices>` 共享，不再添加新设备

### BufferPool

内存池，管理大块内存的分配和回收。

```rust
pub struct BufferPool {
    devices: Arc<Devices>,
    memories: Mutex<Vec<Memory>>,
    free_list: Mutex<FreeList>,
    block_size: usize,        // 每次分配的固定大小，如 4MiB
    chunk_size: usize,        // 每次向系统申请的大块大小，如 256MiB
}
```

关键方法：

```rust
impl BufferPool {
    /// 创建 BufferPool
    fn new(devices: Arc<Devices>, block_size: usize, chunk_size: usize) -> Arc<Self>;

    /// 分配一个 Buffer
    fn allocate(self: &Arc<Self>) -> Result<Buffer>;

    /// 归还 Buffer（由 Buffer::drop 调用）
    fn return_buffer(&self, ...);
}
```

- 当 free list 为空时，申请一个大块 `AlignedMemory`（如 256MiB），在所有 Device 上完成注册得到 `Memory` 对象
- 将大块 `Memory` 切分为固定大小的 block（如 4MiB），加入 free list
- 后续 `allocate()` 从 free list 中分配；`return_buffer()` 归还到 free list

**设计考量：** 复用 `ruapc-bufpool` 的 buddy allocator 核心逻辑（四叉树，1/4/16/64 MiB 层级，intrusive free list）。去掉 `dyn Allocator` 抽象，AlignedMemory 的分配直接使用固定对齐实现。async 等待机制（当内存不足时 await）也可以从 `ruapc-bufpool` 复用。

### Buffer

从 BufferPool 分配的一段可用内存空间。

```rust
pub struct Buffer {
    pool: Arc<BufferPool>,
    ptr: NonNull<u8>,
    len: usize,
    // 用于定位所属 Memory 和在其中的偏移，以便获取 MemoryKey
    memory_index: usize,
    offset_in_memory: usize,
}
```

关键方法和 trait 实现：

```rust
impl Buffer {
    /// 获取指定设备的 MemoryKey（附带正确的偏移信息）
    fn remote_buffer_info(&self, device: &Device) -> Result<RemoteBufferInfo>;
}

impl Deref for Buffer {
    type Target = [u8];
}

impl DerefMut for Buffer { ... }

impl Drop for Buffer {
    /// 归还到 BufferPool 的 free list
    fn drop(&mut self);
}

// 手动实现 Send + Sync
unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}
```

- 持有 `Arc<BufferPool>` 确保 Pool（及其内部的 Memory）不会在 Buffer 存活期间被释放
- 实现 `Deref<Target=[u8]>` 和 `DerefMut`，可以直接当 `&[u8]` / `&mut [u8]` 使用
- `Drop` 时自动归还到 BufferPool 的 free list

### RemoteBufferInfo

远程内存访问所需的信息，通过常规 RPC 消息由应用层传递给对端。

```rust
pub struct RemoteBufferInfo {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
}
```

- `key`：目标内存的 MemoryKey，用于远端验证访问权限
- `addr`：目标内存的起始地址
- `len`：有效数据的字节数（Buffer 的逻辑长度，而非 capacity）
- 需要实现 `Serialize` / `Deserialize` 以便在 RPC 消息中传输（附着在
  `MsgMeta::read_regions` / `MsgMeta::write_regions` 中随请求发送，
  每个字段是 `Vec<RemoteBufferInfo>`）

## Remote Read/Write API

### 逻辑连续空间与 CopyOp

一切远程内存操作都基于**逻辑连续空间**抽象：一组 Buffer / Region 按顺序
拼接，每段贡献自己的逻辑长度（`Buffer::len()`，不是 capacity），
`total_len = Σ len_i`。请求两侧各有至多两个空间：

- **read space**（`MsgMeta::read_regions`）：Client 通过
  `with_read_buffers(&[Buffer])` 附加（借用语义，调用期间由 Client 持有）；
  Server 用 `ctx.remote_read` 从中读取
- **write space**（`MsgMeta::write_regions`）：Client 通过
  `with_write_buffers(Vec<Buffer>)` 附加（**所有权移交**，buffer 被 pin
  住直到请求结束）；Server 用 `ctx.remote_write` 写入

批量传输由 `CopyOp` 描述：

```rust
pub struct CopyOp {
    pub src_offset: u64,  // 源空间内偏移
    pub dst_offset: u64,  // 目的空间内偏移
    pub len: u64,
}
```

read 方向 src = read space、dst = Server 本地空间；write 方向 src = Server
本地空间、dst = write space——两个方向完全对称，底层共用同一套
fragmentation 引擎（`core/scatter.rs`）。

**校验（发起端与执行端都做，互不信任）：** offset/len 溢出检查、越界检查、
op 数量上限（`MAX_COPY_OPS`）、region 数量上限（`MAX_REGIONS`）、目的区间
互不重叠（并发写同一地址是 data race；跨多次调用的重叠由应用负责）。
校验失败返回 `InvalidCopyOp`，不触网。

### Context 层接口

```rust
impl Context {
    /// Client 附加的 read/write 空间视图（total_len、regions）；
    /// 未携带时返回 MissingBufferInfo
    fn remote_read_space(&self) -> Result<RemoteSpace<'_>>;
    fn remote_write_space(&self) -> Result<RemoteSpace<'_>>;

    /// 批量读：把 read space 的多个区间读进 local 空间
    /// （local = Vec<Buffer> 按 len 拼接）。成功返回填充后的 buffers；
    /// 失败时 buffers 随 RemoteIoError 归还（在飞硬件操作除外）
    async fn remote_read(&self, ops: &[CopyOp], local: Vec<Buffer>)
        -> Result<Vec<Buffer>, RemoteIoError>;

    /// 便捷接口：镜像 Client 的分段分配 buffer，1:1 读回整个 read space
    async fn remote_read_all(&self) -> Result<Vec<Buffer>>;

    /// 批量写：把 local 空间的多个区间写进 write space，
    /// 返回"传输已完成"的见证 SentBuffers（内含归还的 local buffers，
    /// 可 take_buffers() 复用；多次写入用 merge() 合并见证）
    async fn remote_write(&self, ops: &[CopyOp], local: Vec<Buffer>)
        -> Result<SentBuffers, RemoteIoError>;

    /// 便捷接口：local 全部内容 1:1 写到 write space 开头
    async fn remote_write_all(&self, local: Vec<Buffer>)
        -> Result<SentBuffers, RemoteIoError>;

    /// 零传输路径的显式见证（无 payload 的 handler 分支）
    fn sent_nothing(&self) -> SentBuffers;
}
```

零字节的 op 批量直接短路，不碰网络。在非 Connected endpoint（即非 Server
端 handler 上下文）调用返回 `NotConnected` 错误。

### `Result<WithBuffers<T>, E>`：强类型的 remote_write 契约

把方法返回值声明为 `Result<WithBuffers<T>, E>`（`ResultWithBuffers<T>` 是
普通别名；任何用户自定义别名、自定义 Error 类型均可），契约在两端同时成立：

```rust
#[ruapc::service]
pub trait BlobService {
    async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<u64>>;
}

// Server：WithBuffers 只能由完成的传输产生（remote_write 返回的
// SentBuffers 见证），写入就发生在 handler 内部 — 延迟可测量、失败可
// 通过 RemoteIoError 拿回 buffers 重试，而"忘记写"在编译期就不可能
// （构造不出返回值）。响应值在传输完成后再确定：
async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<u64>> {
    let bufs = /* 从 buffer_pool 分配并填充、set_len */;
    let t0 = Instant::now();
    let sent = ctx.remote_write_all(bufs).await?;      // 传输在此完成
    Ok(sent.reply(t0.elapsed().as_micros() as u64))    // T 事后决定
}

// Client：预先提供 pinned 目的 buffers，全部随响应一起返回 —
// 客户端不可能"忘记取"，也不可能提前释放正在被写的内存
let (rsp, buffers) = client
    .with_write_buffers(vec![buf_a, buf_b])
    .download(&ctx, &req)
    .await?
    .into_parts();
```

**实现机制：**

- **类型驱动而非名字驱动**：宏对所有方法生成同一份客户端代码，走
  `RpcCall<返回类型>` 分派；`Result<WithBuffers<T>, E>` 命中
  `CallWithBuffer` trait 实现（额外交付 buffers），其余命中 `CallPlain`。
  两组实现靠 `WithBuffers: !Deserialize` 的 bounds 天然不相交
- **witness 保证服务端契约**：`WithBuffers` 字段私有，唯一产生路径是
  `remote_write → SentBuffers::reply`（或显式的 `sent_nothing()`）
- **WriteTarget pin 保证客户端安全**：`with_write_buffers` 的 buffers
  移入 `Arc<WriteTarget>`，挂在 waiter entry 上；`push`/`pull` handler
  执行写入时 clone 这个 Arc——即使原请求超时、entry 被清理，在飞的
  RDMA READ 仍持有内存，绝不会把 NIC 可能还在写的内存还给 pool。
  正常路径下全部 buffers 随响应原子交付并从 `WithBuffers` 返回；
  失败路径可通过 `ClientWithBuffers::take_write_buffers()` 找回
- wire 上的响应就是 `T`（`WithBuffers` 透明序列化），数据走 out-of-band
  的 pull/push 协议；未附加 write buffers 的调用得到空 buffer 列表

## 传输层实现

### TCP：反向 RPC 模拟

**Remote Read 流程（`MemoryService/read`）：**

1. Server 校验 op 批量后发反向 RPC，携带 read regions 回显 + ops
2. Client 重新校验（region 注册表存在性、`addr + len` 不越界、op 边界），
   按 op 顺序把各区间拼成一个内联 blob 返回
3. Client 读后校验原始请求（msgid）仍在等待——否则数据可能来自已回收
   复用的内存，丢弃并返回 Timeout
4. Server 按 ops 把 blob 散射进 local 空间

**Remote Write 流程（`MemoryService/push`）：**

1. Server 把各 op 的源区间按顺序拼成内联 blob，随 ops 发反向 RPC
2. Client 查 waiter 拿到 `Arc<WriteTarget>`（不存在 → Timeout），校验 ops
   （对 write space 边界）、blob 长度 == Σ op.len
3. Client 按 ops 把 blob 拷贝进 pinned buffers；响应即完成通知

### RDMA Read：批量单边 RDMA Read

1. Server 校验后用 fragmentation 引擎把 ops 切成 work request：
   **先按 remote region 边界切**（一个 READ WR 的远端必须连续），每个
   remote 连续块内 local 侧跨段时生成 SG list（上限 = 设备
   `max_send_sge`，超出再拆 WR）——多 buffer 对上层透明
2. 并发 post 全部 WR（每连接 `rdma.max_inflight_read_wrs` 信号量限流，
   permit 由 poll 线程随 completion 归还）；一批 WR 共享一个
   `ReadBatch`（原子计数），最后一个 WC 到达时唤醒等待方
3. NIC 层并发度由握手协商的 `max_rd_atomic`/`max_dest_rd_atomic` 决定：
   双方在 Endpoint 交换中携带设备能力（`rd_atomic_cap`，上限 16），
   两侧都取 min，天然满足 RC 的 initiator ≤ responder 约束
4. 读完成后反向 RPC `is_message_waiting` 校验原始请求存活（单边读
   Client 无感知，超时后内存可能已复用）

### RDMA Write 模拟：控制消息 + Client-side RDMA Read

1. Server 调用 `ctx.remote_write(ops, local)`：把 local buffers 作为反向
   RPC（`MemoryService/pull`）的 **read regions** 附加（与 Client 附加
   read buffers 完全同一机制——角色对称），body 携带 ops
2. Client 查 waiter 拿 `Arc<WriteTarget>`、校验 ops，走**同一套**
   fragmentation + 批量 READ 引擎，从 Server 内存读进 pinned buffers
3. `pull` 响应即完成通知；Server 端 `remote_write` 返回 `SentBuffers`
4. 无需读后存活校验：目的内存由 Arc pin 住（Client 侧），源内存由
   Server 的 future 跨 await 持有

**设计考量：**
- 所有数据流动都通过"本地发起"的 RDMA Read 完成，更易于并发控制、
  避免 RDMA Write 的拥塞问题
- 对称设计：read/write 共用 region 通告、校验、fragmentation、批量
  completion 全套机制

### RDMA Read 软件超时

QP 硬件重传（timeout 0x12 × retry 6）不足以覆盖所有 NIC 卡死场景，
RDMA READ 有独立的软件超时（`rdma.read_timeout_ms`，默认 10s，0 禁用）：

- **不用逐操作 timer**：poll 线程的 housekeeping 里每 100ms 扫一次各连接
  的在飞 `ReadBatch` deadline（map 很小，成本可忽略），避免 tokio timer
  wheel 在高吞吐下成为瓶颈
- 超时触发：等待方立即收到 `RdmaReadTimeout` 错误；连接被 `set_error`
  （QP → ERR），NIC 随后 flush 所有在飞 WR
- **绝不提前回收内存**：`ReadBatch` 持有的内存 hold（local buffers 或
  `Arc<WriteTarget>`）只在**全部** WC（含 flush WC）到达后释放；WC 永远
  不来则宁可滞留，直到 socket 销毁（`ibv_destroy_qp` 返回后保证无在飞
  DMA，`RdmaSocket` 的字段声明顺序保证 QP 先于 hold 析构）
- 在飞 READ 未清空的连接不会被 poll 线程 teardown（`ready_to_remove`
  检查 `rdma_completions` 为空）；poll 线程自身退出时显式 fail 所有
  batch，防止等待方悬挂

## BufferPool 内存管理

### 复用 ruapc-bufpool 的 buddy allocator

新设计复用 `ruapc-bufpool` 的核心数据结构和算法：

- **四叉 buddy tree：** 每个 64MiB block 作为根节点，按 4 叉树分裂为 16MiB / 4MiB / 1MiB 层级
- **Intrusive free list：** free list 节点内嵌在 `BuddyBlock` 中，O(1) 插入和删除，无额外堆分配
- **Bit-packed state array：** 85 个节点的状态（Allocated/Free/Split）用 2 bit 编码，压缩在 22 字节数组中
- **RAII Buffer 归还：** Buffer 持有 `Arc<BufferPool>`，Drop 时自动归还并触发 buddy merge
- **Async 等待：** 内存不足时通过 `tokio::sync::oneshot` 等待其他 Buffer 归还后重试分配

### 与原设计的差异

1. **去掉 `dyn Allocator`：** 原设计通过 `Box<dyn Allocator>` 支持自定义内存分配器，新设计直接使用固定对齐分配（`AlignedMemory`），符合 enum 多态原则
2. **增加 Device 注册：** 分配 `AlignedMemory` 后，额外在所有 Device 上注册，得到 `Memory` 对象
3. **Buffer 携带 MemoryKey 信息：** Buffer 可以通过 `remote_buffer_info(device)` 获取设备相关的 `RemoteBufferInfo`

## 设计原则

### Enum Dispatch

所有运行时多态使用 enum variant（`Device`、`MemoryKey`、`MemoryRegistration`、`Socket`、`SocketPool`）而非 `dyn Trait`。原因：
1. 不需要未知的扩展性，不愿为此牺牲性能
2. async 相关的 dyn trait 运行时多态成本高且不成熟

### Buffer 生命周期安全

- `Buffer` 持有 `Arc<BufferPool>`，保证 Pool 及其内部的 Memory 不会提前释放
- `remote_write` 的异步流程中，future 持有 Buffer 的引用，防止 Buffer 被提前归还
- `Memory` 在 Drop 时依次反注册所有设备，保证资源正确释放

### Devices 固定性

`Devices` 在 `BufferPool` 创建前固定，不支持动态添加设备。这是最简单的方案，避免了已分配 Memory 需要补注册的复杂性。`Memory` 内部的 `registrations` 长度在初始化时就确定，支持 O(1) 索引访问。

### TCP/RDMA 开发测试对称性

应用层 API 对 TCP 和 RDMA 完全一致。开发者可以在 TCP 环境下完成所有开发和测试，部署时切换到 RDMA 网络即可获得高性能，无需修改业务代码。
