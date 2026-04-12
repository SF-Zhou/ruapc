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
        id: u32,
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
        id: u32,
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
    /// 注册表：ID → (内存起始地址, 长度)，用于安全校验
    registry: Mutex<HashMap<u32, (usize, usize)>>,
    next_id: AtomicU32,
}
```

- `register_memory`：分配新的 u32 ID，记录内存地址和长度到 registry
- `unregister`：从 registry 中移除该 ID
- `validate_access(id, offset, len)`：校验 ID 存在性、offset + len 不越界

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

    /// 获取第 idx 个 RDMA 设备（仅计 RDMA 类型，0-based）
    #[cfg(feature = "rdma")]
    fn rdma_device(&self, idx: usize) -> Result<Arc<Device>>;

    /// 获取第一个 TCP 设备
    fn tcp_device(&self) -> Result<Arc<Device>>;

    /// 获取设备数量
    fn len(&self) -> usize;

    /// 遍历所有设备
    fn iter(&self) -> impl Iterator<Item = &Arc<Device>>;
}
```

- `add_device` 在添加时设置 `device.index()`，编号从 0 开始单调递增
- 一旦创建 `BufferPool`，`Devices` 通过 `Arc<Devices>` 共享，不再添加新设备
- `Devices` 由 `State::create()` 在内部构造，不由用户手动传入

### State 初始化：Devices 和 BufferPool 的构造

`State::create(router, config)` 在内部统一完成设备和内存池的初始化，调用者无需手动构造：

1. **构造 Devices：** 自动添加一个 TCP 设备，并在 `rdma` feature 启用时扫描可用 RDMA 设备并全部添加。
2. **构造 BufferPool：** 以所有设备为参数创建共享用户内存池（块大小 4MiB，chunk 大小 256MiB），注册在所有设备上。
3. **注册 MemoryService：** 自动在 router 中注册 MemoryService，处理对端发来的 Remote Read/Write 请求。
4. **构造 SocketPool：** 在 `rdma` feature 启用时，用相同的 RDMA 设备 Arc 创建传输层 rdma buffer pool 并传入 RdmaSocketPool，确保 QP 和用户 Buffer 共享同一 protection domain。

调用者通过 accessor 访问 State 的设备和内存池：

```rust
impl State {
    /// 返回共享设备集合
    pub fn devices(&self) -> &Arc<Devices>;

    /// 返回共享用户内存池
    pub fn buffer_pool(&self) -> &Arc<BufferPool>;
}
```

**典型用法（服务端 handler 内）：**

```rust
// 直接从 State 的共享池分配 buffer
let mut local_buf = ctx.state.buffer_pool().allocate().unwrap();

// 通过 State 的设备获取 RemoteBufferInfo
let rdma_device = ctx.state.devices().rdma_device(0)?;
let rbi = buf.remote_buffer_info(&rdma_device)?;
```

### BufferPool

内存池，管理大块内存的分配和回收。所有 socket pool 共享同一个 `BufferPool` 实例，由 `State` 持有并通过 `state.buffer_pool()` 对外提供。

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
- `len`：要读写的长度
- 需要实现 `Serialize` / `Deserialize` 以便在 RPC 消息中传输

## Remote Read/Write API

### Socket 层接口

Socket 持有 `Arc<Device>`，根据设备类型选择具体的实现机制。

```rust
impl Socket {
    /// 从远端内存读取数据到本地 buffer
    async fn remote_read(
        &self,
        remote: &RemoteBufferInfo,
        local: &mut Buffer,
    ) -> Result<()>;

    /// 将本地 buffer 的数据写入远端内存
    async fn remote_write(
        &self,
        remote: &RemoteBufferInfo,
        local: &Buffer,
    ) -> Result<()>;
}
```

### Context 层接口

Context（上下文）提供更方便的封装，自动处理设备查找和 buffer 管理。

```rust
impl Context {
    /// 便捷接口：从远端读取数据
    async fn remote_read(
        &self,
        socket: &Socket,
        remote: &RemoteBufferInfo,
        local: &mut Buffer,
    ) -> Result<()>;

    /// 便捷接口：向远端写入数据
    async fn remote_write(
        &self,
        socket: &Socket,
        remote: &RemoteBufferInfo,
        local: &Buffer,
    ) -> Result<()>;
}
```

## 传输层实现

### TCP：反向 RPC 模拟

TCP 设备上的 Remote Read/Write 通过反向 RPC 实现：

**Remote Read 流程：**

1. Server 调用 `socket.remote_read(remote_info, local_buffer)`
2. Server 通过反向 RPC 发送 Remote Read 命令给 Client，包含 `RemoteBufferInfo`
3. Client 收到命令后，校验 `remote_info`（ID 存在性、offset + len 不越界）
4. 校验通过后，Client 从本地注册内存中读取数据，通过 RPC 返回
5. Server 将返回数据写入 `local_buffer`

**Remote Write 流程：**

1. Server 调用 `socket.remote_write(remote_info, local_buffer)`
2. Server 通过反向 RPC 发送 Remote Write 命令给 Client，包含 `RemoteBufferInfo` 和待写入数据
3. Client 收到命令后，校验 `remote_info`（ID 存在性、offset + len 不越界）
4. 校验通过后，Client 将数据写入本地注册内存
5. Client 返回写入结果

**安全校验：**

TCP Device 内部维护注册表 `HashMap<u32, (usize, usize)>`（ID → 内存起始地址和长度），每次 Remote Read/Write 时：
- 校验 ID 是否存在于注册表中
- 校验 `offset + len` 是否在注册范围内，防止越界访问
- 反注册时从注册表中移除条目

### RDMA Read：直接 RDMA Read

RDMA 设备上的 Remote Read 直接使用 RDMA Read verbs 操作：

1. Server 调用 `socket.remote_read(remote_info, local_buffer)`
2. 提取 `remote_info.key` 中的 rkey，`local_buffer` 的 lkey
3. 构造 `ibv_send_wr`（opcode = `IBV_WR_RDMA_READ`），设置 remote addr/rkey 和 local addr/lkey
4. 调用 `ibv_post_send` 发起 RDMA Read
5. 通过 completion queue 等待操作完成

### RDMA Write 模拟：控制消息 + Client-side RDMA Read

RDMA 设备上的 Remote Write 不直接使用 RDMA Write verbs，而是通过控制消息 + Client-side RDMA Read 模拟：

**流程：**

1. Server 调用 `socket.remote_write(remote_info, local_buffer)`
2. Server 将 `local_buffer` 的 `RemoteBufferInfo`（包含 Server 端的 rkey、addr、len）通过控制消息发送给 Client
3. Client 收到控制消息后，执行 RDMA Read 从 Server 的 `local_buffer` 读取数据到 `remote_info` 对应的本地内存
4. Client 完成 RDMA Read 后，发送完成通知给 Server
5. Server 收到完成通知，`remote_write` 返回成功

**设计考量：**
- 所有数据流动都通过"本地"的 RDMA Read 完成，更易于并发控制
- 避免 RDMA Write 带来的网络拥塞问题
- 对称设计：双方都需要注册内存。Server 端注册 `local_buffer` 作为数据源，Client 端注册目标内存用于 RDMA Read
- `remote_write` 是异步操作，阻塞直到整个流程完成或超时返回错误
- **Buffer 生命周期安全：** `remote_write` 的 future 必须持有 `local_buffer` 的引用，确保在整个异步流程中 Buffer 不会被 Drop 归还到 free list

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

`Devices` 在 `BufferPool` 创建前固定，不支持动态添加设备。`State::create()` 在内部自动构造 `Devices`（TCP + 可用 RDMA 设备），调用者不需要手动传入。`Memory` 内部的 `registrations` 长度在初始化时就确定，支持 O(1) 索引访问。

### TCP/RDMA 开发测试对称性

应用层 API 对 TCP 和 RDMA 完全一致。开发者可以在 TCP 环境下完成所有开发和测试，部署时切换到 RDMA 网络即可获得高性能，无需修改业务代码。
