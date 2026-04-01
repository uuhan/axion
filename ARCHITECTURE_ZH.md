# Axion 项目架构分析（中文）

本文档用于帮助快速理解 Axion 的整体架构，重点覆盖 SQLite 集成与 LSM 存储引擎设计，包括它们分别解决的问题、关键取舍与优化方向。

## 1. 项目整体架构

Axion 是一个基于 Zig 的高性能嵌入式存储引擎，核心是 LSM-Tree，并通过 SQLite Virtual Table 对 SQL 场景提供集成能力。

### 1.1 目录与模块职责

- `/home/runner/work/axion/axion/src/db.zig`：数据库主入口，串联事务层与存储层。
- `/home/runner/work/axion/axion/src/lsm/`：LSM 核心实现（MemTable、WAL、SSTable、Compaction、Manifest、BlockCache 等）。
- `/home/runner/work/axion/axion/src/mvcc/`：MVCC 与事务管理（版本号、冲突检测、批量提交）。
- `/home/runner/work/axion/axion/src/sqlite/vtab.zig`：SQLite 虚拟表模块，负责把 SQL 操作映射到 Axion 引擎。
- `/home/runner/work/axion/axion/src/c_api.zig` + `/home/runner/work/axion/axion/include/axion.h`：C ABI，供 Python/Rust/Go 等语言通过 FFI 调用。
- `/home/runner/work/axion/axion/src/bench/`：对比测试 Axion Native / SQLite Native / Axion VTab。
- `/home/runner/work/axion/axion/src/tests/`：集成测试（索引、schema、savepoint、运维工具等）。

### 1.2 逻辑分层

1. 接口层：
   - Native API（直接调用 DB）
   - C API（跨语言）
   - SQLite VTab（通过 SQL 使用 Axion）
2. 事务层（MVCC）：
   - 事务快照版本
   - 冲突检测
   - Group Commit（批量提交 WAL）
3. 存储层（LSM）：
   - MemTable（内存）
   - WAL（顺序日志）
   - SSTable（分层持久化）
   - Compaction（后台归并）

---

## 2. SQLite 集成设计

### 2.1 解决的问题

SQLite 原生 B-Tree 在写密集场景下容易受随机写与 fsync 开销影响。Axion 通过 VTab 方式让用户继续使用 SQL，同时把底层存储切换为写优化的 LSM。

价值：
- 不改变 SQL 使用习惯。
- 对写入吞吐和并发更友好。
- 保留 SQLite 生态工具链与查询能力。

### 2.2 关键实现

文件：`/home/runner/work/axion/axion/src/sqlite/vtab.zig`

核心点：
- 实现 SQLite Virtual Table 生命周期与游标回调。
- 在 `xBestIndex` 阶段向 SQLite 声明可下推约束，减少无效扫描。
- 在 `xUpdate` 中把 INSERT/UPDATE/DELETE 映射到 Axion put/delete/事务逻辑。
- 支持通过 `CREATE VIRTUAL TABLE ... USING axion(...)` 传入 WAL 模式与引擎参数。

### 2.3 SQLite 集成的取舍

优点：
- SQL 兼容路径清晰，接入成本低。
- 能复用 SQLite 上层查询与客户端生态。

成本：
- VTab 层引入额外封送与回调开销。
- 复杂查询的性能很依赖 `xBestIndex` 下推质量。
- 范围查询与多条件过滤在某些场景下仍弱于原生 B-Tree 路径。

---

## 3. LSM 存储引擎设计

### 3.1 解决的问题

LSM 主要解决写放大与随机写问题：
- 把随机写变顺序写（WAL append + MemTable flush）。
- 用后台 Compaction 做有序整理。
- 用 Bloom Filter + BlockCache 降低读路径放大。

### 3.2 写路径

1. 写请求进入事务，生成版本号与 WAL 记录。
2. WAL 顺序写入，保障崩溃恢复。
3. 数据写入 MemTable（分片 skiplist）。
4. MemTable 达阈值后 flush 成 L0 SSTable。
5. L0 累积到阈值后触发 compaction，逐层合并到更高层。

### 3.3 读路径

按可见性版本读取：
1. 查 MemTable（最新）
2. 查 Immutable MemTable
3. 查 L0（无序，通常需多表判断）
4. 查 L1+（有序，可借助范围定位）
5. 每步结合 Bloom Filter 和 BlockCache 减少实际磁盘 I/O

### 3.4 核心组件

- MemTable：分片 skiplist，支持并发与范围遍历。
- WAL：append-only + 校验，提供 crash recovery。
- SSTable：不可变有序文件，块索引、前缀压缩、可选压缩。
- VersionSet/Manifest：管理当前可见版本与元数据持久化。
- Compaction：后台归并，清理旧版本与 tombstone。
- BlockCache：热块缓存，减少重复读盘。

### 3.5 关键取舍

LSM 的典型权衡在 Axion 中非常明确：

- 写性能 vs 读放大：
  - 写入吞吐高（顺序写、批量刷盘）。
  - 读可能跨 MemTable + 多层 SSTable（依赖 Bloom/Cache 缓解）。
- 空间放大 vs 写优化：
  - compaction 前后阶段会短暂/持续存在冗余数据副本。
- 范围读 vs 点写：
  - 点写和高并发写优势明显。
  - 范围扫描在部分场景弱于 B-Tree 顺序结构。

---

## 4. MVCC 与事务层

文件：`/home/runner/work/axion/axion/src/mvcc/`

核心机制：
- 全局单调版本号。
- 事务开始获取读快照版本，提交获得写版本。
- 通过最近历史与可见版本判断冲突，提供可串行化语义。
- CommitBatcher 批量刷 WAL，降低 fsync 频率（Group Commit）。

收益：
- 在多线程写场景降低提交抖动。
- 保持一致性同时提升吞吐。

---

## 5. 可优化方向

### 5.1 近期可落地

1. Manifest 持久化优化：
   - 当前 JSON 全量更新可成为元数据瓶颈。
   - 可考虑二进制增量日志格式。

2. L0 compaction 并发化：
   - 在 key-range 可分割前提下并行归并，提高后台吞吐。

3. Bloom Filter 自适应：
   - 按层/按负载动态调整 bits-per-key，平衡内存与误判率。

4. 迭代器复用：
   - 减少高频创建/销毁开销，改善读路径尾延迟。

### 5.2 中长期方向

1. 可切换 compaction 策略（leveled/tiered）以适配不同负载。
2. WAL 在线回收与截断策略，降低长期运行体积。
3. 强化 `xBestIndex` 约束下推能力（范围谓词、多列条件）。
4. 可插拔压缩算法（LZ4/Zstd 等），按场景权衡 CPU 与空间。

---

## 6. 总结

Axion 的设计主线很清晰：
- 通过 LSM + MVCC 获得写密集场景性能与事务语义。
- 通过 SQLite VTab 降低使用门槛、复用 SQL 生态。
- 在工程上接受了 LSM 的典型代价（读/空间放大、compaction 复杂度），并用 Bloom、Cache、批量提交等手段进行补偿。

如果你的场景是高并发写入、可接受后台归并成本、又希望保留 SQL 能力，这套架构是合理且实用的。
