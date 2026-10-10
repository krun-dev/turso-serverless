# Turso Pageserver 对象存储能力与 S3 访问层

对象访问层统一采用 AWS S3 SDK。目录协议依赖通用的原子 `create_if_absent`，用不可变提交记录发布，不要求 `If-Match` 覆盖写。多个库合并为组级 pack，不引入逐批次元数据库读写。发布流程见 [跨库归档设计](turso-pageserver-pack-metadata-design.md)。

## 阿里云替代机制

| 能力 | OSS 的实际语义 | 本方案用途 |
| --- | --- | --- |
| `x-oss-forbid-overwrite: true` | 创建不存在的 key，同名对象存在则失败；bucket 已开启或暂停版本控制时无效 | 用于 commit 原子创建；不用于已有 CURRENT 的 CAS |
| AppendObject 的 `position` | 须等于当前对象长度，否则拒绝；只适用于 appendable object | 可构成按长度比较的追加日志，但接口专用，首版不采用 |
| CopyObject 的 source 条件头 | 检查复制源对象，不能比较目标 CURRENT 旧版本 | 不能替代目标 CAS |
| bucket 版本控制 | 保存同名对象的不同版本 | 不能阻止旧任务成为最新版本或替代发布仲裁 |
| GET/HEAD 条件请求 | 条件读取或检查已有对象 | 读后普通写存在竞争窗口，不能模拟原子创建 |

依据：[PutObject](https://www.alibabacloud.com/help/zh/oss/developer-reference/putobject)、[AppendObject](https://www.alibabacloud.com/help/en/oss/developer-reference/appendobject)、[CopyObject](https://www.alibabacloud.com/help/en/oss/developer-reference/copyobject)。阿里云说明 PUT 携带 If-Match 等条件头可能返回 `400 NotImplemented`，与本次兼容 endpoint 实测一致。[不支持的 PUT 条件头](https://www.alibabacloud.com/help/en/oss/user-guide/0017-00000245)。

## 访问层选择

业务依赖读取、Range、唯一对象写入、原子创建及提示覆盖写。pack、校验、提交链、恢复、owner 隔离与 GC 协议由我们实现。禁止先 HEAD 再 PUT 模拟创建条件，禁止忽略 Unsupported 后降级成覆盖写。

服务只使用一个 S3 客户端。标准 S3 使用 `--create-mode s3`，发送 `If-None-Match: *`；阿里云使用 `--create-mode oss-forbid-overwrite`，通过 S3 兼容端点发送签名的 `x-oss-forbid-overwrite: true`。PUT/GET/Range 使用同一客户端与对象布局。端点通过 `S3_ENDPOINT` 配置；已验证的阿里云兼容端点使用 virtual-hosted bucket 地址，path-style 被该端点拒绝。实现见 [storage.go](../pageserver/storage.go)。

Backend 接口允许以后替换访问层。驱动需要完整提供原子创建语义，不能在业务层绕过统一对象接口。capability 声明不能证明任意兼容服务或 bucket 的实际配置；需要实测强一致创建可见性、重复创建不覆盖及并发仅一个成功。大对象分片完成的条件语义尚需单独验证。

OSS bucket 须从未开启版本控制，不能处于 Enabled 或 Suspended。配置读取权限及禁止变更的管理约束需部署时设置；探针只证明当时有效。当前凭证查询版本控制返回 AccessDenied，没有确认或修改 bucket 配置。

唯一 pack/manifest key 使用普通 PUT，重试写相同字节并核验摘要；固定 revision 的 commit key 必须原子创建。不支持时拒绝发布，不借助 SQL 保存目录头，也不通过 LIST 选“最新文件”。

## 已完成验证

每次实验使用显式配置的桶和独立随机前缀，凭证不写代码、文档或证据。公开证据中的实际桶、内网端点和前缀已脱敏，包含存储绑定的旧本地恢复 fixture 已清理；结果与逐例记录保留。

- 内网 endpoint 的 PUT/GET/Range 成功；If-Match 与 If-None-Match PUT 返回 NotImplemented。增加防覆盖头后重复创建返回冲突，原内容保留。
- 统一 S3 客户端通过端点探针，并验证八个并发创建者只有一个成功；探针对象随后删除。
- 内存与实际 S3 兼容端点各通过 24 组归档、23 组冷读写和完整 20,905 例官方 WAL 对照。STRICT 补充通过；两条原样内存 VACUUM 上游限制保留，四个持久库变体通过，未新增失败或修改快照。

证据：[端点探针](evidence/s3-probe-latest.json)、[并发创建](evidence/s3-concurrent-oss-latest.json)、[内存验收](evidence/cloud-acceptance-memory-latest.json)、[S3 验收](evidence/cloud-acceptance-oss-latest.json)。范围见 [兼容性验证](turso-wal-pageserver-compatibility.md)。

正常双库批次合成一个 pack，以四次逻辑写发布。commit 创建限制为不超过 16 KiB，pack 不超过 64 MiB，未依赖分片条件创建。恢复允许丢失完整未归档尾部；损坏或部分事务不能安装为数据库。

AWS S3 本身尚未实测，标准条件创建模式通过本地 HTTP 协议测试。当前验收工具已替换，安全用例重新验证，历史完整语料证据保留原始运行来源。Keeper、节点接管、长期 GC 和生产容量尚未验收。
