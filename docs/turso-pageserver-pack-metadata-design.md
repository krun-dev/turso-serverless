# Turso Pageserver 跨库归档与元数据库设计

首版采用单 Pageserver、本地持久 WAL 和异步 S3 归档。多个独立小库的增量合并成共享 pack，目录按归档组发布，以减少 S3 数据与元数据请求。元数据库仅保存节点、放置路由和必要的管理操作，访问频率随管理事件增长。正常读页、WAL 提交、归档、目录发布和 compaction 不访问元数据库。S3 保存可独立恢复的 manifest 与数据对象。

首版 pack、S3 兼容归档、完整前缀恢复和后台 pack compaction 已有实验原型；WAL 完整事务边界、不可变对象发布、基线换代与旧版本读取沿用现有实验。元数据库、自动切换、页面层 compaction、GC 和生产资源预算尚未实现。后续 Keeper 复制和自动切换见 [Keeper 与 S3 目标设计](turso-pageserver-keeper-s3-design.md)。

## 首版提交与恢复边界

每个数据库保留独立 `db_id / timeline_id / wal_generation / data_seq`。`data_seq` 在该历史内单调增长，跨 WAL 换代继续递增；它不是跨库事务号。归档组 `archive_group_id` 决定打包和目录发布单元，与物理节点 ID、Keeper group ID 分开。

Pageserver 校验完整、连续的事务 WAL，本地 fsync 并持久发布恢复位置后确认；S3 上传不在普通提交的等待路径上。进程重启从本地恢复，本地盘永久丢失时从 S3 最后完整发布的目录恢复，各库可能丢失未归档的完整事务尾部。损坏或缺失的已发布对象不能通过跳过日志、补零或混用页面版本修复。

首版不做自动接管。切换前停止旧写入者及旧发布进程、撤销其访问资格，再更换 owner/epoch；从较旧 S3 位置恢复时重启 Compute 并创建新历史身份。数据库租约或健康探测不能单独证明旧存储写入者已隔离。引入 Keeper 后，在线提交与版本批准由 Keeper 确认，元数据库中的记录不能替代 quorum。

## 元数据库的使用边界

元数据库可采用可靠部署的 PostgreSQL，应用层通过独立 `PlacementStore` 接口访问。两类管理记录为必需，跨进程恢复的迁移任务需要时再增加第三类；不建立归档批次、目录 revision、页面、事务或对象的关系表。

| 记录 | 保存内容 | 更新时机 |
| --- | --- | --- |
| `storage_placements` | db/timeline 到归档组的映射、组到 owner 的映射、路由版本、管理 epoch、保留策略 | 创建、删除、放置切换和策略变更；与库规模相关但与 SQL 请求数量无关 |
| `storage_nodes` | 节点身份、地址、管理状态 | 注册、配置变更；运行健康不逐请求落库 |
| 可选 `management_operations` | 管理请求 ID、迁移源/目标、步骤与幂等结果 | 迁移、扩缩容等需要跨重启恢复的低频操作；普通归档、compaction、GC 不建行 |

页面 `(db, timeline, page, data_seq)` 索引、pack offset/length、逐事务归档位置和长期对象引用保存在 manifest/pack 索引中，加载为本地可重建缓存。存储 payload、原始 WAL 和提交去重结果保存在日志及恢复记录中；不以外部数据库逐 SQL 写入实现去重。归档批次的去重使用同一 job/upload ID 和内容摘要。

约束如下：

- 已打开数据库的正常读页和 WAL 提交产生零次外部元数据库访问。路由缓存首次未命中或重新打开数据库时可查询，watch 按组/节点复用，不为每个闲置库维持独立数据库连接。
- 每批归档、目录发布和普通 compaction 产生零次元数据库读写。提交位置、归档位置、目录 revision 与对象引用均由日志/S3 manifest 保存，不回写管理数据库。
- 健康与资源水位保存在调度器内存；通过 watch 推送和定期汇总观测，不把每次心跳、调度评分或作业进度写成数据库行。确需管理持久记录时按操作更新，读写有分页和连接预算。
- 管理数据库仅定位归档组和记录管理身份；S3 不可变 commit 记录是已发布恢复根，CURRENT 只是可重建的定位提示。丢失管理数据库后可导入已知组的 S3 恢复目录；导入不能恢复尚未归档的提交，也不能复活旧 owner/epoch。

元数据库暂不可用时，现有 owner 可以在既有授权和本地空间预算内继续读写、归档、目录发布和 compaction；暂停新 owner 授权及管理切换。新会话是否可打开取决于合法路由缓存。达到授权期限或存储积压上限时等待或失败，不能绕过协调自行接管。GC 首版关闭；以后启用时由存储侧引用与删除协议决定安全性，数据库可用性不是删除依据。

## 跨库 pack 与 S3 布局

同租户、相近保留策略的小库组成归档组。小型 WAL 和必要的基线/代际元数据合并上传；目标部署的大型基线独立存储，后续小型 image/delta 也可放入 pack。对象采用独立块压缩、块校验和 footer 索引，支持定位数据库记录和按范围读取。

```text
v1/tenants/{tenant_id}/archive-groups/{archive_group_id}/
  packs/{pack_id}__{sha256}__u{upload_id}.pack
  catalog/manifests/r{revision:020}__{sha256}__u{upload_id}.json
  catalog/commits/r{revision:020}.json
  catalog/CURRENT.json
  large-bases/{db_id}/{timeline_id}/{base_id}__{sha256}__u{upload_id}.image
```

上图是目标布局；当前原型 pack key 为 `{sha256}__u{upload_id}.pack`，所有大小适合实验预算的基线也分块进入 pack，`large-bases` 尚未实现。

pack 记录包含数据库/历史身份、writer epoch、WAL generation、前驱与提交位置、事务边界、长度和摘要。manifest 按库保存基线引用、连续日志范围、可恢复位置、数据库大小与历史引用；每个 extent 指向 `object_key / offset / length / checksum`。读取不从目录名称推断数据库归属或完整性。

原型的 footer 是内容 SHA256 到压缩块 extent 的索引，数据库归属、各代基线/WAL 对象及提交位置保存在组级 manifest；尚未加入 writer epoch 或独立 timeline 的 pack 记录格式。相同内容块可被不同库引用，归属不能仅靠块摘要确定。

首版每组使用一份完整 manifest，组容量有界；后续按实测元数据大小引入周期性目录 checkpoint 与批量增量，重建链长度有上限。不为每个库每轮单独上传 manifest 或更新 CURRENT。读取和重启通过组级提交记录和 manifest 得到精确对象 key。

大小阈值和最长等待时间共同触发上传；无新增数据的组不空发对象。大库增量达到阈值可以单独成包，避免小库等候大事务。等待时间不是故障下的最大丢失窗口，必须观测归档积压并设置背压。

## 批量目录发布与恢复

首选通用的原子 `create_if_absent`，不要求可变对象的 ETag CAS。业务接口不出现厂商请求头；访问层使用 AWS S3 SDK；OSS 也走 S3 兼容端点，只在原子创建时配置防覆盖头。S3 兼容 endpoint 只表明部分 API 兼容，不能证明条件写入有效。具体能力和实测见 [对象存储能力与 S3 访问层](turso-pageserver-object-storage-design.md)。

1. 单个组目录发布者加载最新 commit 和 manifest，在本地持久记录上传计划及输入 revision。重试复用 upload ID 和摘要；GC 关闭时未发布对象保留，首版不额外为每批向 S3 或元数据库登记 intent。
2. 上传不可变 pack、大基线和组级 manifest，校验内容和覆盖。manifest 中每个库必须形成完整、连续的恢复链；大事务跨对象时，全部对象完成后才能发布其提交边界。
3. 发布者在组内串行校验新目录，保持每个库的归档前缀及合法历史引用。异步 compaction 的输入过期时重新合并新增引用或重试，不能覆盖前台写入的尾部。
4. 原子创建固定 key `catalog/commits/r{N+1:020}.json`。记录包括组/历史身份、revision、前驱 commit 摘要、manifest key 与 SHA256。一个 revision 只有一个记录可以创建成功，这一步是 S3 发布点。已存在时读回核验：同一内容视为重试成功；不同内容为冲突，重新加载和校验输入，不能只换 revision 继续发布旧目录。
5. 发布成功后普通覆盖写 CURRENT，保存 commit revision/key/digest，帮助快速定位；提示写失败不撤销 commit。推进本地归档位置，位置从 manifest 读取，不回写管理数据库。超时丢回复时读回固定 commit key，先确认结果，再决定重试。

正常单 pack 批次为一次 pack 写、一次 manifest 写、一次 commit 创建、一次 CURRENT 提示写，共四次逻辑写操作，均按组批量进行。相比 CAS CURRENT 方案增加一次小对象写，不随 SQL 或库数量增加。大包分片、重试、语义探针另计实际 HTTP 请求。没有新增数据的组不上传。未来确有成本收益时可单独引入已验证的 CAS 发布模式；同一归档组不能自动混用协议。

恢复先 GET CURRENT；存在时验证其指向的不可变 commit 摘要和组身份，再依次 GET 固定 key N+1、N+2，直到明确 NotFound。CURRENT 缺失时从 revision 1 开始；正常提示及时更新时只需额外一次下一 revision 的不存在检查，无 LIST。commit 必须连续，前驱摘要一致；验证最终 manifest 及所有依赖对象后才构建恢复数据库。权限错误、超时或摘要错误不能当作 NotFound。发布记录保留，不能把中间缺号当成允许跳过的日志。

CURRENT 被迟到请求写成较旧 revision 时，恢复通过后续 commit 找到较新目录；commit 创建后、提示更新前崩溃同样可恢复。pack/manifest 上传后、commit 创建前崩溃，未发布对象不构成恢复根。提示长时间陈旧或丢失会产生线性追赶 GET；首版保留这一限制，记录追赶长度和延迟，达到预算时明确失败或由管理恢复流程处理，不返回未经确认的“最新”状态。以后优化 checkpoint/分段目录必须保留缺失提示的发现协议，不能仅靠新 checkpoint 文件名。

原子创建只解决某个 revision 的发布竞争，不验证 writer/owner 资格，也不阻止具有权限的旧 owner 抢先发布下一 revision。首版固定单发布者，管理切换先隔离旧写入者及发布者。禁止删除和覆盖 commit；CURRENT 不作为 GC 的唯一保护根。目标部署的数据上传身份不能创建 catalog/commits、更新 CURRENT 或 DELETE，权限集中于目录发布者；当前原型使用同一测试身份，未实现权限拆分。引入 Keeper 后，目录发布者只创建 Keeper 批准的 commit，批准仍不经过外部管理数据库。

本地盘永久丢失时，从已知归档组的 commit 恢复根加载 manifest 及精确对象引用。引用缺失或摘要不匹配时协调重试或报错，不扫描并选择文件名最大的 pack。元数据库整体丢失后，从已知前缀导入各组完整恢复根，先隔离旧服务并重建管理身份；未知归档组发现和未发布孤儿对象盘点允许使用离线 LIST。

## 异步 compaction 与安全回收

页面层 compaction 合并 delta 或生成 image；pack compaction 重写共享对象中仍被引用的数据。两者后台运行，根据缓存的 manifest 选择输入，本地记录整理计划，固定范围，生成新对象后按批量目录发布协议提交，不查元数据库、不登记外部作业行。计划可重建，尚未发布的工作可重做。它们保留各库的数据版本和必要历史，不修改 SQL 事务顺序，不替代引擎 checkpoint/VACUUM 的同步与 WAL 换代屏障。

读者固定 snapshot 与目录引用，旧读继续使用受保护对象；新读使用新目录。正常写入继续追加新日志，目录发布只占短暂协调时间。限制后台并发、CPU、I/O、网络和缓存预算，前台优先。compaction 可能增加延迟和 GET/PUT 流量，应按对象数量、碎片率和读取放大触发。

原型只整理共享 pack 的物理存放，并不生成新的页面 image/delta 层；对象分块为 64 KiB，pack 上限 64 MiB，整批在内存构建。完整恢复先下载被引用块并原子安装新根；在线 ColdStore 则读取已发布页面索引，按需 Range GET 取块。写入采用稀疏覆盖区，FULL sync、checkpoint 和 WAL 换代沿用本地权威发布；新归档快照可同时引用旧的远端对象和新的本地对象。`watch` 归档 worker 独立于 SQL 服务进程运行，固定间隔触发，按大小触发与正式调度仍待实现。

GC 首版关闭。后续启用时，保护当前 S3 根、保留恢复根、读 pin、分支、上传 intent、待发布目录和迁移任务。共享 pack 只有所有库的有效 extent 都已转移或过期，且没有合法旧引用时才能删除；删除单个库不意味着删除整包。原始 WAL 被新基线取代前，还需证明恢复、代际元数据和去重状态完整覆盖。

将删除批准与新目录引用在存储侧串行协调，生成批量、可重试的删除任务；进入删除状态的 key 不可再被新目录引用，重新生成内容使用新上传 ID。删除资格、受保护引用和 intent 必须保存在可恢复的存储状态中；不以外部 SQL 数据库保存逐对象引用计数。待发布目录、已发布 commit 及保留历史同时作为保护根，不能仅按最新本地候选目录计算存活对象。活跃读 pin 首版保存在单 owner 进程，GC 关闭保证安全；启用 GC 或自动接管前必须完成 pin 的失效与恢复协议，不能为省元数据库请求省掉这条边界。

## LIST 与容量验收

正常打包上传、按版本读、compaction 和已批准对象删除均使用明确 key，不需要 LIST。路由缓存提供归档组定位；组内对象与目录由 manifest 自身定位，不需要把对象目录镜像到元数据库。已知组 GET CURRENT 定位并追赶明确 commit key，再加载恢复目录。LIST 用于离线盘点、孤儿对象检查和未知组发现，不定期全桶扫描来生成 compaction 输入。

必须记录每个归档批次的对象 PUT/GET/LIST、元数据库事务数/修改行数、manifest 字节数、归档积压，以及 compaction 读写放大。测试覆盖多库连续前缀、大事务多对象、发布各阶段崩溃、数据库与 S3 单侧不可用、旧任务迟到、长读跨 compaction、VACUUM 换代，以及 GC 对待发布目录/共享 pack 的保护。正常数据读写、归档和 compaction 的外部数据库访问数必须为零；元数据库请求只随管理事件和路由缓存补充增长。S3 请求数量随 pack 批次增长，不随每条 SQL 或每页增长。

## Neon 参考边界

Neon 的 Storage Controller 使用 PostgreSQL 保存 generation、配置和节点管理信息；多数运行状态保存在内存。页面恢复目录由 S3 index 文件保存。其 compaction 从内存 LayerMap 选择输入，正常流程无需扫描 bucket；加载 generation 索引时先尝试明确 key，仅在必要时 LIST 对应前缀。这些分工可借鉴，跨独立 Turso 数据库的共享 pack 和组级目录是本方案的设计。

参考：[Controller 持久化](https://github.com/neondatabase/neon/blob/fa504217c61bbcaf5c512d75830564541f917f8f/storage_controller/src/persistence.rs)、[远端目录](https://github.com/neondatabase/neon/blob/fa504217c61bbcaf5c512d75830564541f917f8f/pageserver/src/tenant/remote_timeline_client.rs)、[索引加载](https://github.com/neondatabase/neon/blob/fa504217c61bbcaf5c512d75830564541f917f8f/pageserver/src/tenant/remote_timeline_client/download.rs)、[compaction](https://github.com/neondatabase/neon/blob/fa504217c61bbcaf5c512d75830564541f917f8f/pageserver/src/tenant/timeline/compaction.rs)。

## 2026-10-10 原型验收

归档验证使用两个真实 Turso FULL WAL 数据库，内存对象后端与阿里 OSS 内网后端分别执行相同的 24 组故障与恢复用例，全部通过；原有 10 + 2 + 5 组本地回归也通过。多库一批仅一个共享 pack；正常小对象批次四次逻辑写；不调用 LIST 或外部元数据库。测试覆盖提示缺失/迟到、commit 前后故障与持久计划重开、归档期间 SQL 继续提交、WAL 部分尾部排除、VACUUM 换代、pack compaction、保留旧归档 revision、过期整理输入拒绝、块损坏拒绝安装，以及删除全部本地 DB/协调文件后仅从对象存储恢复。

`history_id` 在原型内等于传入数据库 ID，固定归档组只有一条历史，使用代际和 frame 位置；独立 timeline 身份、跨换代 data_seq、时间到位置索引及在线分支需要后续实现。恢复到旧位置后必须隔离旧 Compute，并换新归档前缀/历史，不能将旧本地根继续归档到原历史。当前支持按已归档 revision 恢复，不等同于任意时间点回溯。

执行方式与范围见 [实验说明](../README.md)，证据见 [内存归档测试](evidence/archive-memory-latest.json)、[OSS 归档测试](evidence/archive-oss-latest.json) 和 [OSS 在线冷读写测试](evidence/cold-oss-latest.json)。原型要求本地已有经过 `baseline`/权威 WAL 发布初始化的 manifest；不能直接归档任意运行中 DB/WAL 文件。完整云端官方 runner 已接入 ColdStore，范围与最终逐例结果见 [兼容性验证](turso-wal-pageserver-compatibility.md)。

内存与实际 S3 兼容端点的 24 组归档、23 组冷读写及完整官方 WAL 语料均通过验收，未增加上游失败。独立 worker 已验证对在线冷库归档，其对象来自固定目录，缓存独立于 SQL 服务。当前入口见 [实验说明](../README.md)，完整结果见 [S3 验收](evidence/cloud-acceptance-oss-latest.json)。
