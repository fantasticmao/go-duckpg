# go-duckpg

[![Actions Status](https://github.com/fantasticmao/go-duckpg/workflows/ci/badge.svg)](https://github.com/fantasticmao/go-duckpg/actions)
[![codecov](https://codecov.io/gh/fantasticmao/go-duckpg/branch/main/graph/badge.svg)](https://codecov.io/gh/fantasticmao/go-duckpg)
![Go Version](https://img.shields.io/github/go-mod/go-version/fantasticmao/go-duckpg)
[![License](https://img.shields.io/github/license/fantasticmao/go-duckpg)](LICENSE)

README [English](README.md) | [中文](README_ZH.md)

## 这是什么

go-duckpg 是一个 Go 语言的库，它为调用方提供的 [DuckDB](https://duckdb.org/) 数据库实现 [PostgreSQL](https://www.postgresql.org/) 的线协议，并且不实现 PostgreSQL 的 SQL 方言、系统目录，以及明文密码以外的认证方式。当前库版本为 [v1.0.0-beta.1](VERSION)。

![usage.png](docs/usage.png)

### 功能

- **简单查询**：执行一条 SQL，并返回结果行或命令完成标签。
- **扩展查询**：接受 Parse、Bind、Describe、Execute、Close、Sync 与 Flush。
- **池化会话**：在每个套接字存续期间持有一条来自连接池的 DuckDB 连接。
- **明文认证**：设置了 `Users` 时校验启动用户名和明文密码，为 nil 时接受任意客户端。
- **可选 TLS**：`TLS` 已设置时完成 `SSLRequest`，为 nil 时回答 `N`，并拒绝 `GSSEncRequest`。
- **查询取消**：当 `CancelRequest` 携带匹配的密钥时，中断正在执行的语句。
- **会话兼容**：保存 DuckDB 会拒绝的设置，并在客户端读到结果前改写 `version()`。
- **类型映射**：将每个 DuckDB 列通告为最接近的 PostgreSQL 类型，使用文本或二进制编码。

## PostgreSQL 线协议对比 Quack

远程访问一个 DuckDB 数据库时，可以使用 go-duckpg 实现的 PostgreSQL 线协议，也可以使用 [Quack](https://duckdb.org/docs/2.0/quack/overview)。下表逐项列出两者差异。

|            | PostgreSQL 线协议                                                                 | Quack                                                                 |
| ---------- | --------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| **客户端** | `psql`、JDBC、libpq 等 PostgreSQL 客户端直接连接。                                | 另一侧是 DuckDB 实例，包括浏览器中的 DuckDB-Wasm。                    |
| **SQL**    | 语句由 DuckDB 执行，`pg_catalog` 与 PostgreSQL 方言仍未实现。                     | 服务会话可见的 DuckDB 功能均可执行。                                  |
| **类型**   | 列通告为最接近的 PostgreSQL 类型，`LIST`、`STRUCT`、`MAP`、`ARRAY` 以 JSON 传递。 | `application/duckdb` 保持嵌套类型、小数与 interval 无损。             |
| **传输**   | 消息使用调用方指定端口上的 PostgreSQL 前端/后端协议。                             | 消息使用 HTTP 或 HTTPS，默认端口为 9494，前面可以放置常规反向代理。   |
| **结果**   | 该连接返回一条按行的文本或二进制流。                                              | 握手之后一次请求完成查询，大结果通过可并行的 `FETCH` 分块返回。       |
| **认证**   | `Users` 非 nil 时校验明文密码，`Config.TLS` 非 nil 时完成 TLS。                   | 默认必须携带 token，默认只绑定 localhost，非本地 URI 默认使用 HTTPS。 |
| **部署**   | 调用方打开 DuckDB 并把 `*sql.DB` 交给 `Listen`，每个套接字占用池中一条连接。      | `quack` 扩展在持有数据的 DuckDB 进程内提供服务。                      |

### 本机基准

[基准](benchmarks/README.md) 在同一台机器上比较进程内 duckdb-go、go-duckpg 与 Quack。下面的数字来自 Go 1.27.1、DuckDB v2.0.0 和 8 个 CPU，表为 10 万行。每个倍数是中位耗时除以进程内耗时。

- **小结果**：对于 `SELECT 1`、全表聚合、点查和单行插入，go-duckpg 是进程内 duckdb-go 的 1.1–1.7 倍，Quack 是 2.0–3.7 倍。
- **批量扫描**：10 万行扫描中，Quack 为进程内的 1.2 倍，go-duckpg 为 2.9 倍。
- **新建会话**：打开一个 go-duckpg 会话约 0.12 ms，一次 Quack `ATTACH` 加 `CONNECT` 约 2.9 ms。

## 下载与安装

```bash
go get github.com/fantasticmao/go-duckpg
```

- **Go 工具链**：构建和运行该模块需要 Go 1.26。
- **原生构建**：DuckDB 驱动为 darwin 与 linux 的 amd64、arm64，以及 windows 的 amd64 提供 `libduckdb`。

## 快速开始

打开一个 DuckDB 数据库，并把句柄传给 `Listen`。

```go
package main

import (
	"database/sql"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/fantasticmao/go-duckpg/duckpg"
)

func main() {
	db, err := sql.Open("duckdb", "test.db")
	checkError(err)

	err = db.Ping()
	checkError(err)

	err = duckpg.Listen(db, ":15432", duckpg.Config{
		Users:         map[string]string{"duckpg": "123456"},
		ServerVersion: "15.0",
	})
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
```

将 PostgreSQL 客户端指向该地址。URL 中的数据库名不起作用。`Users` 为 nil 时，用户名同样被忽略。

```bash
psql "postgresql://duckpg:123456@127.0.0.1:15432/duckdb?sslmode=disable"
```

> [!IMPORTANT]
> `Listen` 在无法再接受连接时返回，在此之前会持续接受连接。

> [!NOTE]
> 示例中的密码以明文传输。在该端口可从不可信网络访问之前设置 `Config.TLS`，并仅在 `TLS` 为 nil 时保留 `sslmode=disable`。

## 工作原理

go-duckpg 将连接处理、会话状态、语句兼容和值编码分开。

### 架构

- **线协议服务**：接受 TCP 套接字，完成启动，并分发每条前端消息。
- **客户端会话**：保存 DuckDB 连接、预编译语句、已绑定的 portal、事务状态和取消密钥。
- **兼容处理**：放行语句、依据会话状态直接应答，或在执行前改写语句。
- **类型编码**：将每个 DuckDB 列映射为 PostgreSQL OID，并以文本或二进制编码该值。

### 主流程

go-duckpg 在任何 SQL 之前读取启动消息。`Config.TLS` 已设置时，`SSLRequest` 转为 TLS 握手；未配置的 `SSLRequest` 和每一条 `GSSEncRequest` 都会收到字节 `N`。`CancelRequest` 不进入查询循环，并取消进程标识与密钥均匹配的会话上的查询。

`Config.Users` 为 nil 时，go-duckpg 跳过认证。`Config.Users` 非 nil 时，go-duckpg 要求明文密码，并仅在启动用户名是其中一个键且回复与对应值一致时接受该连接。随后 go-duckpg 发送 `AuthenticationOk`、参数状态、`BackendKeyData` 和 `ReadyForQuery`。

简单 `Query` 与扩展协议中的 Parse、Bind、Describe、Execute、Sync 序列共用同一步分类。仅修改已保存 PostgreSQL 设置的语句在本地完成。经过改写的语句以及其余语句，都在该会话的 DuckDB 连接上执行。应答以 `CommandComplete` 和 `ReadyForQuery` 结束。扩展协议出错后，go-duckpg 丢弃消息直到 `Sync`，而 `Terminate` 仍会关闭会话。

```mermaid
flowchart TD
    accept[接受 TCP 套接字] --> startup[读取启动消息]
    startup --> kind{消息种类}
    kind -->|已设置 TLS 的 SSLRequest| tls[完成 TLS 握手]
    kind -->|TLS 为 nil 的 SSLRequest| decline[发送 N]
    kind -->|GSSEncRequest| decline
    tls --> startup
    decline --> startup
    kind -->|CancelRequest| cancel[取消匹配的查询]
    kind -->|StartupMessage| users{是否设置 Users}
    users -->|是| password[匹配用户名与明文密码]
    users -->|否| ready[发送 AuthenticationOk 与 ReadyForQuery]
    password --> ready
    ready --> query[接收简单查询或扩展查询]
    query --> classify{语句如何分类}
    classify -->|已保存的设置| complete[发送 CommandComplete 与 ReadyForQuery]
    classify -->|改写或放行| duckdb[在会话连接上执行]
    duckdb --> complete
    complete --> query
```

## 常见问题

### 为什么 PostgreSQL 系统目录或方言查询会失败？

语句由 DuckDB 执行，go-duckpg 不实现 `pg_catalog`，也不实现 PostgreSQL 的 SQL 方言。

### 为什么连接 URL 中的数据库名没有作用？

启动消息提供用户名，go-duckpg 不读取数据库名。任意数据库名都有效，哪些客户端可以连接由 `Users` 决定。

### 为什么 SCRAM 和 MD5 会失败？

`Users` 已设置时，go-duckpg 要求明文密码；`Users` 为 nil 时，go-duckpg 跳过认证。客户端接受明文密码，并在端口可从不可信网络访问之前设置 `Config.TLS`。

## 变更记录

重要变更列在 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

go-duckpg 以 [MIT License](LICENSE) 发布。
