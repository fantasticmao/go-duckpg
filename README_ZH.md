# go-duckpg

[![Actions Status](https://github.com/fantasticmao/go-duckpg/workflows/ci/badge.svg)](https://github.com/fantasticmao/go-duckpg/actions)
[![codecov](https://codecov.io/gh/fantasticmao/go-duckpg/branch/main/graph/badge.svg)](https://codecov.io/gh/fantasticmao/go-duckpg)
![Go Version](https://img.shields.io/github/go-mod/go-version/fantasticmao/go-duckpg)
[![License](https://img.shields.io/github/license/fantasticmao/go-duckpg)](LICENSE)

README [English](README.md) | [中文](README_ZH.md)

## 这是什么

go-duckpg 是一个 Go 语言的库，它为 [DuckDB](https://duckdb.org/) 实现 [PostgreSQL](https://www.postgresql.org/) 的线协议。PostgreSQL 客户端通过该端点查询调用方提供的 DuckDB 数据库，该库不实现 PostgreSQL 的 SQL 方言、系统目录，以及明文密码以外的认证方式。

![usage.png](docs/usage.png)

库版本为 [v1.0.0](VERSION)。在线协议上，go-duckpg 将 `server_version` 通告为 `15.0`，并把 `version()` 改写为 `PostgreSQL 15.0 (DuckDB 2.0.0)`。

## 功能

- **简单查询**： 执行一条 SQL，并返回结果行或命令完成标签。
- **扩展查询**： 接受 Parse、Bind、Describe、Execute、Close、Sync 与 Flush。
- **池化会话**： 在每个套接字存续期间持有一条来自连接池的 DuckDB 连接。
- **明文认证**： 配置了密码时校验明文密码，密码为空时接受任意客户端。
- **可选 TLS**： `TLS` 已设置时完成 `SSLRequest`，为 nil 时回答 `N`，并拒绝 `GSSEncRequest`。
- **查询取消**： 当 `CancelRequest` 携带匹配的密钥时，中断正在执行的语句。
- **会话兼容**： 保存 DuckDB 2.0 会拒绝的设置，并把 `version()` 改写为 `PostgreSQL 15.0 (DuckDB 2.0.0)`。
- **类型映射**： 将每个 DuckDB 列通告为最接近的 PostgreSQL 类型，使用文本或受支持的二进制格式。

## 下载与安装

```bash
go get github.com/fantasticmao/go-duckpg
```

- **Go 工具链**： 构建和运行该模块需要 Go 1.26。
- **原生构建**： DuckDB 驱动为 darwin 与 linux 的 amd64、arm64，以及 windows 的 amd64 提供 `libduckdb`。

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
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}

	err = duckpg.Listen(db, ":15432", duckpg.Config{
		Password: "123456",
		TLS:      nil,
	})
	if err != nil {
		panic(err)
	}
}
```

将 PostgreSQL 客户端指向该地址。URL 中的用户名和数据库名会被忽略。

```bash
psql "postgresql://duckpg:123456@127.0.0.1:15432/duckdb?sslmode=disable"
```

> [!IMPORTANT]
> `Listen` 会持续接受连接，直到进程退出；当监听器无法再接受连接时，它才返回。

> [!NOTE]
> 示例中的密码以明文传输。在该端口可从不可信网络访问之前设置 `Config.TLS`，并仅在 `TLS` 为 nil 时保留 `sslmode=disable`。

## 工作原理

go-duckpg 将连接处理、会话状态、语句兼容和值编码分开。

### 架构

- **线协议服务**： 接受 TCP 套接字，完成启动，并分发每条前端消息。
- **客户端会话**： 保存 DuckDB 连接、预编译语句、已绑定的 portal、事务状态和取消密钥。
- **兼容处理**： 放行语句、依据会话状态直接应答，或在执行前改写语句。
- **类型编码**： 将每个 DuckDB 列映射为 PostgreSQL OID，并以文本或二进制编码该值。

### 主流程

go-duckpg 在任何 SQL 之前读取启动消息。`Config.TLS` 已设置时，`SSLRequest` 转为 TLS 握手；未配置的 `SSLRequest` 和每一条 `GSSEncRequest` 都会收到字节 `N`。`CancelRequest` 不进入查询循环，并取消进程标识与密钥均匹配的会话上的查询。

`Config.Password` 非空时，go-duckpg 要求 `AuthenticationCleartextPassword`，并将回复与配置的密钥比较。随后 go-duckpg 发送 `AuthenticationOk`、固定的参数状态、`BackendKeyData` 和 `ReadyForQuery`。

简单 `Query` 与扩展协议中的 Parse、Bind、Describe、Execute、Sync 序列共用同一步分类。仅修改已保存 PostgreSQL 设置的语句在本地完成。经过改写的语句（包括 `version()`）以及其余语句，都在该会话的 DuckDB 连接上执行。应答以 `CommandComplete` 和 `ReadyForQuery` 结束。扩展协议出错后，go-duckpg 丢弃消息直到 `Sync`，而 `Terminate` 仍会关闭会话。

```mermaid
flowchart TD
    accept[接受 TCP] --> readMsg[读取启动消息]
    readMsg --> kind{消息种类}
    kind -->|已配置 TLS 的 SSLRequest| handshake[完成 TLS 握手]
    kind -->|未配置 TLS 的 SSLRequest| replyN[回复 N]
    kind -->|GSSEncRequest| replyN
    handshake --> readMsg
    replyN --> readMsg
    kind -->|CancelRequest| cancel[取消匹配的查询]
    kind -->|StartupMessage| auth{是否设置密码}
    auth -->|是| password[要求明文密码]
    auth -->|否| ready[AuthenticationOk 与 ReadyForQuery]
    password --> ready
    ready --> query[简单查询或扩展查询]
    query --> classify{兼容分类}
    classify -->|本地设置| done[CommandComplete 与 ReadyForQuery]
    classify -->|改写或放行| exec[在会话连接上执行 SQL]
    exec --> done
    done --> query
```

## 常见问题

### 为什么 version() 显示 PostgreSQL 15.0？

PostgreSQL 客户端期望得到 PostgreSQL 版本字符串，而 DuckDB 不返回该字符串。go-duckpg 将 `SELECT version()` 改写为 `PostgreSQL 15.0 (DuckDB 2.0.0)`，库版本则仍以 [VERSION](VERSION) 的内容为准。

### 为什么任意用户名都能连接成功？

go-duckpg 不读取启动消息中的用户或数据库字段。将 `Password` 留空即可接受任意客户端，或设置该值，使比较只针对明文密钥。

### 为什么 SET extra_float_digits 会成功？

DuckDB 2.0 会拒绝客户端在连接时发送的若干 PostgreSQL 参数。go-duckpg 在会话中保存 `extra_float_digits`、`application_name`、`client_encoding`、`standard_conforming_strings`、`DateStyle`、`TimeZone`、`IntervalStyle` 和 `bytea_output`，并将其余设置（例如 `search_path`）转发给 DuckDB。

## 许可证

go-duckpg 以 [MIT 许可证](LICENSE) 发布。
