# 运行时列生成模式 — 设计文档

**状态**: Draft v2 · **作者**: joechenrh · **日期**: 2026-04-21

---

## 1. 背景

目前 data-writer 通过 SQL `CREATE TABLE` 语句派生 `ColumnSpec`，并支持在列的 `COMMENT` 里写少量旋钮（`null_percent` / `max_length` / `mean` / `stddev` / `order`）来定制生成。每种 SQL 类型的生成逻辑写死在 `generate*Parquet` 函数里。

真实性能测试（例如 PingCAP-QE/perftest 的 `bingoplus/prod_dc`）要复刻客户生产表的统计分布，观测到的生成模式远超当前工具的能力：加权枚举 + 长尾兜底、前缀+序号、同行派生、条件分布、模板化多字段等。这类复杂分布目前只能靠外部 Lua 脚本通过 sysbench 写 INSERT 实现，没法利用 data-writer 的高吞吐批量写 parquet/CSV 管线（实测 2 GB/s 写 S3，8C/16G EC2）。

## 2. 目标 / 非目标

### 2.1 目标

- **G1** — 用户能在配置里为指定列声明生成策略，无需改 `CREATE TABLE` SQL，也无需改工具内部代码。
- **G2** — 覆盖 perftest 脚本里观察到的 9 种模式（常量、均匀、加权枚举+尾部、前缀+序号、拼接多段、模板多字段、同行派生、条件分布、时间窗口）。
- **G3** — 当列没有声明运行时策略、或策略只用声明式原语时，生成性能不低于当前 batch 管线（保持 2 GB/s 写 S3）。
- **G4** — 为声明式覆盖不了的场景提供 Go 函数逃生舱，性能**同样 ns 级**（避免 Lua/yaegi 的 μs 级损耗）。
- **G5** — 向后兼容：现有 SQL COMMENT 旋钮继续生效；没有新配置的列行为不变。
- **G6** — 用户写 Go 函数时**尽量少写样板代码**：通过 `data-writer scaffold` 生成骨架，通过 bootstrap 期 AST 扫描自动注册，通过类型化 `ctx` 访问器去掉断言。

### 2.2 非目标

- **跨行依赖（E2）** 与 **跨表外键（E3）**：本设计只做**同行依赖（E1）**。跨行/跨表要引入可在 goroutine 间共享的 ID 池，与现有 lock-free pipeline 耦合较深，延后到独立设计。
- **运行时脚本（Lua / JS / yaegi）**：已在设计讨论中排除——μs 级每行调用开销会把 S3 吞吐从 2 GB/s 打到 500 MB/s 甚至更低。Go 静态注册在本项目的任务管线下无成本地取代了它。
- **热更新**：配置在启动时加载一次，运行中不变。

## 3. 现状速览

- `src/spec.go`：`ColumnSpec` + SQL COMMENT 解析。
- `src/data_gen.go`：每种 SQL 类型一个 `generate*Parquet` 函数，写入预分配的类型化 batch buffer。
- `src/parquet_writer.go::writeNextColumn`：**列主序**循环——每列单独从头跑到尾，内部以 `BatchSize=10` 行为粒度滚动。
- `src/csv_writer.go` / `streaming.go`：CSV 路径是**行主序**。

关键约束：parquet 的 `SerialRowGroupWriter.NextColumn()` 要求列按 schema 顺序串行写，不能交叉。这会影响"同行引用"的实现，见 §8。

## 4. 总体设计：两层架构 + 任务管线集成

```
┌───────────────────────────────────────────────────────────────┐
│ Layer 1: 声明式 DSL（TOML + Lua 风味表达式，纯 Go 执行）         │
│                                                               │
│   字面量 / uniform_int / uniform_float / gauss /               │
│   choose / switch / time_between /                            │
│   stdlib: sha256·md5·hex·b64 / substr·lower·upper /           │
│           zipf·pareto·lognormal / dt.add·dt.trunc             │
│                                                               │
│   → 覆盖 ~95% 场景，ns 级 batch 执行                           │
├───────────────────────────────────────────────────────────────┤
│ Layer 2: Go 函数逃生舱（静态注册，编译期烘焙）                  │
│                                                               │
│   用户写 generators/*.go → scaffold 生成骨架                  │
│   → bootstrap AST 扫描自动注册                                 │
│   → go build 后与内置原语同级调用                              │
│   → ~ns 级，无 VM 调用开销                                     │
└───────────────────────────────────────────────────────────────┘
                            ▲
                            │ 集成于现有任务管线：
                            │ EC2 bootstrap 把用户 .go drop 进项目，
                            │ codegen + go build，跑任务
```

**分层的好处**：

1. 常见分布（加权枚举、模板字符串、哈希、常用分布）走纯 Go 原语 + DSL stdlib，直接写入类型化 buffer。
2. 真正长尾的复杂逻辑交给用户写 Go，走静态注册路径，性能和内建原语同级。
3. 用户的工作量排序：**字面量 → DSL 表达式 → DSL 小节 → 填 Go 函数体**。

## 5. 配置格式

### 5.1 `[columns]` 字典

`[columns]` 是一个字典，键是列名（对应 `CREATE TABLE` 里的列），值按以下规则解释：

| 值的 TOML 形式                | 语义                                          |
| ----------------------------- | --------------------------------------------- |
| 标量字面量（string/int/float/bool） | 常量列                                     |
| `"= expr"`                    | 表达式列（见 §6）                               |
| `"@go"`                       | Go 函数列，函数名默认等于列名（见 §9）          |
| inline table `{ ... }`        | 完整 spec，可加修饰符                           |
| `[columns.<name>]` 小节       | 多值规则（`choose` 长列表 / `switch` / `time_between`），见 §6 |

### 5.2 完整示例

```toml
[common]
path   = "s3://my-bucket/orders/"
prefix = "orders"
rows   = 10000000
format = "parquet"

[columns]
# ─── 常量（字面量）───
product_id   = "C66"
currency     = "PHP"
bonus_amount = 0

# ─── 简单表达式（一行）───
id           = "= uniform_int(1e9, 3e9) .. uniform_int(1e8, 9e8)"
user_id      = "= uniform_int(1, 5000000)"
login_name   = "= 'bingoplus_' .. uniform_int(1, 12054528)"
cur_ip       = "= uniform_int(1,40) .. '.' .. uniform_int(110,150) .. '.' .. uniform_int(50,90) .. '.' .. uniform_int(180,220)"

# ─── 同行派生 ───
bill_no      = "= col.game_type .. '_' .. col.bill_value"
billtime     = "= col.reckontime - uniform_int(60, 3600)"

# ─── DSL stdlib 覆盖的"看似需要写 Go"的场景 ───
device_finger = "= sha256(col.user_id, col.device_id, rowID)[:12]"
priority_tier = "= zipf(1000, 1.07)"
amount        = "= lognormal(3.5, 1.2)"
name_lower    = "= lower(col.raw_name)"

# ─── 真刁钻的列 → Go 逃生舱 ───
custom_field  = "@go"

# ─── 修饰符 ───
email  = { expr = "= 'u' .. uniform_int(1, 1e8) .. '@example.com'", null_percent = 5 }
notes  = { kind = "go", null_percent = 20 }     # Go 函数（函数名默认 = "notes"）+ 修饰符

[columns.game_type]      # 长的加权枚举 → 小节
choose = [
  { w = 0.24, v = "100" },
  { w = 0.17, v = "101" },
  { w = 0.11, v = "102" },
]
else = "= uniform_int(109, 292) .. ''"

[columns.platform_id]    # 条件分布 → 小节
switch = "= col.table_id"
cases  = { 1 = "= choose({ 161 = 0.966, 162 = 0.034 })" }
else   = "= 200 + col.table_id"

[columns.reckontime]     # 时间窗口 → 小节
time_between = ["2025-06-03 00:00:00", "2025-07-05 18:10:12"]
format       = "2006-01-02 15:04:05"
```

### 5.3 与 SQL COMMENT 的关系

- 现有 `null_percent` / `max_length` / `mean` / `stddev` / `order` SQL COMMENT 解析**保留**。
- TOML `[columns]` 条目里可设相同的公共修饰符，适用于所有形式的值。
- 同一列 SQL COMMENT + TOML 都有公共修饰符时，**TOML 优先**，日志 `WARN` 一次。
- TOML 条目不存在时，行为完全等同现状。

## 6. Layer 1：表达式语言与原语

### 6.1 小型表达式语言（受限、无状态）

**语法总览**（EBNF 简化）：

```
expr     = literal | ident | call | expr op expr | '(' expr ')' | '-' expr | expr '[' slice ']'
ident    = name ('.' name)*
call     = ident '(' args? ')'
args     = expr (',' expr)* (',' kwarg)* | map
kwarg    = name '=' expr
map      = '{' (literal '=' expr) (',' literal '=' expr)* '}'
slice    = number (':' number)?
literal  = number | string | bool | nil
op       = '+' | '-' | '*' | '/' | '..'
```

**变量**：

| 名称            | 含义                                 |
| --------------- | ------------------------------------ |
| `rowID`         | 当前行号（int64）                     |
| `col.<name>`    | 同行已生成的兄弟列值                  |

**运算符**（优先级高→低）：`-` 一元 · `* /` · `+ -` · `..`

字符串拼接用 `..`（Lua 风格）。两侧参数自动 `toString`，和 perftest 团队 Lua 脚本习惯一致，避免 `+` 数字/字符串二义。

**切片**：`expr[lo:hi]` 与 `expr[:hi]` 对字符串生效（字节下标）。主要给 `sha256(...)[:12]` 这种用法。

### 6.2 原语：生成基础值

| 函数                           | 语义                                           | 返回类型 |
| ------------------------------ | ---------------------------------------------- | -------- |
| `uniform_int(lo, hi)`          | 整数均匀，闭区间 `[lo, hi]`                     | int64    |
| `uniform_float(lo, hi)`        | 浮点均匀，`[lo, hi)`                            | float64  |
| `gauss(mean, stddev)`          | 正态，超出列类型范围时截断                      | 同列类型 |
| `choose(weights, else=expr)`   | inline 加权选择。`weights` 是 `{v1=w1, v2=w2}` map | 键的类型 |

覆盖 perftest 模式 1–3 的主干。

### 6.3 DSL 标准库

v1 一次性加齐的内建函数。**每个函数都是纯 Go 实现，batch-friendly**，避免用户下沉到 Go 逃生舱。

**哈希 / 编码**

| 函数                     | 说明                                         | 返回 |
| ------------------------ | -------------------------------------------- | ---- |
| `sha256(x, y, ...)`      | 把参数串行化后 sha256，返回小写 hex string    | string (64 chars) |
| `md5(x, y, ...)`         | 同上，md5                                    | string (32 chars) |
| `hex(x)` / `b64(x)`      | 编码到 hex / base64                          | string |

串行化规则：数值按 `strconv`、字符串原样、`rowID` 作为 int64 追加、列值按运行时类型。

**字符串**

| 函数                        | 说明                                      |
| --------------------------- | ----------------------------------------- |
| `lower(s)` / `upper(s)`     | 大小写转换（ASCII fast path）              |
| `substr(s, lo, hi)`         | 字节下标切片（同 `s[lo:hi]`，slice 的函数形式） |

**统计分布**

| 函数                    | 说明                                              |
| ----------------------- | ------------------------------------------------- |
| `zipf(N, s)`            | Zipfian `[1, N]`，参数 s > 1。CDF 每列启动期预计算  |
| `pareto(alpha)`         | Pareto，`xm = 1`                                  |
| `lognormal(mu, sigma)`  | 对数正态                                          |

**时间**

| 函数                                 | 说明                                                    |
| ------------------------------------ | ------------------------------------------------------- |
| `dt.add(t, "1h30m")`                 | Go duration 语法，返回新的时间戳（int64 μs since epoch） |
| `dt.trunc(t, "day" / "hour" / ...)`  | 向下截断到单位                                           |

**示例：把"该写 Go"的场景收编到 DSL 一行**

```toml
device_finger = "= sha256(col.user_id, col.device_id, rowID)[:12]"         # 原本要写 Go
priority_tier = "= zipf(1000, 1.07)"                                        # 原本要写 Go
amount        = "= lognormal(3.5, 1.2)"                                     # 原本要写 Go
api_route     = "= lower(col.service) .. '/v1/' .. substr(col.endpoint, 0, 20)"
session_day   = "= dt.trunc(col.reckontime, 'day')"
```

### 6.4 小节原语（不能 inline）

下面三个规则条目多、结构化，必须开 `[columns.<name>]` 小节：

**`choose`（长列表）**

```toml
[columns.game_type]
choose = [
  { w = 0.24, v = "100" },
  { w = 0.17, v = "101" },
]
else = "= uniform_int(109, 292) .. ''"
```

inline map 形态 `choose({a = 0.24, b = 0.17}, else = ...)` 与 `choose = [...]` 小节形态语义等价，选哪个看条目数量与 TOML 可读性。

**`switch`（条件分布）**

```toml
[columns.platform_id]
switch = "= col.table_id"
cases  = { 1 = "= choose({ 161 = 0.966, 162 = 0.034 })" }
else   = "= 200 + col.table_id"
```

- `switch` 是一个表达式（不限于单列；可以 `"= col.a + col.b"`）。
- `cases` 的 key 必须是可精确相等比较的标量（数值/字符串）。
- `else` 必填。

覆盖 perftest 模式 8。

**`time_between`（时间窗口）**

```toml
[columns.reckontime]
time_between = ["2025-06-03 00:00:00", "2025-07-05 18:10:12"]
format       = "2006-01-02 15:04:05"     # Go time layout；"unix_ms" / "unix_us" 输出整数
distribution = "uniform"                  # "uniform" | "left_skew_stepped"
step         = "12h"                      # 仅 left_skew_stepped 需要
```

`left_skew_stepped` 实现 perftest 里 `max - uniform(1,N)*step` 的形态，覆盖模式 9 的左偏阶梯子模式。

### 6.5 公共修饰符

任何形式的 spec 都可以附加：

```toml
null_percent = 10       # 0-100，整行生成后按此概率改写为 null
max_length   = 32       # 字符串列生效；若生成结果超长则按 UTF-8 安全截断
```

## 7. 列求值顺序（拓扑排序）

加载 specs 时：

1. 扫描所有表达式中的 `col.<name>` 引用、`switch` 的 `switch` 字段引用，构建 DAG。
2. `@go` 列默认被放到拓扑序**最后**；若用户需要其它 Go 列或 DSL 列读 `@go` 列的值，必须显式声明依赖（见 §9.4）。
3. Kahn 拓扑排序。检测到环 → 启动失败，错误信息列出环上的列名。
4. 输出 parquet 时 **schema 列顺序不变**（用户在 `CREATE TABLE` 里的定义），生成阶段按拓扑顺序。

**失败模式**：

- 循环引用：启动报错。
- 引用不存在的列：启动报错。
- 跨表引用：启动报错（v1 只支持同表）。

## 8. 与现有 batch pipeline 的集成

### 8.1 快路径（无跨列引用）

全表无任何跨列引用（`col.X`、`switch`、`@go` 未声明 deps、§6 stdlib 函数调用不带 `col.X`）时，保持现有列主序循环不变。每列独立从头到尾生成，`BatchSize=10` 的 buffer 复用。Layer 1 原语 / stdlib 实现成"就地写入 `[]int32`/`[]int64`/`[]parquet.ByteArray`"的函数。零额外成本。

### 8.2 慢路径（有跨列引用）

发现跨列引用时，切到**行批主序生成 + 列主序 flush**：

1. `ParquetWriter.Init` 时为**被引用的列**分配整个 row group 大小的 buffer（而非 `BatchSize`）。
2. 一个 row group 内，外层循环按 `BatchSize` 行推进，内层按拓扑顺序过所有列，各自把 `BatchSize` 行写入自己的"整 RG buffer"对应偏移。
3. 一个 row group 生成完后，按 schema 顺序把每个整-RG buffer 串行 `WriteBatch` 到 parquet `SerialRowGroupWriter`。

**内存代价**：被引用列需要 `rowsPerRowGroup × sizeof(element)` buffer。默认 `rows=4000` 单列约 32KB；大 row group（1M 行）时 1–5MB 级别，可接受。

**被引用的判定**：拓扑分析时标记"被其它列以 `col.X` / `switch` / Go `ctx.XXX(name)` 引用过"的列需要整-RG buffer；没被引用的列仍用 `BatchSize` 小 buffer。

### 8.3 CSV 路径

CSV 本来就是行主序：按拓扑顺序生成每行，结果放 `[]any`，按 schema 顺序拼行输出。不需要 §8.2 的两段式。

## 9. Layer 2：Go 函数逃生舱

### 9.1 用户工作量最小化

用户**只写函数体**。所有样板代码由工具生成：

| 元素                             | 由谁生成                                            |
| -------------------------------- | --------------------------------------------------- |
| `package user`、imports          | `data-writer scaffold` 生成骨架时写入                 |
| `func Xxx(*gen.Ctx) any` 签名    | scaffold 根据 schema 生成，含返回类型注释             |
| `init()` + `RegisterGenerator`   | bootstrap 期 AST 扫描自动生成 `registry_gen.go`        |
| 类型化访问（无 `.(int32)` 断言） | `gen.Ctx` 提供类型化方法                              |

### 9.2 scaffold 命令（v1 必备）

```bash
data-writer scaffold --sql schema.sql --cfg config.toml -o generators/user_gens.go
```

读 schema + config，对每个 `@go` 列生成一个空壳：

```go
// Code generated by data-writer scaffold. Edit function bodies only.
// Regenerate with: data-writer scaffold --sql schema.sql --cfg config.toml -o ...
package user

import (
    "data-writer/pkg/gen"
    // TODO imports
)

// Column: custom_field  SQL: VARCHAR(64)
// Deps (declared in config): none
func Custom_field(ctx *gen.Ctx) any {
    // TODO: return string (max 64 chars), or nil
    return ""
}
```

重跑 scaffold 不会覆盖已写好的函数体（按函数名保留）；只增删 `@go` 列对应的函数占位。

### 9.3 `gen.Ctx` API

```go
package gen

type Ctx struct {
    RowID int64
    Rng   *rand.Rand           // worker-local RNG
    // 私有字段：当前行已生成的兄弟列 buffer
}

// 类型化访问兄弟列（对应 SQL 类型）
func (c *Ctx) Int32(name string) int32
func (c *Ctx) Int64(name string) int64
func (c *Ctx) Float32(name string) float32
func (c *Ctx) Float64(name string) float64
func (c *Ctx) String(name string) string
func (c *Ctx) Bool(name string) bool
func (c *Ctx) IsNull(name string) bool

// 通用接口，类型不确定时用
func (c *Ctx) Col(name string) any

type GenFunc func(*Ctx) any
```

**返回值类型**必须匹配目标列的 SQL 类型：

| SQL 类型                         | 返回 Go 类型             |
| -------------------------------- | ------------------------ |
| TINYINT/SMALLINT/MEDIUMINT/INT   | `int32`                  |
| BIGINT                           | `int64`                  |
| FLOAT                            | `float32`                |
| DOUBLE                           | `float64`                |
| VARCHAR/CHAR/BLOB/VARBINARY      | `string`                 |
| TIMESTAMP/DATETIME               | `int64` (unix μs)        |
| DATE                             | `int32` (days since epoch) |
| 任意                             | `nil` → NULL             |

类型不匹配 → 运行时 panic，打印行号 + 列名。

### 9.4 依赖声明

Go 函数读 `ctx.Int32("user_id")` 时，工具不静态分析函数体（避免引入 Go AST 分析维护成本）。用户在 config 里**显式声明**依赖：

```toml
device_finger = { kind = "go", deps = ["user_id", "device_id"] }
```

- 不声明 deps → 该 Go 列被排到拓扑序最后，允许读任意已生成列（但不能被其它列读）。
- 声明 deps → 加入拓扑图；该 Go 列可以被其它列通过 `col.device_finger` 引用。
- 声明了但运行时读了未声明的列 → panic，提示用户补 deps。

### 9.5 bootstrap 流程（AST 扫描 + 自动注册）

EC2 bootstrap 脚本（orchestrator 侧约定）：

```bash
#!/bin/bash
set -e

# 1. 克隆工具仓库
git clone <data-writer-repo> /opt/data-writer
cd /opt/data-writer

# 2. Drop 用户文件
mkdir -p src/user
cp /task/generators/*.go src/user/ 2>/dev/null || true

# 3. 代码生成：扫描 src/user/ 里所有 func Xxx(*gen.Ctx) any，产生 registry_gen.go
go run ./cmd/codegen -in src/user -out src/user/registry_gen.go

# 4. 编译
go build -o bin/data-writer ./src

# 5. 跑任务
bin/data-writer --cfg /task/config.toml --sql /task/schema.sql
```

`cmd/codegen` 是工具内建的小工具（~100 行 Go），用 `go/parser` 扫目标目录，对每个符合签名的函数产出一条 `RegisterGenerator("Xxx", Xxx)`。

### 9.6 允许的 imports（v1 策略）

用户 Go 文件**只允许**：

- Go 标准库
- `data-writer/pkg/gen`（`gen.Ctx` + 公共 helper）

禁止 `os/exec`、`net/http`、第三方包等。执行方式：codegen 阶段同时做一次静态检查，发现不在白名单的 import 直接拒绝 `go build`。

这条针对内部用户仍然适用——一是减小调试面、二是防止"用户无意引入慢操作"（例如 HTTP 调用）在每行生成里跑。

## 10. 向后兼容

- 没有 `[columns]` 段的配置：行为完全等同现状。
- 有 SQL COMMENT 没有 TOML：继续按 COMMENT 解析，行为不变。
- 同时冲突：TOML 优先，日志 `WARN` 一次。
- 新 `kind` / 新函数名：启动期 fail-fast。

## 11. 性能与基准

| 场景                                   | 预期                                       |
| -------------------------------------- | ------------------------------------------ |
| 无 `[columns]` 配置                    | 与 `main` 对齐（±2%），保持 2 GB/s 写 S3   |
| 只有 Layer 1 原语 + stdlib 且无跨列引用 | 与 `main` 对齐（±5%）                      |
| Layer 1 + 跨列引用（慢路径）            | 单列 ~1.2–1.5× 当前耗时（整-RG 缓冲成本）    |
| Layer 2 Go 函数（任意）                 | 和 Layer 1 同级（ns），S3 吞吐不受影响    |

在 `performance_test.sh` 里加一组 bingoplus-like 配置作为回归基准。

## 12. 里程碑

- **M1** — Layer 1 基础原语（`constant` / `uniform_int` / `uniform_float` / `gauss` / inline `choose` / 小节 `choose`）+ 公共修饰符 + TOML `[columns]` 解析 + 快路径集成。
- **M2** — 表达式语言（解析器、`..`、`col.X`、`rowID`）+ 拓扑排序 + 慢路径整-RG buffer 集成。
- **M3** — `switch` + `time_between`。
- **M4** — DSL stdlib：`sha256` / `md5` / `hex` / `b64` / `substr` / `lower` / `upper` / `zipf` / `pareto` / `lognormal` / `dt.add` / `dt.trunc`。
- **M5** — Layer 2 Go registry：`gen.Ctx` + `kind=go` + `cmd/codegen` AST 扫描 + `data-writer scaffold` + 白名单 import 校验。
- **M6** — 性能回归基准 + 文档 & 示例。

## 13. 开放问题

1. **TOML `[columns]` 会不会太长**？bingoplus 42 列全部一个 TOML 可读，但 100+ 列的表就嫌挤。要不要允许 `common.column_specs_file = "column_specs.toml"` 指向外部文件？倾向支持，作为 M1 的小 follow-up。
2. **表达式解析器：自研 vs `github.com/expr-lang/expr`**。自研约 400–500 行、文法精确可控；用 expr 节省时间但需要一层适配来禁止不想要的构造（比如 array literal、lambda、范围运算）。倾向自研——受限文法更安全、错误信息可定制。
3. **`time_between` 的 `left_skew_stepped` 参数化**：目前是 `distribution = "left_skew_stepped" + step = "12h"`，按"过去 N 天均匀"的形态显式参数化。是否还需要 `right_skew` / `gaussian_over_time` 等？倾向 v1 只做 `uniform` + `left_skew_stepped`，其余后续按需扩展。
4. **`parquet BufferedRowGroupWriter`**：如果 arrow-go 提供允许列交错写的 writer，§8.2 的整-RG buffer 方案可换成更省内存的增量方案。M2 开工前做一次小 POC 验证。
5. **scaffold 的幂等性**：用户已填的函数体必须保留，只增删 `@go` 列对应的占位。实现上会按函数名做 merge，边界 case（重命名、删除后又加回）需要明确规则。
6. **`@go` 列的 deps 能否从源码静态推断**？v1 选"用户显式声明"，工程量小。如果未来想自动推断，在 codegen 里加一遍 AST 扫描即可——属于增量增强，不影响现在的设计。

## 14. 任务管线集成

本工具在你们现有的 "用户提交任务 → 起 EC2 → 跑 Go 程序" 流程里落地，以下是 orchestrator 侧的约定：

### 14.1 任务包结构

```
task/
├── config.toml                # [common] + [columns] + ...
├── schema.sql                 # CREATE TABLE
└── generators/                # 可选，仅当配置里有 @go 列时需要
    ├── user_gens.go           # 用户填函数体
    └── ...                    # 允许多文件
```

### 14.2 EC2 bootstrap 脚本（伪代码）

```bash
git clone <data-writer-repo> /opt/data-writer
cd /opt/data-writer
[ -d /task/generators ] && cp /task/generators/*.go src/user/
go run ./cmd/codegen -in src/user -out src/user/registry_gen.go
go build -o bin/data-writer ./src
bin/data-writer --cfg /task/config.toml --sql /task/schema.sql
```

### 14.3 依赖范围（v1）

- 用户 Go 代码只能 import：Go stdlib + `data-writer/pkg/gen`。
- 不允许 `go.mod` 扩展——保持 bootstrap 无外网依赖拉取。
- 如果将来有典型第三方依赖（例如特定 hash 算法），倾向收编到 `data-writer/pkg/gen` stdlib 而非开放 `go.mod`。

### 14.4 失败语义

| 错误                                      | 何时报 | 报给谁              |
| ----------------------------------------- | ------ | ------------------- |
| config 引用未知函数 / 列 / DSL 名         | 启动期 | stderr，退出码 ≠ 0 |
| 表达式解析错                              | 启动期 | stderr              |
| 用户 Go 代码 import 白名单外的包           | codegen | stderr              |
| 用户 Go 代码签名不符                      | codegen | stderr              |
| 拓扑有环 / 跨表 `col.X`                   | 启动期 | stderr              |
| Go 函数返回类型不匹配目标列               | 运行时 | panic + 行号 + 列名 |
| 未声明 deps 而读了兄弟列                  | 运行时 | panic               |

---

## 附：对 perftest 模式的覆盖矩阵

| perftest 模式                          | 推荐方式                                | 需要写 Go？ |
| -------------------------------------- | --------------------------------------- | ----------- |
| 1. 常量                                | 字面量                                  | 否          |
| 2. 均匀整数                            | `uniform_int`                           | 否          |
| 3. 加权枚举 + 尾部兜底                 | `choose` + `else`                       | 否          |
| 4. 前缀 + 序号                         | `"= prefix .. uniform_int(...)"`        | 否          |
| 5. 拼接多段数字大 ID                   | `"= uniform_int(...) .. uniform_int(...)"` | 否       |
| 6. 模板化多字段（IP/UUID-ish）         | `".."` 连接多个 `uniform_int`            | 否          |
| 7. 同行派生                            | `col.X` + 算术                          | 否          |
| 8. 条件分布（基于兄弟列）              | `[columns.X]` + `switch`                | 否          |
| 9. 时间窗口（均匀 / 左偏阶梯）         | `[columns.X]` + `time_between`          | 否          |
| 哈希指纹 / 派生 token                  | `sha256(...)[:n]`（stdlib）              | 否          |
| Zipf / Pareto / Lognormal 分布         | stdlib 原语                             | 否          |
| 字符串大小写 / 切片                    | `lower` / `upper` / `substr`            | 否          |
| 真刁钻的业务逻辑                       | `@go` + scaffold                        | **是**      |
