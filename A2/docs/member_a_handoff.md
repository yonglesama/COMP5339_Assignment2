# MQTT 消息交接

## B 使用的文件

路径相对项目根目录（包含 A1/ 和 A2/）。

| 文件 | 内容 |
|---|---|
| `A2/data/examples/` | 四类 JSON 消息样例 |
| `A2/data/demo/event_stream.jsonl` | 32 条消息，包含价格变化和一个完整充电会话，用于订阅及状态处理测试 |
| `A2/data/events/event_stream.jsonl` | 2,479,112 条全量消息，已按事件时间排序 |

## MQTT 连接

默认 broker 为 `localhost:1883`，订阅 `comp5339/ev/#`。多机调试时，发布端和订阅端使用同一个可访问的 broker 地址。

主题格式：`comp5339/ev/{station_id}/{event_type}`。

消息体为 JSON，公共字段为 schema_version、event_id、sequence_number、event_type、event_time、event_timezone、station_id、payload。

| 消息 | 数据含义 | JSON 样例 |
|---|---|---|
| price_update | payload.price_per_kwh：站点当前价格，AUD/kWh | [price_update.json](../data/examples/price_update.json) |
| session_start | payload.session_id、connector_id、price_per_kwh：开始会话并锁定本次价格 | [session_start.json](../data/examples/session_start.json) |
| meter | 同一 session_id 的 voltage_v、current_a、power_kw、energy_kwh、soc_pct：更新电表数据 | [meter.json](../data/examples/meter.json) |
| session_end | charge_end_time 为停止充电，end_time 为拔枪；占用在 end_time 释放 | [session_end.json](../data/examples/session_end.json) |

event_time 为 ISO 时间字符串，event_timezone=Australia/Sydney。生成器按固定 24 小时模拟，不做夏令时换算。电表单位为 V、A、kW、kWh、百分比，energy_kwh 是会话累计电量。

演示流的 sequence_number 不连续，按文件顺序处理。event_id 用于重复消息去重；重复播放前清空测试状态，才能重新看到完整状态变化。同一插头前后会话可能在同秒交接，结束时按 session_id 移除旧会话。

## 32 条演示消息的预期结果

从空的订阅状态开始，先订阅，再发布 `A2/data/demo/event_stream.jsonl`。

| 项目 | 预期值 |
|---|---|
| 站点 / 会话 | STD7565099B8 / STD7565099B8-3-202610010100 |
| 消息数 | 32：2 条 price_update、1 条 session_start、28 条 meter、1 条 session_end |
| 最新价格 | 0.66 → 0.65 AUD/kWh |
| 活跃会话数 | 0 → 1 → 0 |
| 会话电表数据 | 按时间收到 28 个采样点 |
| 结束消息中的电量 / 费用 | 26.39 kWh / AUD 17.15 |

## C 使用的关联键

A1 数据库：`A1/data/final/ev_chargers.duckdb`。

- `station_id` 与 A1 的 `stations.station_id` 相同；站点资料在 `A2/data/input/generator_stations.csv`。
- `connector_id` 是站内模拟插头编号。用 `(station_id, connector_id)` 在 `A2/data/input/station_connector_mapping.csv` 中查找 A1 的 `charger_id` 和 `plug_number`。

范围为 414 个有已知正功率的 DC 站点、1,610 个插头。AC 和功率缺失记录未纳入。每站按最大 DC 功率建模，两个站点包含不同功率的 charger。
