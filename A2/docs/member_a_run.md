# 安装与运行

## 安装

使用 Python 3.11，以下命令在项目根目录（包含 A1/ 和 A2/）执行。

```powershell
python -m pip install -r A2/requirements.txt
```

本地 broker 和 MQTT 测试需要额外安装：

```powershell
python -m pip install -r A2/requirements-dev.txt
```

## 本机回放

终端 1：启动 broker，监听 `127.0.0.1:1883`。

```powershell
python A2/src/publishing/local_broker.py
```

终端 2：运行订阅端，订阅 `comp5339/ev/#`。订阅端的实现接口位于 `A2/src/subscription/`。

终端 3：发布 36 条演示消息，每条间隔 1 秒。

```powershell
python A2/src/publishing/run.py --project-root . --stream A2/data/demo/event_stream.jsonl --delay 1
```

多机运行时使用双方均能访问的 broker；发布命令加 `--host 地址 --port 端口`，订阅端配置相同地址。上面的本地 broker 只接受本机连接。

省略 `--stream` 可发布一日完整文件。完整流有 172,803 条消息，按最低 0.1 秒间隔至少需要 4.8 小时。最终 DuckDB 通过 C 的同一验证接口离线导入完整流；真实 MQTT 使用演示流验证。

## 重新生成数据

重建正式数据：

```powershell
python A2/run_member_a.py
```

参数：414 个 DC 站点、2026-10-01 起 1 天、AEMO NSW1 电价、seed=5339、电表间隔 1 分钟。价格、会话和电表 CSV 输出到 `A2/data/generated/`，完整消息流输出到 `A2/data/events/event_stream.jsonl`。

时间沿用老师生成器的无偏移模拟标签，不按夏令时转换；跨切换日的标签不能直接当作精确的当地实际时刻。

小数据集：`python A2/run_member_a.py --profile dev`，输出到 `A2/data/dev/`。

分阶段重建时按以下顺序运行：

```powershell
python A2/run_pipeline.py --stage station-export
python A2/run_pipeline.py --stage synthetic-generation
python A2/run_pipeline.py --stage event-preparation
python A2/run_pipeline.py --stage demo-preparation
```

## 测试

```powershell
python -m unittest discover -s A2/tests -p test_member_a.py -v
python A2/validate_member_a.py
python A2/tests/mqtt_smoke.py --stream A2/data/demo/event_stream.jsonl --report-prefix mqtt_demo
```

最后一条命令自动启动临时 broker 和订阅者。`A2/data/quality/mqtt_demo_summary.csv` 中应有：expected_messages=36、received_messages=36、identical_payloads_and_order=True、最小发布间隔至少 0.1 秒。
