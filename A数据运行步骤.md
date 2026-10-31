# 用 A 生成的数据复现报告

当前报告使用 A 按老师生成器生成的数据，不使用先前手工编写的开发测试数据。

## 看实时充电

1. 在项目根目录运行：

   ```powershell
   .venv\Scripts\python.exe A2/src/dashboard/run.py --project-root . --mode demo --initial-station ST4D056616A3 --port 8051 --open-browser
   ```
2. 浏览器会打开端口 **8051**，并选中 **Evie，73 Belmore Road, Randwick NSW 2031**。
3. 初始统计全部为 0，没有价格和活跃会话，这是空事件库的正常状态。
4. 点击 **Run demo**，等待约 **35 秒**。这段时间会出现一个活跃会话，价格从 0.75 变为 0.74，曲线不断增加。
5. 结束后应显示：活跃会话 **0**、完成会话 **1**、电量 **22.60 kWh**、费用 **AUD 16.72**，曲线共 **32 个点**。
6. 想重来就点 **Reset demo**，恢复为空事件库后再次 Run demo。

本次回放的是 A 从完整事件流中选出的 36 条事件：2 条价格、1 条开始、32 条电表、1 条结束。
事件原始内容和时间戳未修改；每条消息间隔 1 秒发布，因此展示是加速回放，不是现实中只充了 35 秒。
站点插头类型原资料未提供，页面如实显示 Not provided。

## 看完整历史统计

在项目根目录运行以下命令，打开端口 **8052**：

```powershell
.venv\Scripts\python.exe A2/src/dashboard/run.py --project-root . --mode history --port 8052 --open-browser
```

正式库以只读方式打开，应看到：

- 完成会话：**5,084**。
- 总电量：**129,422.20 kWh**。
- 总费用：**AUD 88,908.69**。
- 活跃会话：**0**，因为这批数据中的所有会话已经结束。

这组统计来自完整的 172,803 条事件，和上面的单次充电演示是不同范围，报告已经分别列出。
全量数据进行了数据库核对，但没有通过 MQTT 再回放全部 172,803 条。

## 数据保护与报告文件

实时回放只写入独立运行副本；重置不会修改正式库。历史查看模式为只读。
原始 A1、正式 A2 和空事件种子库的 SHA-256 在实际验证前后保持一致。

报告：`A2/docs/member_d_report.pdf`；可编辑文字：`A2/docs/member_d_report.md`。
报告截图以 `a_generated_` 开头，位于 `A2/docs/images/`。
推荐入口 `Start_Dashboard.cmd` 使用 unified 模式，在同一页面显示 C 的历史统计并回放 A 的三站点 MQTT 事件。上面的 `--mode demo` 和 `--mode history` 命令分别用于单会话演示和纯只读历史。旧自编测试库已从交付项目移出。
