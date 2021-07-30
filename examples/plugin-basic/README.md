# Plugin-Example-Basic
展示以插件方式使用 [KTrader-Broker-CTP](https://github.com/ktrader-tech/ktrader-broker-ctp) 的最简单基础的示例项目。

## 运行
1. 新建目录 ./app/plugins，并将需要测试的插件 ZIP 放到该目录下。
2. 将 ./app/src/main/kotlin/com/example/basic/App.kt 中的 config 中的内容替换为你自己的测试账号的登录参数。
3. 在当前目录下运行命令 `.\gradlew run`(Windows) 或 `./gradlew run`(Linux) 。

在一开始你会看到以下输出信息，这与日志打印相关，不影响项目运行。在类库依赖中添加一个 [SLF4J](http://www.slf4j.org/) 支持的日志类库即可消除该信息。
```text
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
```