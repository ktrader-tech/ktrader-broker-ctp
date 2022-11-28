# KTrader-Broker-CTP
[![Maven Central](https://img.shields.io/maven-central/v/org.rationalityfrontline.ktrader/ktrader-broker-ctp.svg?label=maven%20central)](https://search.maven.org/search?q=g:%22org.rationalityfrontline.ktrader%22%20AND%20a:%22ktrader-broker-ctp%22)
![JDK](https://img.shields.io/badge/jdk-%3E%3D11-orange)
![platform](https://img.shields.io/badge/platform-windows%7Clinux-green)
[![Apache License 2.0](https://img.shields.io/github/license/ktrader-tech/ktrader-broker-ctp)](https://github.com/ktrader-tech/ktrader-broker-ctp/blob/master/LICENSE)

[KTrader-API](https://github.com/ktrader-tech/ktrader-api) 中 BrokerApi 接口的 CTP 实现。

对底层 CTP 的调用使用了 CTP 的 Java 封装 [JCTP](https://github.com/ktrader-tech/jctp) ，支持 64 位的 Windows 及 Linux 操作系统。
默认使用的 JCTP 版本为 `6.6.1_P1-1.0.4`，如果需要更换为其它版本，请参考 [Download](#download) 部分。

## 功能特性
众所周知，CTP 使用繁琐（如登录流程）且存在很多的坑，但本框架封装后暴露给终端用户的接口是简洁且统一的
* 利用 [Kotlin 协程](https://github.com/Kotlin/kotlinx.coroutines) 将 CTP 的异步接口封装为同步调用方式，降低心智负担，提升开发效率
* 内置自成交风控，存在自成交风险的下单请求会本地拒单
* 内置撤单数量风控，单合约日内撤单数达到 499 次后会本地拒绝该合约的后续撤单请求
* 内置 CTP 流控处理，调用层无需关注任何 CTP 流控信息
* 支持期货及期权的交易（目前尚不支持期权行权及自对冲）
* 实现了本地持仓、订单、成交、Tick 的缓存及维护，让查询请求本地快速返回，避免频繁网络请求
* 自动查询账户真实的手续费率（包括中金所申报手续费）与保证金率，并计算持仓、订单、成交相关的手续费、保证金、冻结资金（期权也支持）
* 封装提供了一些 CTP 原生不支持的功能，如查询当前已订阅行情，Tick 中带有合约交易状态及 Tick 内成交量成交额等
* 对 CTP 推送的 Tick 进行了修改，实现了标准化的 Tick 推送流程
* 网络断开重连时会自动订阅原先已订阅的行情，不用手动重新订阅
* 支持 7x24 小时全自动运行

关于对 CTP Tick 的修改，具体如下：
* 任何 AUCTION_MATCHED/STOP_TRADING/CLOSED 状态的 Tick.time 一定是标准时间（如 08:59/11:30/15:00）
* 任何合约单日内的 Tick 状态推送一定会经过 AUCTION_ORDERING -> AUCTION_MATCHED -> \[CONTINUOUS_MATCHING -> STOP_TRADING\] -> CLOSED 这一标准流程
* 为了保证上面这一标准流程，当合约交易状态变更时，如果未收到对应状态的 Tick 推送，将补发推送纯状态 Tick，即将上一 Tick 抹除交易量数据并修改状态为新状态的 Tick。对于 AUCTION_ORDERING/CONTINUOUS_MATCHING 的状态更改，会立即检查补发。而对于 AUCTION_MATCHED/STOP_TRADING/CLOSED 的状态更改，会等待 StatusTickDelay 毫秒后再检查补发
* 所有修改过时间的 Tick 都会在 Tick.extras 中用 "originalTime" 字段附上修改前的原 Tick 时间
* 所有修改过状态的 Tick 都会在 Tick.extras 中用 "isStatusTick" 字段表明该 Tick 为补发的纯状态 Tick


## 快速入门
```kotlin
private fun testCtpApi(brokerExtension: BrokerExtension) {
    println("Broker 信息开始 ----------------------------------")
    println(brokerExtension)
    println("Broker 信息结束 ----------------------------------")
    // 创建 CTP 配置参数
    val config = mutableMapOf(
        "MdFronts" to listOf("tcp://0.0.0.0:0").toString(),  // 行情前置地址
        "TdFronts" to listOf("tcp://0.0.0.0:0").toString(),  // 交易前置地址
        "InvestorID" to "123456",  // 资金账号
        "Password" to "123456",  // 资金账号密码
        "BrokerID" to "1234",  // BROKER ID
        "AppID" to "rf_ktrader_1.0.0",  // APPID
        "AuthCode" to "ASDFGHJKL",  // 授权码
        "UserProductInfo" to "",  //用户产品信息
        "CachePath" to "./data/ctp",  // 本地缓存文件存储目录
        "Timeout" to "6000",  // 接口调用超时时间（单位：毫秒）
        "statusTickDelay" to "2800",  // 补发暂停交易（AUCTION_MATCHED/STOP_TRADING/CLOSED）的纯状态 Tick 的延迟时间（单位：毫秒）
    )
    // 创建 CtpBrokerApi 实例
    val api = brokerExtension.createApi(File("./data/ctp"), KotlinLogging.logger { }, "0.4.0", config["InvestorID"]!!, config)
    // 订阅所有事件
    api.kEvent.subscribeMultiple<BrokerEvent>(BrokerEventType.values().asList(), tag = api.sourceId) { event -> runBlocking {
        // 处理事件推送
        val brokerEvent = event.data
        when (brokerEvent.type) {
            // Tick 推送
            BrokerEventType.TICK -> {
                val tick = brokerEvent.data as Tick
                println("Tick 推送：${tick.code}, ${tick.price}, ${tick.time}")
            }
            // 其它事件（网络连接、订单回报、成交回报等）
            else -> {
                println(brokerEvent)
            }
        }
    }}
    // 测试 api
    runBlocking {
        api.connect()
        println("CTP 已连接")
        println("当前交易日：${api.getTradingDay()}")
        println("查询账户资金：")
        println(api.queryAssets())
        api.close()
        println("CTP 已关闭")
    }
}

fun main() {
    println("------------ 启动 ------------")
    val deleteOnFinish = false  // 是否运行完后删除插件
    val pluginManager = object : DefaultPluginManager(Path.of("./plugins/")) {
        override fun createExtensionFactory(): ExtensionFactory {
            return SingletonExtensionFactory(this)
        }
    }
    pluginManager.addPluginStateListener { event ->
        println("插件状态变更：${event.plugin.pluginId} (${event.plugin.pluginPath}), ${event.oldState} -> ${event.pluginState}")
    }
    println("加载插件...")
    pluginManager.loadPlugins()
    println("启用插件...")
    pluginManager.startPlugins()
    println("调用插件...")
    pluginManager.getExtensions(BrokerExtension::class.java).forEach { brokerExtension ->
        if (brokerExtension.name == "CTP") testCtpApi(brokerExtension)
    }
    if (deleteOnFinish) {
        println("删除插件...")
        pluginManager.plugins.map { it.pluginId }.forEach {
            pluginManager.deletePlugin(it)
        }
    } else {
        println("停用插件...")
        pluginManager.stopPlugins()
    }
    println("卸载插件...")
    pluginManager.unloadPlugins()
    println("------------ 退出 ------------")
}
```
完整的可运行程序实例在 examples 目录中。

## 使用说明
初始化参数：
* MdFronts: List<String> 行情前置
* TdFronts: List<String> 交易前置
* InvestorID: String 投资者资金账号
* Password: String 投资者资金账号的密码
* BrokerID: String 经纪商ID
* AppID: String 交易终端软件的标识码
* AuthCode: String 交易终端软件的授权码
* UserProductInfo: String 交易终端软件的产品信息
* Timeout: Long 接口调用超时时间（单位：毫秒）。默认为 6000
* StatusTickDelay: Long 补发暂停交易（AUCTION_MATCHED/STOP_TRADING/CLOSED）的纯状态 Tick 的延迟时间（单位：毫秒）。默认为 2800
* CachePath: String 存贮订阅信息文件等临时文件的目录。默认为 ./data/ctp/

支持的额外参数：
* subscribeTick/subscribeTicks/querySecurity/queryLastTick：[extras.ensureFullInfo: Boolean = true]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），如果之前没查过，会耗时】
* subscribeAllTicks/querySecurities/queryAllSecurities/queryOptions：[extras.ensureFullInfo: Boolean = false]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），如果之前没查过，会耗时。当 useCache 为 false 时无效，且返回的 SecurityInfo 信息不完整】

关于证券代码，统一格式为“交易所代码.合约代码”。如 "SHFE.ru2109" 表示上期所的橡胶2109合约。全部交易所如下：

| 交易所  | 交易所代码 | 合约代码大小写 | 示例代码         |
|------|-------|---------|--------------|
| 中金所  | CFFEX | 大写      | CFFEX.IF2109 |
| 上期所  | SHFE  | 小写      | SHFE.ru2109  |
| 能源中心 | INE   | 小写      | INE.sc2109   |
| 大商所  | DCE   | 小写      | DCE.m2109    |
| 郑商所  | CZCE  | 大写      | CZCE.MA109   |

## Download

[Releases](https://github.com/ktrader-tech/ktrader-broker-ctp/releases)

如果需要使用其它版本的 JCTP，将插件压缩包内 lib 目录下的 jctp-xxxx.jar 替换成你所需要的版本的 jar 即可。

JCTP jar 下载地址：[Maven Repository](https://repo1.maven.org/maven2/org/rationalityfrontline/jctp/)

## License

KTrader-Broker-CTP is released under the [Apache 2.0 license](https://github.com/ktrader-tech/ktrader-broker-ctp/blob/master/LICENSE).

```
Copyright 2022 RationalityFrontline

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```