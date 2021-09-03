package com.example.basic

import kotlinx.coroutines.runBlocking
import org.pf4j.DefaultPluginManager
import org.pf4j.ExtensionFactory
import org.pf4j.SingletonExtensionFactory
import org.rationalityfrontline.kevent.KEVENT
import org.rationalityfrontline.ktrader.api.broker.BrokerEvent
import org.rationalityfrontline.ktrader.api.broker.BrokerEventType
import org.rationalityfrontline.ktrader.api.broker.BrokerExtension
import org.rationalityfrontline.ktrader.api.datatype.Tick
import java.nio.file.Path

private fun testCtpApi(brokerExtension: BrokerExtension) {
    println("Broker 信息开始 ----------------------------------")
    println(brokerExtension)
    println("Broker 信息结束 ----------------------------------")
    // 创建 CTP 配置参数
    val config = mutableMapOf(
        "mdFronts" to listOf("tcp://0.0.0.0:0").toString(),  // 行情前置地址
        "tdFronts" to listOf("tcp://0.0.0.0:0").toString(),  // 交易前置地址
        "investorId" to "123456",  // 资金账号
        "password" to "123456",  // 资金账号密码
        "brokerId" to "1234",  // BROKER ID
        "appId" to "rf_ktrader_1.0.0",  // APPID
        "authCode" to "ASDFGHJKL",  // 授权码
        "cachePath" to "./data/ctp",  // 本地缓存文件存储目录
        "disableAutoSubscribe" to "false",  // 是否禁用自动订阅
        "disableFeeCalculation" to "false",  // 是否禁用费用计算
    )
    // 创建 CtpBrokerApi 实例
    val api = brokerExtension.createApi(config, KEVENT)
    // 订阅所有事件
    KEVENT.subscribeMultiple<BrokerEvent>(BrokerEventType.values().asList(), tag = api.sourceId) { event -> runBlocking {
        // 处理事件推送
        val brokerEvent = event.data
        when (brokerEvent.type) {
            // Tick 推送
            BrokerEventType.TICK -> {
                val tick = brokerEvent.data as Tick
                println("Tick 推送：${tick.code}, ${tick.lastPrice}, ${tick.time}")
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
    // 退订事件
    KEVENT.removeSubscribersByTag(api.sourceId)
}

fun main() {
    println("------------ 启动 ------------")
    val deleteOnFinish = false  // 是否运行完后删除插件，可以用来测试是否存在内存泄露
    val pluginManager = object : DefaultPluginManager(Path.of("./plugins/")) {
        override fun createExtensionFactory(): ExtensionFactory {
            return SingletonExtensionFactory()
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
