package com.example.basic

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.rationalityfrontline.ktrader.api.broker.BrokerEvent
import org.rationalityfrontline.ktrader.api.broker.BrokerEventType
import org.rationalityfrontline.ktrader.api.datatype.*
import org.rationalityfrontline.ktrader.broker.ctp.CtpBrokerApi
import org.rationalityfrontline.ktrader.broker.ctp.CtpConfig

fun main() {
    println("------------ 启动 ------------")
    // 创建 CTP 配置参数
    val config = CtpConfig(
        mdFronts = listOf("tcp://0.0.0.0:0"),  // 行情前置地址
        tdFronts = listOf("tcp://0.0.0.0:0"),  // 交易前置地址
        investorId = "123456",  // 资金账号
        password = "123456",  // 资金账号密码
        brokerId = "1234",  // BROKER ID
        appId = "rf_ktrader_1.0.0",  // APPID
        authCode = "ASDFGHJKL",  // 授权码
        userProductInfo = "",  // 产品信息
        cachePath = "./data/ctp",  // 本地缓存文件存储目录
        timeout = 6000,  // 接口调用超时时间（单位：毫秒）
    )
    // 创建 CtpBrokerApi 实例
    val api = CtpBrokerApi(config)
    println(api.version)
    // 设置 TickToTrade 测试
    var tttTestCount = 0
    // 订阅所有事件
    api.kEvent.subscribeMultiple<BrokerEvent>(BrokerEventType.values().asList()) { event -> runBlocking {
        // 处理事件推送
        val brokerEvent = event.data
        when (brokerEvent.type) {
            // Tick 推送
            BrokerEventType.TICK -> {
                val tick = brokerEvent.data as Tick
                if (tttTestCount <= 10) {
                    // 下无效单测试 TickToTrade
                    api.insertOrder(tick.code, 0.0, 1, Direction.LONG, OrderOffset.OPEN, extras = mapOf("tickTime" to (tick.tttTime.toString())))
                    tttTestCount++
                }
            }
            BrokerEventType.ORDER_STATUS -> {
                val order = brokerEvent.data as Order
                val tickToTrade = order.tttTime - (order.extras?.get("tickTime")?.toLong() ?: 0)
                println("TickToTrade: $tickToTrade ns")
            }
            // 其它事件（网络连接、订单回报、成交回报等）
            else -> {
                println(brokerEvent)
            }
        }
    }}
    // 测试 api
    runBlocking {
        api.setTestingTickToTrade(true)
        api.connect()
        println("CTP 已连接")
        println("当前交易日：${api.getTradingDay()}")
        println("查询账户资金：")
        println(api.queryAssets())
        println("查询账户持仓：")
        println(api.queryPositions().joinToString("\n"))
        println("查询当日全部订单：")
        println(api.queryOrders(onlyUnfinished = false).joinToString("\n"))
        println("查询当日全部成交记录：")
        println(api.queryTrades().joinToString("\n"))
        api.subscribeTick("DCE.m2205")
        delay(10000)
        api.close()
        println("CTP 已关闭")
    }
    println("------------ 退出 ------------")
}
