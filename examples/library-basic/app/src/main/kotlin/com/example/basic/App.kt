package com.example.basic

import kotlinx.coroutines.runBlocking
import org.rationalityfrontline.kevent.KEVENT
import org.rationalityfrontline.ktrader.broker.api.BrokerEvent
import org.rationalityfrontline.ktrader.broker.api.BrokerEventType
import org.rationalityfrontline.ktrader.broker.ctp.CtpBrokerApi
import org.rationalityfrontline.ktrader.broker.ctp.CtpConfig
import org.rationalityfrontline.ktrader.datatype.Direction
import org.rationalityfrontline.ktrader.datatype.OrderOffset
import org.rationalityfrontline.ktrader.datatype.OrderType
import org.rationalityfrontline.ktrader.datatype.Tick

fun main() {
    println("------------ 启动 ------------")
    // 创建 CTP 配置参数
    val config = CtpConfig(
        mdFronts = listOf(  // 行情前置地址
            "tcp://0.0.0.0:0",
        ),
        tdFronts = listOf(  // 交易前置地址
            "tcp://0.0.0.0:0",
        ),
        investorId = "123456",  // 资金账号
        password = "123456",  // 资金账号密码
        brokerId = "1234",  // BROKER ID
        appId = "rf_ktrader_1.0.0",  // APPID
        authCode = "ASDFGHJKL",  // 授权码
        userProductInfo = "",  // 产品信息
        cachePath = "./data/ctp",  // 本地缓存文件存储目录
        disableAutoSubscribe = false,  // 是否禁用自动订阅
        disableFeeCalculation = false,  // 是否禁用费用计算
    )
    // 创建 CtpBrokerApi 实例
    val api = CtpBrokerApi(config, KEVENT)
    println(api.version)
    // 订阅所有事件
    KEVENT.subscribeMultiple<BrokerEvent>(BrokerEventType.values().asList()) { event -> runBlocking {
        // 处理事件推送
        val brokerEvent = event.data
        when (brokerEvent.type) {
            // Tick 推送
            BrokerEventType.TICK -> {
                val tick = brokerEvent.data as Tick
                // 当某合约触及涨停价时，以跌停价挂1手多单开仓限价委托单
                if (tick.lastPrice == tick.todayHighLimitPrice) {
                    api.insertOrder(tick.code, tick.todayLowLimitPrice, 1, Direction.LONG, OrderOffset.OPEN, OrderType.LIMIT)
                }
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
        println("查询账户持仓：")
        println(api.queryPositions().joinToString("\n"))
        println("查询当日全部订单：")
        println(api.queryOrders(onlyUnfinished = false).joinToString("\n"))
        println("查询当日全部成交记录：")
        println(api.queryTrades().joinToString("\n"))
        // 订阅行情
        api.subscribeTick("SHFE.ru2201")
//        Thread.currentThread().join()  // 如果需要 7x24 小时不间断运行，取消注释此行。（如需主动退出运行请使用 System.exit(0) 或 exitProcess(0)）
        api.close()
        println("CTP 已关闭")
    }
    // 清空 KEVENT
    KEVENT.clear()
    println("------------ 退出 ------------")
}
