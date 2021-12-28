@file:Suppress("JoinDeclarationAndAssignment")

package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.kevent.EventDispatchMode
import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.kevent.SubscriberThreadMode
import org.rationalityfrontline.ktrader.api.ApiInfo
import org.rationalityfrontline.ktrader.api.broker.*
import org.rationalityfrontline.ktrader.api.data.DataApi
import org.rationalityfrontline.ktrader.api.datatype.*
import java.time.LocalDate

/**
 * [BrokerApi] 的 CTP 实现
 */
class CtpBrokerApi(val config: CtpConfig) : BrokerApi, ApiInfo by CtpBrokerInfo {
    
    private val mdApi: CtpMdApi
    private val tdApi: CtpTdApi

    override val account: String = this.config.investorId
    override var brokerStatus: BrokerStatus = BrokerStatus.CREATED
        private set(value) {
            if (field == value) return
            field = value
            postBrokerEvent(BrokerEventType.BROKER_STATUS, value)
        }
    override val mdConnected: Boolean get() = mdApi.connected
    override val tdConnected: Boolean get() = tdApi.connected

    override val kEvent: KEvent = KEvent(
        name = "CTP-$account",
        defaultDispatchMode = EventDispatchMode.SEQUENTIAL,
        defaultThreadMode = SubscriberThreadMode.BACKGROUND,
    )

    init {
        mdApi = CtpMdApi(this.config, kEvent, sourceId)
        tdApi = CtpTdApi(this.config, kEvent, sourceId)
        mdApi.tdApi = tdApi
        tdApi.mdApi = mdApi
    }

    override val isTestingTickToTrade: Boolean
        get() = mdApi.isTestingTickToTrade && tdApi.isTestingTickToTrade

    override var dataApi: DataApi? = null

    /**
     * 向 [kEvent] 发送一条 [BrokerEvent]
     */
    private fun postBrokerEvent(type: BrokerEventType, data: Any) {
        kEvent.post(type, BrokerEvent(type, sourceId, data))
    }

    /**
     * 向 [kEvent] 发送一条 [BrokerEvent].[LogEvent]
     */
    private fun postBrokerLogEvent(level: LogLevel, msg: String) {
        postBrokerEvent(BrokerEventType.LOG, LogEvent(level, msg))
    }

    override suspend fun connect(extras: Map<String, String>?) {
        if (brokerStatus != BrokerStatus.CREATED) return
        brokerStatus = BrokerStatus.CONNECTING
        kEvent.subscribe<BrokerEvent>(BrokerEventType.CONNECTION) { brokerEvent ->
            val connectionEvent = brokerEvent.data.data as ConnectionEvent
            when (connectionEvent.type) {
                ConnectionEventType.MD_LOGGED_IN,
                ConnectionEventType.TD_LOGGED_IN, -> {
                    if (mdConnected && tdConnected) brokerStatus = BrokerStatus.CONNECTED
                }
                ConnectionEventType.MD_NET_DISCONNECTED,
                ConnectionEventType.TD_NET_DISCONNECTED,
                ConnectionEventType.MD_LOGGED_OUT,
                ConnectionEventType.TD_LOGGED_OUT, -> {
                    if (brokerStatus != BrokerStatus.CLOSING) {
                        brokerStatus = BrokerStatus.CONNECTING
                    }
                }
                else -> Unit
            }
        }
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.connect】开始连接")
        if (!mdConnected) mdApi.connect()
        if (!tdConnected) tdApi.connect()
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.connect】连接成功")
    }

    override suspend fun close(extras: Map<String, String>?) {
        brokerStatus = BrokerStatus.CLOSING
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.close】开始关闭")
        tdApi.close()
        mdApi.close()
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.close】关闭成功")
        brokerStatus = BrokerStatus.CLOSED
        kEvent.release()
    }

    override fun getTradingDay(): LocalDate {
        val tradingDay = when {
            mdConnected -> mdApi.getTradingDay()
            tdConnected -> tdApi.getTradingDay()
            else -> null
        }
        return if (tradingDay == null) {
            throw Exception("行情前置与交易前置均不可用，无法获取当前交易日")
        } else {
            Converter.dateC2A(tradingDay)
        }
    }

    override fun setTestTickToTrade(value: Boolean) {
        mdApi.isTestingTickToTrade = value
        tdApi.isTestingTickToTrade = value
    }

    override suspend fun subscribeTicks(codes: Collection<String>, extras: Map<String, String>?): List<SecurityInfo> {
        return mdApi.subscribeMarketData(codes, extras)
    }

    override suspend fun unsubscribeTicks(codes: Collection<String>, extras: Map<String, String>?) {
        mdApi.unsubscribeMarketData(codes, extras)
    }

    override suspend fun subscribeAllTicks(extras: Map<String, String>?): List<SecurityInfo> {
        return mdApi.subscribeAllMarketData(extras)
    }

    override suspend fun unsubscribeAllTicks(extras: Map<String, String>?) {
        mdApi.unsubscribeAllMarketData(extras)
    }

    override suspend fun subscribeTick(code: String, extras: Map<String, String>?): SecurityInfo? {
        return subscribeTicks(listOf(code), extras).firstOrNull()
    }

    override suspend fun unsubscribeTick(code: String, extras: Map<String, String>?) {
        unsubscribeTicks(listOf(code), extras)
    }

    /**
     * [useCache] 无效，总是查询本地维护的数据，CTP 无此查询接口
     */
    override suspend fun queryTickSubscriptions(useCache: Boolean, extras: Map<String, String>?): List<String> {
        return mdApi.querySubscriptions(useCache, extras)
    }

    override suspend fun queryLastTick(code: String, useCache: Boolean, extras: Map<String, String>?): Tick? {
        return runWithRetry({ tdApi.queryLastTick(code, useCache, extras) })
    }

    override suspend fun querySecurity(code: String, useCache: Boolean, extras: Map<String, String>?): SecurityInfo? {
        return runWithRetry({ tdApi.querySecurity(code, useCache, extras) })
    }

    override suspend fun querySecurities(
        productId: String,
        useCache: Boolean,
        extras: Map<String, String>?
    ): List<SecurityInfo> {
        return runWithRetry({ tdApi.querySecurities(productId, useCache, extras) })
    }

    override suspend fun queryAllSecurities(useCache: Boolean, extras: Map<String, String>?): List<SecurityInfo> {
        return runWithRetry({ tdApi.queryAllSecurities(useCache, extras) })
    }

    override suspend fun queryOptions(
        underlyingCode: String,
        type: OptionsType?,
        useCache: Boolean,
        extras: Map<String, String>?
    ): List<SecurityInfo> {
        return runWithRetry({ tdApi.queryOptions(underlyingCode, type, useCache, extras) })
    }

    override suspend fun insertOrder(
        code: String,
        price: Double,
        volume: Int,
        direction: Direction,
        offset: OrderOffset,
        orderType: OrderType,
        minVolume: Int,
        extras: Map<String, String>?
    ): Order {
        return tdApi.insertOrder(code, price, volume, direction, offset, orderType, minVolume, extras)
    }

    override suspend fun cancelOrder(orderId: String, extras: Map<String, String>?) {
        tdApi.cancelOrder(orderId, extras)
    }

    override suspend fun cancelAllOrders(extras: Map<String, String>?) {
        queryOrders(onlyUnfinished = true).forEach { cancelOrder(it.orderId) }
    }

    override suspend fun queryOrder(orderId: String, useCache: Boolean, extras: Map<String, String>?): Order? {
        return runWithRetry({ tdApi.queryOrder(orderId, useCache, extras) })
    }

    override suspend fun queryOrders(code: String?, onlyUnfinished: Boolean, useCache: Boolean, extras: Map<String, String>?): List<Order> {
        return runWithRetry({ tdApi.queryOrders(code, onlyUnfinished, useCache, extras) })
    }

    override suspend fun queryTrade(tradeId: String, useCache: Boolean, extras: Map<String, String>?): Trade? {
        return runWithRetry({ tdApi.queryTrade(tradeId, useCache, extras) })
    }

    override suspend fun queryTrades(code: String?, orderId: String?, useCache: Boolean, extras: Map<String, String>?): List<Trade> {
        return runWithRetry({ tdApi.queryTrades(code, orderId, useCache, extras) })
    }

    override suspend fun queryAssets(useCache: Boolean, extras: Map<String, String>?): Assets {
        return runWithRetry({ tdApi.queryAssets(useCache, extras) })
    }

    override suspend fun queryPosition(code: String, direction: Direction, useCache: Boolean, extras: Map<String, String>?): Position? {
        return runWithRetry({ tdApi.queryPosition(code, direction, useCache, extras) })
    }

    override suspend fun queryPositionDetails(code: String, direction: Direction, useCache: Boolean, extras: Map<String, String>?): PositionDetails? {
        return runWithRetry({ tdApi.queryPositionDetails(code, direction, useCache, extras) })
    }

    override suspend fun queryPositionDetails(code: String?, useCache: Boolean, extras: Map<String, String>?): List<PositionDetails> {
        return runWithRetry({ tdApi.queryPositionDetails(code, useCache, extras) })
    }

    override suspend fun queryPositions(code: String?, useCache: Boolean, extras: Map<String, String>?): List<Position> {
        return runWithRetry({ tdApi.queryPositions(code, useCache, extras) })
    }
}