@file:Suppress("JoinDeclarationAndAssignment")

package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.broker.api.*
import org.rationalityfrontline.ktrader.datatype.*
import java.time.LocalDate

/**
 * [BrokerApi] 的 CTP 实现
 */
class CtpBrokerApi(val config: CtpConfig, override val kEvent: KEvent) : BrokerApi {
    
    private val mdApi: CtpMdApi
    private val tdApi: CtpTdApi

    override val name: String = CtpBrokerInfo.name
    override val version: String = CtpBrokerInfo.version
    override val account: String = this.config.investorId
    override val mdConnected: Boolean get() = mdApi.connected
    override val tdConnected: Boolean get() = tdApi.connected

    init {
        mdApi = CtpMdApi(this.config, kEvent, sourceId)
        tdApi = CtpTdApi(this.config, kEvent, sourceId)
        mdApi.tdApi = tdApi
        tdApi.mdApi = mdApi
    }

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
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.connect】开始连接")
        if (!mdConnected) mdApi.connect()
        if (!tdConnected) tdApi.connect()
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.connect】连接成功")
    }

    override fun close() {
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.close】开始关闭")
        tdApi.close()
        mdApi.close()
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.close】关闭成功")
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

    override suspend fun subscribeTicks(codes: Collection<String>, extras: Map<String, String>?) {
        mdApi.subscribeMarketData(codes, extras)
    }

    override suspend fun unsubscribeTicks(codes: Collection<String>, extras: Map<String, String>?) {
        mdApi.unsubscribeMarketData(codes, extras)
    }

    override suspend fun subscribeAllTicks(extras: Map<String, String>?) {
        mdApi.subscribeAllMarketData(extras)
    }

    override suspend fun unsubscribeAllTicks(extras: Map<String, String>?) {
        mdApi.unsubscribeAllMarketData(extras)
    }

    override suspend fun subscribeTick(code: String, extras: Map<String, String>?) {
        subscribeTicks(listOf(code), extras)
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
        return runWithRetry({ tdApi.queryInstrument(code, useCache, extras) })
    }

    override suspend fun queryAllSecurities(useCache: Boolean, extras: Map<String, String>?): List<SecurityInfo> {
        return runWithRetry({ tdApi.queryAllInstruments(useCache, extras) })
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

    override suspend fun prepareFeeCalculation(codes: Collection<String>?, extras: Map<String, String>?) {
        tdApi.prepareFeeCalculation(codes, extras)
    }

    override fun calculatePosition(position: Position, extras: Map<String, String>?) {
        tdApi.calculatePosition(position, extras = extras)
    }

    override fun calculateOrder(order: Order, extras: Map<String, String>?) {
        tdApi.calculateOrder(order, extras)
    }

    override fun calculateTrade(trade: Trade, extras: Map<String, String>?) {
        tdApi.calculateTrade(trade, extras)
    }
}