@file:Suppress("JoinDeclarationAndAssignment")

package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.broker.api.*
import java.time.LocalDate

class CtpBrokerApi(config: Map<String, Any>, kEvent: KEvent) : BrokerApi(config, kEvent) {
    private val ctpConfig: CtpConfig = CtpBrokerInfo.parseConfig(config)
    private val mdApi: CtpMdApi
    private val tdApi: CtpTdApi
    override val name: String = CtpBrokerInfo.name
    override val version: String = CtpBrokerInfo.version
    override val account: String = ctpConfig.investorId
    override val mdConnected: Boolean get() = mdApi.connected
    override val tdConnected: Boolean get() = tdApi.connected

    init {
        mdApi = CtpMdApi(ctpConfig, kEvent, sourceId)
        tdApi = CtpTdApi(ctpConfig, kEvent, sourceId)
        mdApi.tdApi = tdApi
        tdApi.mdApi = mdApi
    }

    override suspend fun connect(connectMd: Boolean, connectTd: Boolean, extras: Map<String, Any>?) {
        if (connectMd && !mdConnected) mdApi.connect()
        if (connectTd && !tdConnected) tdApi.connect()
    }

    override suspend fun close() {
        tdApi.close()
        mdApi.close()
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
            LocalDate.parse("${tradingDay.slice(0..3)}-${tradingDay.slice(4..5)}-${tradingDay.slice(6..7)}")
        }
    }

    override suspend fun subscribeMarketData(codes: Collection<String>, extras: Map<String, Any>?) {
        mdApi.subscribeMarketData(codes, extras)
    }

    override suspend fun unsubscribeMarketData(codes: Collection<String>, extras: Map<String, Any>?) {
        mdApi.unsubscribeMarketData(codes, extras)
    }

    override suspend fun subscribeAllMarketData(extras: Map<String, Any>?) {
        mdApi.subscribeAllMarketData(extras)
    }

    override suspend fun unsubscribeAllMarketData(extras: Map<String, Any>?) {
        mdApi.unsubscribeAllMarketData(extras)
    }

    /**
     * [useCache] 与 [retry] 无效，总是查询本地维护的数据，CTP 无此查询接口
     */
    override suspend fun querySubscriptions(useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): List<String> {
        return mdApi.querySubscriptions(useCache, extras)
    }

    override suspend fun queryLastTick(code: String, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): Tick? {
        return if (retry) {
            runWithRetry({ tdApi.queryLastTick(code, useCache, extras) })
        } else {
            tdApi.queryLastTick(code, useCache, extras)
        }
    }

    override suspend fun queryInstrument(code: String, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): Instrument? {
        return if (retry) {
            runWithRetry({ tdApi.queryInstrument(code, useCache, extras) })
        } else {
            tdApi.queryInstrument(code, useCache, extras)
        }
    }

    override suspend fun queryAllInstruments(useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): List<Instrument> {
        return if (retry) {
            runWithRetry({ tdApi.queryAllInstruments(useCache, extras) })
        } else {
            tdApi.queryAllInstruments(useCache, extras)
        }
    }

    override fun insertOrder(
        code: String,
        price: Double,
        volume: Int,
        direction: Direction,
        offset: OrderOffset,
        orderType: OrderType,
        extras: Map<String, Any>?
    ): Order {
        return tdApi.insertOrder(code, price, volume, direction, offset, orderType, extras)
    }

    override fun cancelOrder(orderId: String, extras: Map<String, Any>?) {
        tdApi.cancelOrder(orderId, extras)
    }

    override suspend fun queryOrder(orderId: String, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): Order? {
        return if (retry){
            runWithRetry({ tdApi.queryOrder(orderId, useCache, extras) })
        } else {
            tdApi.queryOrder(orderId, useCache, extras)
        }
    }

    override suspend fun queryOrders(code: String?, onlyUnfinished: Boolean, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): List<Order> {
        return if (retry) {
            runWithRetry({ tdApi.queryOrders(code, onlyUnfinished, useCache, extras) })
        } else {
            tdApi.queryOrders(code, onlyUnfinished, useCache, extras)
        }
    }

    override suspend fun queryTrade(tradeId: String, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): Trade? {
        return if (retry) {
            runWithRetry({ tdApi.queryTrade(tradeId, useCache, extras) })
        } else {
            tdApi.queryTrade(tradeId, useCache, extras)
        }
    }

    override suspend fun queryTrades(code: String?, orderId: String?, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): List<Trade> {
        return if (retry) {
            runWithRetry({ tdApi.queryTrades(code, orderId, useCache, extras) })
        } else {
            tdApi.queryTrades(code, orderId, useCache, extras)
        }
    }

    override suspend fun queryAssets(useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): Assets {
        return if (retry) {
            runWithRetry({ tdApi.queryAssets(useCache, extras) })
        } else {
            tdApi.queryAssets(useCache, extras)
        }
    }

    override suspend fun queryPosition(code: String, direction: Direction, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): Position? {
        return if (retry) {
            runWithRetry({ tdApi.queryPosition(code, direction, useCache, extras) })
        } else {
            tdApi.queryPosition(code, direction, useCache, extras)
        }
    }

    override suspend fun queryPositions(code: String?, useCache: Boolean, retry: Boolean, extras: Map<String, Any>?): List<Position> {
        return if (retry) {
            runWithRetry({ tdApi.queryPositions(code, useCache, extras) })
        } else {
            tdApi.queryPositions(code, useCache, extras)
        }
    }

    override suspend fun prepareFeeCalculation(codes: Collection<String>?, extras: Map<String, Any>?) {
        tdApi.prepareFeeCalculation(codes, extras)
    }

    override fun calculatePosition(position: Position, extras: Map<String, Any>?) {
        tdApi.calculatePosition(position, extras = extras)
    }

    override fun calculateOrder(order: Order, extras: Map<String, Any>?) {
        tdApi.calculateOrder(order, extras)
    }

    override fun calculateTrade(trade: Trade, extras: Map<String, Any>?) {
        tdApi.calculateTrade(trade, extras)
    }
}