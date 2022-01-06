@file:Suppress("JoinDeclarationAndAssignment")

package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
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
    var mdConnected: Boolean = false
        set(value) {
            if (field == value) return
            field = value
            brokerStatus = if (value && tdConnected) BrokerStatus.CONNECTED else {
                if (brokerStatus != BrokerStatus.CLOSING) BrokerStatus.CONNECTING else BrokerStatus.CLOSING
            }
        }
    var tdConnected: Boolean = false
        set(value) {
            if (field == value) return
            field = value
            brokerStatus = if (value && mdConnected) BrokerStatus.CONNECTED else {
                if (brokerStatus != BrokerStatus.CLOSING) BrokerStatus.CONNECTING else BrokerStatus.CLOSING
            }
        }

    override val kEvent: KEvent = KEvent(
        name = "CTP-$account",
        defaultDispatchMode = EventDispatchMode.POSTING,
        defaultThreadMode = SubscriberThreadMode.POSTING,
    )

    /**
     * 协程 scope
     */
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    override var isTestingTickToTrade: Boolean = false
        private set(value) {
            if (field == value) return
            field = value
            postBrokerEvent(BrokerEventType.CUSTOM_EVENT, CustomEvent("TICK_TO_TRADE", value.toString()))
        }

    override var dataApi: DataApi? = null
        set(value) {
            if (field == value) return
            field = value
            postBrokerEvent(BrokerEventType.CUSTOM_EVENT, CustomEvent("DATA_API", value?.account ?: ""))
        }

    init {
        mdApi = CtpMdApi(this)
        tdApi = CtpTdApi(this)
        mdApi.tdApi = tdApi
        tdApi.mdApi = mdApi
    }

    /**
     * 向 [kEvent] 发送一条 [BrokerEvent]
     */
    fun postBrokerEvent(type: BrokerEventType, data: Any) {
        kEvent.post(type, BrokerEvent(type, sourceId, data))
    }

    /**
     * 向 [kEvent] 发送一条 [BrokerEvent].[LogEvent]
     */
    fun postBrokerLogEvent(level: LogLevel, msg: String) {
        postBrokerEvent(BrokerEventType.LOG, LogEvent(level, msg))
    }

    override suspend fun connect(extras: Map<String, String>?) {
        if (brokerStatus != BrokerStatus.CREATED) return
        brokerStatus = BrokerStatus.CONNECTING
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.connect】开始连接")
        mdApi.connect()
        tdApi.connect()
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.connect】连接成功")
    }

    override suspend fun close(extras: Map<String, String>?) {
        brokerStatus = BrokerStatus.CLOSING
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.close】开始关闭")
        tdApi.close()
        mdApi.close()
        postBrokerLogEvent(LogLevel.INFO, "【CtpBrokerApi.close】关闭成功")
        brokerStatus = BrokerStatus.CLOSED
        scope.cancel()
        kEvent.release()
    }

    override suspend fun getTradingDay(extras: Map<String, String>?): LocalDate {
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

    override suspend fun setTestingTickToTrade(value: Boolean, extras: Map<String, String>?) {
        isTestingTickToTrade = value
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
        type: OptionsType,
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
        closePositionPrice: Double,
        orderType: OrderType,
        minVolume: Int,
        extras: Map<String, String>?
    ): Order {
        return tdApi.insertOrder(code, price, volume, direction, offset, closePositionPrice, orderType, minVolume, extras)
    }

    override suspend fun cancelOrder(orderId: String, extras: Map<String, String>?) {
        tdApi.cancelOrder(orderId, extras)
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