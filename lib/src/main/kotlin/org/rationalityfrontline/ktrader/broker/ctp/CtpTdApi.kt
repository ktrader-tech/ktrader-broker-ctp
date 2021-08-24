@file:Suppress("UNCHECKED_CAST", "UNUSED_PARAMETER", "MemberVisibilityCanBePrivate")

package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.*
import org.rationalityfrontline.jctp.*
import org.rationalityfrontline.jctp.jctpConstants.*
import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.broker.api.*
import org.rationalityfrontline.ktrader.datatype.*
import java.io.File
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.math.max
import kotlin.math.sign

internal class CtpTdApi(val config: CtpConfig, val kEvent: KEvent, val sourceId: String) {
    private val tdApi: CThostFtdcTraderApi
    private val tdSpi: CtpTdSpi
    /**
     * 协程请求列表，每当网络断开（OnFrontDisconnected）时会清空（resumeWithException）
     */
    private val requestMap: ConcurrentHashMap<Int, RequestContinuation> = ConcurrentHashMap()
    /**
     * 自增的请求 id，每当网络连接时（OnFrontConnected）重置为 0
     */
    private val requestId = AtomicInteger(0)
    private fun nextRequestId(): Int = requestId.incrementAndGet()
    /**
     * 本地维护的订单引用（同一交易日内递增不重复的整数，做本地文件维护是因为 tdSpi.OnRtnTrade 的回报中只有 orderRef 而没有 frontId 与 sessionId，所以需要尽量保证 orderRef 的唯一性）。
     * 默认从 10000 开始（为了减少与其它 session 的订单引用重复的概率，更好的确保 orderRef 的跨 session 唯一性）
     */
    private val orderRef = AtomicInteger(10000)
    private fun nextOrderRef(): Int {
        val nextOrderRef = orderRef.incrementAndGet()
        cacheFile.writeText("$tradingDay\n$nextOrderRef")
        return nextOrderRef
    }
    /**
     * 协程 scope
     */
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    /**
     * 上次更新的交易日。当 [connected] 处于 false 状态时可能因过期而失效
     */
    private var tradingDay = ""
        set(value) {
            field = value
            tradingDate = Converter.dateC2A(value)
        }
    private var tradingDate = LocalDate.now()
    /**
     * 用于记录维护交易日及 orderRef 的缓存文件
     */
    private val cacheFile: File
    /**
     * 行情 Api 对象，用于获取最新 Tick，并在查询全市场合约时更新其 codeMap
     */
    lateinit var mdApi: CtpMdApi
    /**
     * 是否已调用过 [CThostFtdcTraderApi.Init]
     */
    private var inited = false
    /**
     * 交易前置是否已连接
     */
    private var frontConnected: Boolean = false
    /**
     * 是否已完成登录操作（即处于可用状态）
     */
    var connected: Boolean = false
        private set
    /**
     * 前置编号（客户端连接到的前置机的编号，同一交易前置地址的前置编号相同），用于确定 orderId，参见 tdSpi.OnRspUserLogin
     */
    var frontId: Int = 0
        private set
    /**
     * 会话编号（客户端连接到前置机的连接会话编号，不同连接的会话编号不同，断开重连后也会变动），用于确定 orderId，参见 tdSpi.OnRspUserLogin
     */
    var sessionId: Int = 0
        private set
    /**
     * 缓存的合约信息，key 为合约 code
     */
    val instruments: MutableMap<String, SecurityInfo> = mutableMapOf()
    /**
     * 品种代码表，key 为合约 code，value 为品种代码(productId)。用于从 code 快速映射到 [productStatusMap]
     */
    private val codeProductMap: MutableMap<String, String> = mutableMapOf()
    /**
     * 品种状态表，key 为品种代码，value 为品种状态
     */
    private val productStatusMap: MutableMap<String, MarketStatus> = mutableMapOf()
    /**
     * 缓存的 tick 表，key 为合约 code
     */
    private val cachedTickMap: MutableMap<String, Tick> = mutableMapOf()
    /**
     * 期货保证金类型
     */
    private var futuresMarginPriceType: MarginPriceType = MarginPriceType.PRE_SETTLEMENT_PRICE
    /**
     * 期权保证金类型
     */
    private var optionsMarginPriceType: MarginPriceType = MarginPriceType.PRE_SETTLEMENT_PRICE
    /**
     * 本地缓存的资产信息，并不维护
     */
    private val assets: Assets = Assets(config.investorId, LocalDate.now(), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    /**
     * 上次查询账户资产的时间
     */
    private var lastQueryAssetsTime = 0L
    /**
     * 本地维护的持仓信息，key 为 code，value 为 [BiPosition]
     */
    private val positions: MutableMap<String, BiPosition> = mutableMapOf()
    /**
     * 缓存的订单，key 为 orderRef（本 session 订单）或 orderId（其它 session 订单），value 为 order
     */
    private val todayOrders: MutableMap<String, Order> = mutableMapOf()
    /**
     * 缓存的成交记录
     */
    private val todayTrades: MutableList<Trade> = mutableListOf()
    /**
     * 缓存的未完成的多头订单，按挂单价从低到高排序
     */
    private val unfinishedLongOrders: MutableList<Order> = mutableListOf()
    /**
     * 缓存的未完成的空头订单，按挂单价从低到高排序
     */
    private val unfinishedShortOrders: MutableList<Order> = mutableListOf()
    /**
     * 做多订单的最高挂单价，用于检测自成交
     */
    private val maxLongPrice: Double get() = unfinishedLongOrders.firstOrNull()?.price ?: Double.NEGATIVE_INFINITY
    /**
     * 做空订单的最低挂单价，用于检测自成交
     */
    private val minShortPrice: Double get() = unfinishedShortOrders.lastOrNull()?.price ?: Double.POSITIVE_INFINITY
    /**
     * 合约撤单次数统计，用于检测频繁撤单，key 为 code，value 为撤单次数
     */
    private val cancelStatistics: MutableMap<String, Int> = mutableMapOf()

    init {
        val cachePath = config.cachePath.ifBlank { ".data/ctp/" }
        val tdCachePath = "${if (cachePath.endsWith('/')) cachePath else "$cachePath/"}${config.investorId.ifBlank { "unknown" }}/td/"
        File(tdCachePath).mkdirs()
        cacheFile = File("${tdCachePath}cache.txt")
        tdApi = CThostFtdcTraderApi.CreateFtdcTraderApi(tdCachePath)
        tdSpi = CtpTdSpi()
        tdApi.apply {
            RegisterSpi(tdSpi)
            // QUICK 订阅私有流
            SubscribePrivateTopic(THOST_TE_RESUME_TYPE.THOST_TERT_QUICK)
            // RESTART 订阅公有流，用于获取合约交易状态，参见 tdSpi.OnRtnInstrumentStatus
            SubscribePublicTopic(THOST_TE_RESUME_TYPE.THOST_TERT_RESTART)
            config.tdFronts.forEach { tFront ->
                RegisterFront(tFront)
            }
        }
    }

    /**
     * 获取合约当前状态，如果合约不存在或无法查询到状态，返回 [MarketStatus.UNKNOWN]
     */
    fun getInstrumentStatus(code: String): MarketStatus {
        val product = codeProductMap[code]
        return if (product == null) MarketStatus.UNKNOWN else productStatusMap[product] ?: MarketStatus.UNKNOWN
    }

    /**
     * 依据 [order] 的 direction 向未成交订单缓存中有序插入未成交订单（按挂单价从低到高）
     */
    private fun insertUnfinishedOrder(order: Order) {
        when (order.direction) {
            Direction.LONG -> unfinishedLongOrders.insert(order)
            Direction.SHORT -> unfinishedShortOrders.insert(order)
            Direction.UNKNOWN -> postBrokerLogEvent(LogLevel.WARNING, "【CtpTdApi.insertUnfinishedOrder】订单方向为 Direction.UNKNOWN（${order.code}, ${order.orderId}）")
        }
    }

    /**
     * 从未成交订单缓存中移除未成交订单
     */
    private fun removeUnfinishedOrder(order: Order) {
        when (order.direction) {
            Direction.LONG -> unfinishedLongOrders.remove(order)
            Direction.SHORT -> unfinishedShortOrders.remove(order)
            Direction.UNKNOWN -> postBrokerLogEvent(LogLevel.WARNING, "【CtpTdApi.removeUnfinishedOrder】订单方向为 Direction.UNKNOWN（${order.code}, ${order.orderId}）")
        }
    }

    /**
     * 将符合 [predicate] 条件的标签为 [tag] 的协程请求用 [result] 正常完成
     */
    private fun <T> resumeRequests(tag: String, result: T, predicate: ((RequestContinuation) -> Boolean)? = null) {
        requestMap.values.filter { it.tag == tag }.forEach { req ->
            if (predicate?.invoke(req) != false) {
                (req.continuation as Continuation<T>).resume(result)
                requestMap.remove(req.requestId)
            }
        }
    }

    /**
     * 将符合 [predicate] 条件的标签为 [tag] 的协程请求用 [errorInfo] 的报错信息异常完成
     */
    private fun resumeRequestsWithException(tag: String, errorInfo: String, predicate: ((RequestContinuation) -> Boolean)? = null) {
        requestMap.values.filter { it.tag == tag }.forEach { req ->
            if (predicate?.invoke(req) != false) {
                req.continuation.resumeWithException(Exception(errorInfo))
                requestMap.remove(req.requestId)
            }
        }
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

    /**
     * 向 [kEvent] 发送一条 [BrokerEvent].[ConnectionEvent]
     */
    private fun postBrokerConnectionEvent(msgType: ConnectionEventType, msg: String = "") {
        postBrokerEvent(BrokerEventType.CONNECTION, ConnectionEvent(msgType, msg))
    }

    /**
     * 连接交易前置并自动完成登录（还会自动查询持仓、订单、成交记录等信息，详见 [CtpTdSpi.OnFrontConnected]）。在无法连接至前置的情况下可能会长久阻塞。
     * 该操作不可加超时限制，因为可能在双休日等非交易时间段启动程序。
     */
    suspend fun connect() {
        if (inited) return
        suspendCoroutine<Unit> { continuation ->
            val requestId = Int.MIN_VALUE // 因为 OnFrontConnected 中 requestId 会重置为 0，为防止 requestId 重复，取整数最小值
            requestMap[requestId] = RequestContinuation(requestId, continuation, "connect")
            postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】连接前置服务器...")
            tdApi.Init()
            inited = true
        }
    }

    /**
     * 关闭并释放资源，会发送一条 [BrokerEventType.CONNECTION] ([ConnectionEventType.TD_NET_DISCONNECTED]) 信息
     */
    fun close() {
        if (frontConnected) tdSpi.OnFrontDisconnected(0)
        scope.cancel()
        instruments.clear()
        productStatusMap.clear()
        codeProductMap.clear()
        cachedTickMap.clear()
        todayOrders.clear()
        unfinishedLongOrders.clear()
        unfinishedShortOrders.clear()
        cancelStatistics.clear()
        todayTrades.clear()
        positions.clear()
        tdApi.Release()
        tdApi.delete()
    }

    /**
     * 获取当前交易日
     */
    fun getTradingDay(): String {
        return if (connected) tradingDay else tdApi.GetTradingDay()
    }

    /**
     * 向交易所发送订单报单，会自动检查自成交。[extras.minVolume: Int]【最小成交量。仅当 [orderType] 为 [OrderType.FAK] 时生效】
     */
    suspend fun insertOrder(
        code: String,
        price: Double,
        volume: Int,
        direction: Direction,
        offset: OrderOffset,
        orderType: OrderType,
        extras: Map<String, String>? = null
    ): Order {
        val (exchangeId, instrumentId) = parseCode(code)
        val orderRef = nextOrderRef().toString()
        // 检查是否存在自成交风险
        var errorInfo: String? = when {
            direction == Direction.LONG && price >= minShortPrice -> "本地拒单：存在自成交风险（当前做多价格为 $price，最低做空价格为 ${minShortPrice}）"
            direction == Direction.SHORT && price <= maxLongPrice -> "本地拒单：存在自成交风险（当前做空价格为 $price，最高做多价格为 ${maxLongPrice}）"
            else -> null
        }
        // 无自成交风险，执行下单操作
        if (errorInfo == null) {
            val reqField = CThostFtdcInputOrderField().apply {
                this.orderRef = orderRef
                brokerID = config.brokerId
                investorID = config.investorId
                exchangeID = exchangeId
                instrumentID = instrumentId
                limitPrice = price
                this.direction = Converter.directionA2C(direction)
                volumeTotalOriginal = volume
                volumeCondition = THOST_FTDC_VC_AV
                combOffsetFlag = Converter.offsetA2C(offset)
                combHedgeFlag = Converter.THOST_FTDC_HF_Speculation
                contingentCondition = THOST_FTDC_CC_Immediately
                forceCloseReason = THOST_FTDC_FCC_NotForceClose
                isAutoSuspend = 0
                userForceClose = 0
                when (orderType) {
                    OrderType.LIMIT -> {
                        orderPriceType = THOST_FTDC_OPT_LimitPrice
                        timeCondition = THOST_FTDC_TC_GFD
                    }
                    OrderType.FAK -> {
                        orderPriceType = THOST_FTDC_OPT_LimitPrice
                        timeCondition = THOST_FTDC_TC_IOC
                        val rawMinVolume = extras?.get("minVolume")?.toIntOrNull()
                        if (rawMinVolume != null) {
                            volumeCondition = THOST_FTDC_VC_MV
                            minVolume = rawMinVolume
                        }
                    }
                    OrderType.FOK -> {
                        orderPriceType = THOST_FTDC_OPT_LimitPrice
                        timeCondition = THOST_FTDC_TC_IOC
                        volumeCondition = THOST_FTDC_VC_CV
                    }
                    OrderType.MARKET -> {
                        orderPriceType = THOST_FTDC_OPT_AnyPrice
                        timeCondition = THOST_FTDC_TC_IOC
                        limitPrice = 0.0
                    }
                    else -> {
                        errorInfo = "未支持 $orderType 类型的订单"
                    }
                }
            }
            if (errorInfo == null) {
                runWithResultCheck({ tdApi.ReqOrderInsert(reqField, nextRequestId()) }, {})
            }
        }
        // 构建返回的 order 对象
        val now = LocalDateTime.now()
        val order = Order(
            config.investorId,
            "${frontId}_${sessionId}_${orderRef}",
            code, price, volume, direction, offset, orderType,
            OrderStatus.SUBMITTING, "报单已提交",
            0, 0.0, 0.0, 0.0, 0.0,
            now, now,
            extras = mutableMapOf<String, String>().apply {
                if (extras != null) {
                    putAll(extras)
                }
            }
        )
        if (errorInfo == null) {
            todayOrders[orderRef] = order
            insertUnfinishedOrder(order)
        } else {
            order.status = OrderStatus.ERROR
            order.statusMsg = errorInfo!!
            todayOrders[orderRef] = order
        }
        return order.deepCopy()
    }

    /**
     * 撤单，会自动检查撤单次数是否达到 499 次上限。[orderId] 格式为 frontId_sessionId_orderRef
     */
    suspend fun cancelOrder(orderId: String, extras: Map<String, String>? = null) {
        val cancelReqField = CThostFtdcInputOrderActionField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            userID = config.investorId
            actionFlag = THOST_FTDC_AF_Delete
        }
        val splitResult = orderId.split('_')
        val order: Order?
        when (splitResult.size) {
            3 -> {
                cancelReqField.apply {
                    frontID = splitResult[0].toInt()
                    sessionID = splitResult[1].toInt()
                    orderRef = splitResult[2]
                    order = todayOrders[splitResult[2]] ?: todayOrders[orderId]
                    if (order != null) instrumentID = parseCode(order.code).second
                }
            }
            else -> {
                throw IllegalArgumentException("不合法的 orderId ($orderId)。正确格式为：frontId_sessionId_orderRef")
            }
        }
        if (order == null || order.orderId != orderId) {
            throw Exception("本地拒撤：未找到对应的订单记录")
        } else {
            if (order.status !in setOf(OrderStatus.UNKNOWN, OrderStatus.SUBMITTING, OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED)) {
                throw Exception("本地拒撤：订单当前状态不可撤（${order.status}）")
            }
            if (cancelStatistics.getOrDefault(order.code, 0) >= 499) {
                throw Exception("本地拒撤：达到撤单次数上限（已撤 ${cancelStatistics[order.code]} 次）")
            }
        }
        runWithResultCheck({ tdApi.ReqOrderAction(cancelReqField, nextRequestId()) }, {})
    }

    /**
     * 查询最新 [Tick]
     */
    suspend fun queryLastTick(code: String, useCache: Boolean, extras: Map<String, String>? = null): Tick? {
        if (useCache) {
            val cachedTick = mdApi.lastTicks[code]
            if (cachedTick != null) {
                cachedTick.status = getInstrumentStatus(code)
                cachedTickMap[code] = cachedTick
                return cachedTick
            }
        }
        val qryField = CThostFtdcQryDepthMarketDataField().apply {
            instrumentID = parseCode(code).second
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryDepthMarketData(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = code)
            }
        })
    }

    /**
     * 查询某一特定合约的信息。[extras.queryFee: Boolean = false]【是否查询保证金率及手续费率，如果之前没查过，可能会耗时。当 useCache 为 false 时无效】
     */
    suspend fun queryInstrument(code: String, useCache: Boolean = true, extras: Map<String, String>? = null): SecurityInfo? {
        if (useCache) {
            val cachedInstrument = instruments[code]
            if (cachedInstrument != null) {
                if (extras?.get("queryFee") == "true") {
                    prepareFeeCalculation(code)
                }
                return cachedInstrument
            }
        }
        val (exchangeId, instrumentId) = parseCode(code)
        val qryField = CThostFtdcQryInstrumentField().apply {
            exchangeID = exchangeId
            instrumentID = instrumentId
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInstrument(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = code)
            }
        })
    }

    /**
     * 查询全市场合约的信息
     */
    suspend fun queryAllInstruments(useCache: Boolean = true, extras: Map<String, String>? = null): List<SecurityInfo> {
        if (useCache && instruments.isNotEmpty()) return instruments.values.toList()
        val qryField = CThostFtdcQryInstrumentField()
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInstrument(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS * 2) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = mutableListOf<SecurityInfo>())
            }
        })
    }

    /**
     * 依据 [orderId] 查询 [Order]。[orderId] 格式为 frontId_sessionId_orderRef。未找到对应订单时返回 null。
     */
    suspend fun queryOrder(orderId: String, useCache: Boolean = true, extras: Map<String, String>? = null): Order? {
        if (useCache) {
            var order: Order? = todayOrders[orderId.split("_").last()] ?: todayOrders[orderId]
            if (order != null && order.orderId != orderId) {
                order = null
            }
            if (order != null) {
                calculateOrder(order)
                return order
            }
        }
        val qryField = CThostFtdcQryOrderField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
        }
        val requestId = nextRequestId()
        return runWithResultCheck<Order?>({ tdApi.ReqQryOrder(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryOrdersData(orderId))
            }
        })?.apply { calculateOrder(this) }
    }

    /**
     * 查询订单
     */
    suspend fun queryOrders(code: String? = null, onlyUnfinished: Boolean = true, useCache: Boolean = true, extras: Map<String, String>? = null): List<Order> {
        if (useCache) {
            var orders: List<Order> = if (onlyUnfinished) {
                mutableListOf<Order>().apply {
                    addAll(unfinishedLongOrders)
                    addAll(unfinishedShortOrders)
                }
            } else todayOrders.values.toList()
            if (code != null) {
                orders = orders.filter { it.code == code }
            }
            return orders.onEach {
                if (it.offset == OrderOffset.OPEN && it.volume > it.filledVolume) {
                    calculateOrder(it)
                }
            }
        } else {
            val qryField = CThostFtdcQryOrderField().apply {
                brokerID = config.brokerId
                investorID = config.investorId
                if (code != null) {
                    val (excId, insId) = parseCode(code)
                    exchangeID = excId
                    instrumentID = insId
                }
            }
            val requestId = nextRequestId()
            return runWithResultCheck<List<Order>>({ tdApi.ReqQryOrder(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS * 2) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryOrdersData(null, code, onlyUnfinished))
                }
            }).onEach { calculateOrder(it) }
        }
    }

    /**
     * 依据 [tradeId] 查询 [Trade]。[tradeId] 格式为 tradeId_orderRef。未找到对应成交记录时返回 null。
     */
    suspend fun queryTrade(tradeId: String, useCache: Boolean = true, extras: Map<String, String>? = null): Trade? {
        if (useCache) {
            val trade = todayTrades.find { it.tradeId == tradeId }
            if (trade != null) return trade
        }
        val qryField = CThostFtdcQryTradeField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            tradeID = tradeId.split("_").first()
        }
        val requestId = nextRequestId()
        return runWithResultCheck<Trade?>({ tdApi.ReqQryTrade(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryTradesData(tradeId))
            }
        })?.apply { calculateTrade(this) }
    }

    /**
     * 查询成交记录
     */
    suspend fun queryTrades(code: String? = null,  orderId: String? = null, useCache: Boolean = true, extras: Map<String, String>? = null): List<Trade> {
        if (useCache) {
            return when {
                orderId != null -> todayTrades.filter { it.orderId == orderId }
                code != null -> todayTrades.filter { it.code == code }
                else -> todayTrades.toList()
            }
        } else {
            val reqData = QueryTradesData()
            val qryField = CThostFtdcQryTradeField().apply {
                brokerID = config.brokerId
                investorID = config.investorId
                if (code != null) {
                    val (excId, insId) = parseCode(code)
                    exchangeID = excId
                    instrumentID = insId
                    reqData.code = code
                }
                if (orderId != null) {
                    val order = todayOrders[orderId.split("_").last()] ?: todayOrders[orderId] ?: return listOf()
                    val (excId, insId) = parseCode(order.code)
                    exchangeID = excId
                    instrumentID = insId
                    reqData.orderSysId = order.orderSysId
                }
            }
            val requestId = nextRequestId()
            return runWithResultCheck<List<Trade>>({ tdApi.ReqQryTrade(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS * 2) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, data = reqData)
                }
            }).onEach { calculateTrade(it) }
        }
    }

    /**
     * 查询账户资金信息
     */
    suspend fun queryAssets(useCache: Boolean = true, extras: Map<String, String>? = null): Assets {
        // 10 秒内，使用上次查询结果
        if (useCache && System.currentTimeMillis() - lastQueryAssetsTime < 10000) {
            return assets.deepCopy()
        }
        val qryField = CThostFtdcQryTradingAccountField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            currencyID = "CNY"
        }
        val requestId = nextRequestId()
        return runWithResultCheck<Assets>({ tdApi.ReqQryTradingAccount(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation)
            }
        }).apply {
            positionPnl = 0.0
            positions.values.forEach { bi ->
                bi.long?.let {
                    calculatePosition(it, false)
                    positionPnl += it.pnl
                }
                bi.short?.let {
                    calculatePosition(it, false)
                    positionPnl += it.pnl
                }
            }
        }
    }

    /**
     * 查询本地维护的持仓信息中合约 [code] 的持仓信息。如果 [isClose] 为 false（默认），返回其 [direction] 持仓，否则返回 [direction] 的反向持仓。如无持仓，返回 null
     */
    private fun queryCachedPosition(code: String, direction: Direction, isClose: Boolean = false): Position? {
        if (direction == Direction.UNKNOWN) return null
        val biPosition = positions[code] ?: return null
        return when (direction) {
            Direction.LONG -> if (isClose) biPosition.short else biPosition.long
            Direction.SHORT -> if (isClose) biPosition.long else biPosition.short
            else -> null
        }
    }

    /**
     * 查询合约 [code] 的 [direction] 方向的持仓信息
     */
    suspend fun queryPosition(code: String, direction: Direction, useCache: Boolean = true, extras: Map<String, String>? = null): Position? {
        if (direction == Direction.UNKNOWN) return null
        return if (useCache) {
            queryCachedPosition(code, direction)?.deepCopy()
        } else {
            val qryField = CThostFtdcQryInvestorPositionField().apply {
                instrumentID = parseCode(code).second
            }
            val requestId = nextRequestId()
            runWithResultCheck({ tdApi.ReqQryInvestorPosition(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, tag = direction.name, data = mutableListOf<Position>())
                }
            })
        }
    }

    /**
     * 查询持仓信息，如果 [code] 为 null（默认），则查询账户整体持仓信息
     */
    suspend fun queryPositions(code: String? = null, useCache: Boolean = true, extras: Map<String, String>? = null): List<Position> {
        if (useCache) {
            val positionList = mutableListOf<Position>()
            // 查询全体持仓
            if (code.isNullOrEmpty()) {
                positions.values.forEach { biPosition ->
                    biPosition.long?.let { positionList.add(it) }
                    biPosition.short?.let { positionList.add(it) }
                }
            } else { // 查询单合约持仓
                positions[code]?.let { biPosition ->
                    biPosition.long?.let { positionList.add(it) }
                    biPosition.short?.let { positionList.add(it) }
                }
            }
            positionList.forEach { calculatePosition(it) }
            return positionList.map { it.deepCopy() }
        } else {
            val qryField = CThostFtdcQryInvestorPositionField().apply {
                if (!code.isNullOrEmpty()) instrumentID = parseCode(code).second
            }
            val requestId = nextRequestId()
            return runWithResultCheck({ tdApi.ReqQryInvestorPosition(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS * 2) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, tag = code ?: "", data = mutableListOf<Position>())
                }
            })
        }
    }

    /**
     * 查询合约 [code] 的 [direction] 方向的持仓明细
     */
    suspend fun queryPositionDetails(code: String, direction: Direction, useCache: Boolean, extras: Map<String, String>?): PositionDetails? {
        val qryField = CThostFtdcQryInvestorPositionDetailField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            instrumentID = parseCode(code).second
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInvestorPositionDetail(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryPositionDetailsData(code, direction))
            }
        })
    }

    /**
     * 查询持仓明细，如果 [code] 为 null（默认），则查询账户整体持仓明细
     */
    suspend fun queryPositionDetails(code: String?, useCache: Boolean, extras: Map<String, String>?): List<PositionDetails> {
        val qryField = CThostFtdcQryInvestorPositionDetailField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInvestorPositionDetail(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryPositionDetailsData(code))
            }
        })
    }

    /**
     * 查询期货及期权的保证金价格类型
     */
    private suspend fun queryMarginPriceType() {
        val qryField = CThostFtdcQryBrokerTradingParamsField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            currencyID = "CNY"
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryBrokerTradingParams(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation)
            }
        })
    }

    /**
     * 查询期货保证金率，如果 [code] 为 null（默认），则查询所有当前持仓合约的保证金率。
     * 已查过保证金率的不会再次查询。查询到的结果会自动更新到对应的 [instruments] 中
     */
    private suspend fun queryFuturesMarginRate(code: String? = null) {
        if (code == null) {
            val qryField = CThostFtdcQryInstrumentMarginRateField().apply {
                brokerID = config.brokerId
                investorID = config.investorId
                hedgeFlag = THOST_FTDC_HF_Speculation
            }
            val requestId = nextRequestId()
            runWithResultCheck<Unit>({ tdApi.ReqQryInstrumentMarginRate(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        } else {
            val instrument = instruments[code]
            if (instrument != null && instrument.marginRate == null && instrument.type == SecurityType.FUTURES) {
                val qryField = CThostFtdcQryInstrumentMarginRateField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    hedgeFlag = THOST_FTDC_HF_Speculation
                    instrumentID = parseCode(code).second
                }
                val requestId = nextRequestId()
                runWithResultCheck<Unit>({ tdApi.ReqQryInstrumentMarginRate(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                        requestMap[requestId] = RequestContinuation(requestId, continuation)
                    }
                })
            }
        }
    }

    /**
     * 查询期权保证金，如果 [code] 为 null（默认），则查询所有当前持仓合约的保证金。
     * 已查过保证金的不会再次查询。查询到的结果会自动更新到对应的 [instruments] 中，字段映射参见 [Converter.optionsMarginC2A]
     */
    private suspend fun queryOptionsMargin(code: String? = null) {
        if (code == null) {
            val qryField = CThostFtdcQryOptionInstrTradeCostField().apply {
                brokerID = config.brokerId
                investorID = config.investorId
                hedgeFlag = THOST_FTDC_HF_Speculation
            }
            val requestId = nextRequestId()
            runWithResultCheck<Unit>({ tdApi.ReqQryOptionInstrTradeCost(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        } else {
            val instrument = instruments[code]
            if (instrument != null && instrument.marginRate == null && instrument.type == SecurityType.OPTIONS) {
                val qryField = CThostFtdcQryOptionInstrTradeCostField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    hedgeFlag = THOST_FTDC_HF_Speculation
                    instrumentID = parseCode(code).second
                }
                val requestId = nextRequestId()
                runWithResultCheck<Unit>({ tdApi.ReqQryOptionInstrTradeCost(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                        requestMap[requestId] = RequestContinuation(requestId, continuation)
                    }
                })
            }
        }
    }

    /**
     * 查询期货手续费率，如果 [code] 为 null（默认），则查询所有当前持仓合约的手续费率。如果遇到中金所合约，会进行申报手续费的二次查询。
     * 已查过手续费的不会再次查询。查询到的结果会自动更新到对应的 [instruments] 中
     */
    private suspend fun queryFuturesCommissionRate(code: String? = null) {
        if (code == null) {
            val qryField = CThostFtdcQryInstrumentCommissionRateField().apply {
                brokerID = config.brokerId
                investorID = config.investorId
            }
            val requestId = nextRequestId()
            runWithResultCheck<List<Job>>({ tdApi.ReqQryInstrumentCommissionRate(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, data = mutableListOf<Job>())
                }
            }).forEach { it.join() }
        } else {
            val instrument = instruments[code]
            if (instrument != null && instrument.commissionRate == null && instrument.type == SecurityType.FUTURES) {
                val qryField = CThostFtdcQryInstrumentCommissionRateField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    val (excId, insId) = parseCode(code)
                    exchangeID = excId
                    instrumentID = insId
                }
                val requestId = nextRequestId()
                runWithResultCheck<List<Job>>({ tdApi.ReqQryInstrumentCommissionRate(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                        requestMap[requestId] = RequestContinuation(requestId, continuation, data = mutableListOf<Job>())
                    }
                }).forEach { it.join() }
            }
        }
    }

    /**
     * 查询期货申报手续费，仅限中金所。
     * 已查过手续费的依然会再次查询。查询到的结果会自动更新到对应的 [instruments] 中
     */
    private suspend fun queryFuturesOrderCommissionRate(code: String) {
        val qryField = CThostFtdcQryInstrumentOrderCommRateField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            instrumentID = parseCode(code).second
        }
        val requestId = nextRequestId()
        runWithResultCheck<Unit>({ tdApi.ReqQryInstrumentOrderCommRate(qryField, requestId) }, {
            suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation)
            }
        })
    }

    /**
     * 查询期权手续费率，如果 [code] 为 null（默认），则查询所有当前持仓合约的手续费率。
     * 已查过手续费的不会再次查询。查询到的结果会自动更新到对应的 [instruments] 中
     */
    private suspend fun queryOptionsCommissionRate(code: String? = null) {
        if (code == null) {
            val qryField = CThostFtdcQryOptionInstrCommRateField().apply {
                brokerID = config.brokerId
                investorID = config.investorId
            }
            val requestId = nextRequestId()
            runWithResultCheck<Unit>({ tdApi.ReqQryOptionInstrCommRate(qryField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        } else {
            val instrument = instruments[code]
            if (instrument != null && instrument.commissionRate == null && instrument.type == SecurityType.OPTIONS) {
                val qryField = CThostFtdcQryOptionInstrCommRateField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    val (excId, insId) = parseCode(code)
                    exchangeID = excId
                    instrumentID = insId
                }
                val requestId = nextRequestId()
                runWithResultCheck<Unit>({ tdApi.ReqQryOptionInstrCommRate(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                        requestMap[requestId] = RequestContinuation(requestId, continuation)
                    }
                })
            }
        }
    }

    /**
     * 获取缓存的期货/期权手续费率，如果没有，则查询后再获取
     */
    private fun getOrQueryCommissionRate(instrument: SecurityInfo): CommissionRate? {
        if (config.disableFeeCalculation) return null
        if (instrument.commissionRate == null) {
            runBlocking { prepareFeeCalculation(instrument.code, false) }
        }
        return instrument.commissionRate
    }

    /**
     * 获取缓存的期货保证金率，如果没有，则查询后再获取
     */
    private fun getOrQueryMarginRate(instrument: SecurityInfo): MarginRate? {
        if (config.disableFeeCalculation) return null
        if (instrument.marginRate == null) {
            runBlocking { prepareFeeCalculation(instrument.code, false) }
        }
        return instrument.marginRate
    }

    /**
     * 查询单一合约的手续费率及保证金率，查询到的结果会自动更新到对应的 [instruments] 中，已经查过的不会再次查询
     */
    private suspend fun prepareFeeCalculation(code: String, throwException: Boolean = true) {
        val instrument = instruments[code] ?: return
        fun handleException(e: Exception, msg: String) {
            if (throwException){
                throw e
            } else {
                postBrokerLogEvent(LogLevel.ERROR, msg)
            }
        }
        when (instrument.type) {
            SecurityType.FUTURES -> {
                if (instrument.commissionRate == null) {
                    postBrokerLogEvent(LogLevel.INFO, "自动查询期货手续费率：$code")
                    runWithRetry({ queryFuturesCommissionRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.prepareFeeCalculation】查询期货手续费率出错：$code, $e")
                    }
                }
                if (instrument.marginRate == null) {
                    postBrokerLogEvent(LogLevel.INFO, "自动查询期货保证金率：$code")
                    runWithRetry({ queryFuturesMarginRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.prepareFeeCalculation】查询期货保证金率出错：$code, $e")
                    }
                }
            }
            SecurityType.OPTIONS -> {
                if (instrument.commissionRate == null) {
                    postBrokerLogEvent(LogLevel.INFO, "自动查询期权手续费率：$code")
                    runWithRetry({ queryOptionsCommissionRate(code) }) { e ->
                        handleException(e, "查询期权手续费率出错：$code, $e")
                    }
                }
                if (instrument.marginRate == null) {
                    postBrokerLogEvent(LogLevel.INFO, "自动查询期权保证金率：$code")
                    runWithRetry({ queryOptionsMargin(code) }) { e ->
                        handleException(e, "查询期保证金出错：$code, $e")
                    }
                }
            }
            else -> Unit
        }
    }

    /**
     * 查询手续费率及保证金率，只有查过费率的合约，其返回的 [Trade], [Order], [Position] 才会带有手续费、保证金等相关信息。
     * 查询到的结果会自动更新到对应的 [instruments] 中，已经查过的不会再次查询
     */
    suspend fun prepareFeeCalculation(codes: Collection<String>? = null, extras: Map<String, String>? = null) {
        if (codes == null) {
            runWithRetry({ queryFuturesCommissionRate() })
            runWithRetry({ queryFuturesMarginRate() })
            runWithRetry({ queryOptionsCommissionRate() })
            runWithRetry({ queryOptionsMargin() })
        } else {
            codes.forEach {
                prepareFeeCalculation(it)
            }
        }
    }

    /**
     * 获取缓存的最新/旧的 [Tick]，如果都没有，那么试图查询最新的并缓存
     * @return [Pair.first] 为查询到的 [Tick]，可能为 null。[Pair.second] 为 [Boolean]，表示查询到的 Tick 是否是实时最新的
     */
    private fun getOrQueryTick(code: String): Pair<Tick?, Boolean> {
        var tick: Tick? = null
        var isLatestTick = false // 查询到的 tick 是否是最新的
        // 如果行情已连接且未禁止自动订阅，则优先尝试获取行情缓存的最新 tick
        if (mdApi.connected && !config.disableAutoSubscribe) {
            tick = mdApi.lastTicks[code]
            // 如果缓存的 tick 为空，说明未订阅该合约，那么订阅该合约以方便后续计算
            if (tick == null) {
                try {
                    postBrokerLogEvent(LogLevel.INFO, "自动订阅行情：$code")
                    runBlocking { mdApi.subscribeMarketData(listOf(code)) }
                } catch (e: Exception) {
                    postBrokerLogEvent(LogLevel.ERROR, "【CtpTdApi.getOrQueryTick】计算保证金时自动订阅合约行情失败：$code, $e")
                }
            } else {
                isLatestTick = true
            }
        }
        // 如果未从行情 API 中获得最新 tick，尝试从本地缓存中获取旧的 tick
        if (tick == null) {
            tick = cachedTickMap[code]
            // 如果未从本地缓存中获得旧的 tick，查询最新 tick（查询操作会自动缓存 tick 至本地缓存中）
            if (tick == null) {
                runBlocking {
                    postBrokerLogEvent(LogLevel.INFO, "自动查询并缓存最新 Tick：$code")
                    tick = runWithRetry({ queryLastTick(code, useCache = false) }) { e->
                        postBrokerLogEvent(LogLevel.ERROR, "【CtpTdApi.getOrQueryTick】查询合约最新 Tick 失败：$code, $e")
                        null
                    }
                    if (tick != null)isLatestTick = true
                }
            }
        }
        return Pair(tick, isLatestTick)
    }

    /**
     * 计算期货保证金
     */
    private fun calculateFuturesMargin(instrument: SecurityInfo, direction: Direction, yesterdayVolume: Int, todayVolume: Int, avgOpenPrice: Double, fallback: Double): Double {
        if (yesterdayVolume + todayVolume == 0) return 0.0
        val marginRate = getOrQueryMarginRate(instrument) ?: return fallback
        val (tick, isLatestTick) = getOrQueryTick(instrument.code)
        if (tick == null) {
            return fallback
        } else {
            fun calculateMargin(volume: Int, price: Double): Double {
                return when (direction) {
                    Direction.LONG -> volume * marginRate.longMarginRatioByVolume + volume * instrument.volumeMultiple * price * marginRate.longMarginRatioByMoney
                    Direction.SHORT -> volume * marginRate.shortMarginRatioByVolume + volume * instrument.volumeMultiple * price * marginRate.shortMarginRatioByMoney
                    else -> 0.0
                }
            }
            var settlementVolume = yesterdayVolume  // 用昨结算价计算保证金的持仓
            var todayMargin = 0.0  // 与现价相关的保证金
            // 如果存在今仓
            if (todayVolume > 0) {
                if (futuresMarginPriceType == MarginPriceType.OPEN_PRICE) {
                    todayMargin = calculateMargin(todayVolume, avgOpenPrice)
                } else {
                    if (isLatestTick) {
                        // 如果保证金价格类型与现价有关，那么特别计算今仓保证金
                        when (futuresMarginPriceType) {
                            MarginPriceType.TODAY_SETTLEMENT_PRICE -> todayMargin = calculateMargin(todayVolume, tick.todayAvgPrice)
                            MarginPriceType.LAST_PRICE -> todayMargin = calculateMargin(todayVolume, tick.lastPrice)
                            else -> settlementVolume += todayVolume
                        }
                    } else {
                        settlementVolume += todayVolume
                    }
                }
            }
            val settlementMargin = calculateMargin(settlementVolume,  tick.preSettlementPrice)
            return todayMargin + settlementMargin
        }
    }

    /**
     * 计算期权保证金
     */
    private fun calculateOptionsMargin(instrument: SecurityInfo, direction: Direction, volume: Int, avgOpenPrice: Double, fallback: Double, isOpen: Boolean): Double {
        if (volume == 0) return 0.0
        when (direction) {
            Direction.LONG -> {  // 买方
                return if (isOpen) avgOpenPrice * instrument.volumeMultiple else 0.0
            }
            Direction.SHORT -> {  // 卖方
                val marginRate = getOrQueryMarginRate(instrument) ?: return fallback
                fun calculateMargin(price: Double): Double {
                    return volume * (price * instrument.volumeMultiple + max(marginRate.longMarginRatioByMoney, marginRate.shortMarginRatioByMoney))
                }
                if (optionsMarginPriceType == MarginPriceType.OPEN_PRICE && !isOpen) {  // 唯一一种不需查询 Tick 的情况
                    return calculateMargin(avgOpenPrice)
                }
                val (tick, isLatestTick) = getOrQueryTick(instrument.code)
                return if (tick == null) {
                    fallback
                } else {
                    val price = if (isLatestTick) {
                        when (optionsMarginPriceType) {
                            MarginPriceType.MAX_PRE_SETTLEMENT_PRICE_LAST_PRICE -> max(tick.lastPrice, tick.preSettlementPrice)
                            MarginPriceType.LAST_PRICE -> tick.lastPrice
                            else -> tick.preSettlementPrice
                        }
                    } else {
                        tick.preSettlementPrice
                    }
                    calculateMargin(price)
                }
            }
            else -> return fallback
        }
    }

    /**
     * 计算 Position 的 value, avgOpenPrice, lastPrice, pnl
     * @param calculateValue 是否计算保证金，默认为 true
     */
    fun calculatePosition(position: Position, calculateValue: Boolean = true, extras: Map<String, String>? = null) {
        val instrument = instruments[position.code] ?: return
        if (instrument.type == SecurityType.FUTURES || instrument.type == SecurityType.OPTIONS) {
            // 计算开仓均价
            if (position.volume != 0 && instrument.volumeMultiple != 0) {
                position.avgOpenPrice = position.openCost / position.volume / instrument.volumeMultiple
            }
            // 获取最新价并计算 pnl
            val lastTick =  mdApi.lastTicks[position.code]
            if (lastTick != null && position.volume > 0) {
                position.lastPrice = lastTick.lastPrice
                position.pnl = position.lastPrice * instrument.volumeMultiple * position.volume - position.openCost
                if (position.direction == Direction.SHORT) {
                    position.pnl *= -1
                }
            }
            // 计算保证金
            if (calculateValue) {
                when (instrument.type) {
                    SecurityType.FUTURES -> {
                        // 这里传入的开仓成本是全体开仓成本，而不是今仓开仓成本，这导致在保证金价格类型为 OPEN_PRICE 时的保证金计算会不准确
                        position.value = calculateFuturesMargin(instrument, position.direction, position.yesterdayVolume, position.todayVolume, position.avgOpenPrice, position.value)
                    }
                    SecurityType.OPTIONS -> {
                        position.value = calculateOptionsMargin(instrument, position.direction, position.volume, position.avgOpenPrice, position.value, false)
                    }
                    else -> Unit
                }
            }
        }
    }

    /**
     * 计算 Order 的 avgFillPrice, frozenCash, 申报手续费（仅限中金所股指期货）。turnover 由 OnRtnTrade 中的 Trade.turnover 累加得到。
     */
    fun calculateOrder(order: Order, extras: Map<String, String>? = null) {
        val instrument = instruments[order.code] ?: return
        if (instrument.type == SecurityType.FUTURES || instrument.type == SecurityType.OPTIONS) {
            // 计算成交均价
            if (order.filledVolume != 0 && instrument.volumeMultiple != 0) {
                order.avgFillPrice = order.turnover / order.filledVolume / instrument.volumeMultiple
            }
            // 如果是开仓，计算冻结资金
            val restVolume = order.volume - order.filledVolume
            if (order.offset == OrderOffset.OPEN && restVolume > 0) {
                when (instrument.type) {
                    SecurityType.FUTURES -> order.frozenCash = calculateFuturesMargin(instrument, order.direction, 0, restVolume, order.price, 0.0)
                    SecurityType.OPTIONS -> order.frozenCash = calculateOptionsMargin(instrument, order.direction, restVolume, order.price, 0.0, true)
                    else -> return
                }
            }
            // 如果是中金所股指期货，计算申报手续费
            if (order.code.startsWith(ExchangeID.CFFEX) && instrument.type == SecurityType.FUTURES) {
                val com = getOrQueryCommissionRate(instrument)
                if (com != null) {
                    when (order.status) {
                        OrderStatus.ACCEPTED,
                        OrderStatus.PARTIALLY_FILLED,
                        OrderStatus.FILLED -> {
                            if (!order.insertFeeCalculated) {
                                order.commission += com.orderInsertFeeByTrade + com.orderInsertFeeByVolume * order.volume
                                order.insertFeeCalculated = true
                            }
                        }
                        OrderStatus.CANCELED -> {
                            if (!order.cancelFeeCalculated) {
                                order.commission += com.orderCancelFeeByTrade + com.orderCancelFeeByVolume * order.volume
                                order.cancelFeeCalculated = true
                            }
                        }
                        else -> Unit
                    }
                }
            }
        }
    }

    /**
     * 计算 Trade 的 turnover 与 commission，如果是平仓会依据 [Trade.offset] 判断是否按平今计算手续费
     */
    fun calculateTrade(trade: Trade, extras: Map<String, String>? = null) {
        val instrument = instruments[trade.code] ?: return
        when (instrument.type) {
            SecurityType.FUTURES,
            SecurityType.OPTIONS, -> {
                if (trade.turnover == 0.0) {
                    trade.turnover = trade.volume * trade.price * instrument.volumeMultiple
                }
                if (trade.commission == 0.0) {
                    val com = getOrQueryCommissionRate(instrument)
                    if (com != null) {
                        when (trade.offset) {
                            OrderOffset.OPEN -> {
                                trade.commission = trade.turnover * com.openRatioByMoney + trade.volume * com.openRatioByVolume
                            }
                            OrderOffset.CLOSE,
                            OrderOffset.CLOSE_YESTERDAY -> {
                                trade.commission = trade.turnover * com.closeRatioByMoney + trade.volume * com.closeRatioByVolume
                            }
                            OrderOffset.CLOSE_TODAY -> {
                                trade.commission = trade.turnover * com.closeTodayRatioByMoney + trade.volume * com.closeTodayRatioByVolume
                            }
                            else -> postBrokerLogEvent(LogLevel.WARNING, "【CtpTdApi.calculateTrade】订单开平仓类型未知（${trade.code}, ${trade.offset}）")
                        }
                    }
                }
            }
            else -> return
        }
    }

    /**
     * Ctp TdApi 的回调类
     */
    private inner class CtpTdSpi : CThostFtdcTraderSpi() {

        /**
         * 请求客户端认证
         */
        private suspend fun reqAuthenticate() {
            val reqField = CThostFtdcReqAuthenticateField().apply {
                appID = config.appId
                authCode = config.authCode
                userProductInfo = config.userProductInfo
                userID = config.investorId
                brokerID = config.brokerId
            }
            val requestId = nextRequestId()
            runWithResultCheck<Unit>({ tdApi.ReqAuthenticate(reqField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        }

        /**
         * 请求用户登录
         */
        private suspend fun reqUserLogin() {
            val reqField = CThostFtdcReqUserLoginField().apply {
                userID = config.investorId
                password = config.password
                brokerID = config.brokerId
                userProductInfo = config.userProductInfo
            }
            val requestId = nextRequestId()
            runWithResultCheck<Unit>({ tdApi.ReqUserLogin(reqField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        }

        /**
         * 请求结算单确认
         */
        private suspend fun reqSettlementInfoConfirm() {
            val reqField = CThostFtdcSettlementInfoConfirmField().apply {
                investorID = config.investorId
                brokerID = config.brokerId
            }
            val requestId = nextRequestId()
            runWithResultCheck<Unit>({ tdApi.ReqSettlementInfoConfirm(reqField, requestId) }, {
                suspendCoroutineWithTimeout(TIMEOUT_MILLS) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        }

        /**
         * 当合约交易状态变化时回调。会更新 [productStatusMap]
         * 这是唯一的公有流订阅的信息，注意该回调信息实测存在延迟，可以多达 1 秒
         */
        override fun OnRtnInstrumentStatus(pInstrumentStatus: CThostFtdcInstrumentStatusField) {
            val marketStatus = when (pInstrumentStatus.instrumentStatus) {
                THOST_FTDC_IS_AuctionOrdering -> MarketStatus.AUCTION_ORDERING
                THOST_FTDC_IS_AuctionMatch -> MarketStatus.AUCTION_MATCHED
                THOST_FTDC_IS_NoTrading,
                THOST_FTDC_IS_BeforeTrading -> MarketStatus.STOP_TRADING
                THOST_FTDC_IS_Continous -> MarketStatus.CONTINUOUS_MATCHING
                THOST_FTDC_IS_Closed -> MarketStatus.CLOSED
                else -> MarketStatus.UNKNOWN
            }
            productStatusMap[pInstrumentStatus.instrumentID] = marketStatus
            if (connected) {  // 过滤当日重放的数据
                val ticks = mdApi.lastTicks.values.filter { codeProductMap[it.code] == pInstrumentStatus.instrumentID }
                if (ticks.isNotEmpty()) {
                    try {
                        val enterTime = LocalTime.parse(pInstrumentStatus.enterTime).atDate(LocalDate.now())
                        ticks.forEach { postBrokerEvent(BrokerEventType.TICK, it.copy(status = marketStatus, time = enterTime, volume = 0, turnover = 0.0, openInterestDelta = 0)) }
                    } catch (e: Exception) {
                        postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnInstrumentStatus】解析 enterTime 失败：$e")
                    }
                }
            }
        }

        /**
         * 发生错误时回调。如果没有对应的协程请求，会发送一条 [BrokerEventType.LOG] 信息；有对应的协程请求时，会将其异常完成
         */
        override fun OnRspError(pRspInfo: CThostFtdcRspInfoField, nRequestID: Int, bIsLast: Boolean) {
            val request = requestMap[nRequestID]
            if (request == null) {
                val errorInfo = "${pRspInfo.errorMsg}, requestId=$nRequestID, isLast=$bIsLast"
                val connectRequests = requestMap.values.filter { it.tag == "connect" }
                if (connectRequests.isEmpty()) {
                    postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspError】$errorInfo")
                } else {
                    resumeRequestsWithException("connect", errorInfo)
                }
            } else {
                request.continuation.resumeWithException(Exception(pRspInfo.errorMsg))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 行情前置连接时回调。会将 [requestId] 置为 0；发送一条 [BrokerEventType.CONNECTION] 信息
         */
        override fun OnFrontConnected() {
            frontConnected = true
            requestId.set(0)
            postBrokerConnectionEvent(ConnectionEventType.TD_NET_CONNECTED)
            scope.launch {
                fun resumeConnectWithException(errorInfo: String) {
                    resumeRequestsWithException("connect", errorInfo)
                    throw Exception(errorInfo)
                }
                try {
                    // 请求客户端认证
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】客户端认证...")
                    try {
                        reqAuthenticate()
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】客户端认证成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("请求客户端认证失败：$e")
                    }
                    // 请求用户登录
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】资金账户登录...")
                    try {
                        reqUserLogin()
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】资金账户登录成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("请求用户登录失败：$e")
                    }
                    // 请求结算单确认
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】结算单确认...")
                    try {
                        reqSettlementInfoConfirm()
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】结算单确认成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("请求结算单确认失败：$e")
                    }
                    // 查询全市场合约
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询全市场合约...")
                    runWithRetry({
                        val allInstruments = queryAllInstruments(false, null)
                        allInstruments.forEach {
                            instruments[it.code] = it
                            codeProductMap[it.code] = it.productId
                            mdApi.codeMap[it.code.split('.', limit = 2)[1]] = it.code
                        }
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询全市场合约成功")
                    }) { e ->
                        resumeConnectWithException("查询全市场合约失败：$e")
                    }
                    // 查询保证金价格类型、持仓合约的保证金率及手续费率（如果未禁止费用计算）
                    if (!config.disableFeeCalculation) {
                        // 查询保证金价格类型
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询保证金价格类型...")
                        runWithRetry({
                            queryMarginPriceType()
                            postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询保证金价格类型成功")
                        }) { e ->
                            resumeConnectWithException("查询保证金价格类型失败：$e")
                        }
                        // 查询持仓合约的手续费率及保证金率
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询持仓合约手续费率及保证金率...")
                        try {
                            prepareFeeCalculation()
                            postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询持仓合约手续费率及保证金率成功")
                        } catch (e: Exception) {
                            resumeConnectWithException("查询持仓合约手续费率及保证金率失败：$e")
                        }
                    }
                    // 查询账户持仓
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询账户持仓...")
                    runWithRetry({
                        queryPositions(useCache = false)
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询账户持仓成功")
                    }) { e ->
                        resumeConnectWithException("查询账户持仓失败：$e")
                    }
                    // 订阅持仓合约行情（如果行情可用且未禁止自动订阅）
                    if (mdApi.connected && !config.disableAutoSubscribe) {
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】订阅持仓合约行情...")
                        try {
                            mdApi.subscribeMarketData(positions.keys)
                            postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】订阅持仓合约行情成功")
                        } catch (e: Exception) {
                            resumeConnectWithException("订阅持仓合约行情失败：$e")
                        }
                    }
                    // 查询当日订单
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日订单...")
                    runWithRetry({
                        val orders = queryOrders(onlyUnfinished = false, useCache = false)
                        val finishedStatus = setOf(OrderStatus.CANCELED, OrderStatus.FILLED, OrderStatus.ERROR)
                        orders.forEach {
                            todayOrders[it.orderId] = it
                            if (it.status !in finishedStatus) {
                                when (it.direction) {
                                    Direction.LONG -> unfinishedLongOrders.insert(it)
                                    Direction.SHORT -> unfinishedShortOrders.insert(it)
                                    else -> postBrokerLogEvent(LogLevel.WARNING, "【交易接口登录】查询到未知方向的订单（${it.code}, ${it.direction}）")
                                }
                            }
                            if (it.status == OrderStatus.CANCELED) {
                                cancelStatistics[it.code] = cancelStatistics.getOrDefault(it.code, 0) + 1
                            }
                        }
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日订单成功")
                    }) { e ->
                        resumeConnectWithException("查询当日订单失败：$e")
                    }
                    // 查询当日成交记录
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日成交记录...")
                    runWithRetry({
                        val trades = queryTrades(useCache = false)
                        todayTrades.addAll(trades)
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日成交记录成功")
                    }) { e ->
                        resumeConnectWithException("查询当日成交记录失败：$e")
                    }
                    // 登录操作完成
                    connected = true
                    postBrokerConnectionEvent(ConnectionEventType.TD_LOGGED_IN)
                    resumeRequests("connect", Unit)
                } catch (e: Exception) {  // 登录操作失败
                    resumeRequestsWithException("connect", e.message ?: e.toString())
                }
            }
        }

        /**
         * 交易前置断开连接时回调。会将 [connected] 置为 false；发送一条 [BrokerEventType.CONNECTION] 信息；异常完成所有的协程请求
         */
        override fun OnFrontDisconnected(nReason: Int) {
            frontConnected = false
            connected = false
            postBrokerConnectionEvent(ConnectionEventType.TD_NET_DISCONNECTED, "${getDisconnectReason(nReason)} ($nReason)")
            val e = Exception("网络连接断开：${getDisconnectReason(nReason)} ($nReason)")
            requestMap.values.forEach {
                it.continuation.resumeWithException(e)
            }
            requestMap.clear()
        }

        /**
         * 客户端认证请求响应
         */
        override fun OnRspAuthenticate(
            pRspAuthenticateField: CThostFtdcRspAuthenticateField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 用户登录请求响应
         */
        override fun OnRspUserLogin(
            pRspUserLogin: CThostFtdcRspUserLoginField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (pRspUserLogin == null) {
                    request.continuation.resumeWithException(Exception("pRspUserLogin 为 null"))
                    requestMap.remove(nRequestID)
                    return
                }
                frontId = pRspUserLogin.frontID
                sessionId = pRspUserLogin.sessionID
                tradingDay = pRspUserLogin.tradingDay
                var lastTradingDay = ""
                var lastMaxOrderRef = 10000
                if (cacheFile.exists()) {
                    val lines = cacheFile.readLines()
                    if (lines.size >= 2) {
                        lastTradingDay = lines[0]
                        lastMaxOrderRef = lines[1].toIntOrNull() ?: lastMaxOrderRef
                    }
                }
                // 如果交易日未变，则延续使用上一次的 maxOrderRef
                if (lastTradingDay == tradingDay) {
                    orderRef.set(lastMaxOrderRef)
                } else { // 如果交易日变动，则清空各种缓存，并将 orderRef 重置为 10000
                    orderRef.set(10000)
                    todayOrders.clear()
                    todayTrades.clear()
                    unfinishedLongOrders.clear()
                    unfinishedShortOrders.clear()
                    cancelStatistics.clear()
                    instruments.clear()
                    codeProductMap.clear()
                    cachedTickMap.clear()
                    mdApi.codeMap.clear()
                    positions.clear()
                    cacheFile.writeText("$tradingDay\n${orderRef.get()}")
                    postBrokerEvent(BrokerEventType.NEW_TRADING_DAY, Converter.dateC2A(tradingDay))
                }
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 结算单确认请求响应
         */
        override fun OnRspSettlementInfoConfirm(
            pSettlementInfoConfirm: CThostFtdcSettlementInfoConfirmField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 报单请求响应，仅本 session 发出的报单请求发生错误时会触发此回调
         */
        override fun OnRspOrderInsert(
            pInputOrder: CThostFtdcInputOrderField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            checkRspInfo(pRspInfo, {}, { errorCode, errorMsg ->
                if (pInputOrder == null) return
                val order = todayOrders[pInputOrder.orderRef] ?: return
                val orderId = "${frontId}_${sessionId}_${pInputOrder.orderRef}"
                if (orderId != order.orderId) return
                order.apply {
                    status = OrderStatus.ERROR
                    statusMsg = "$errorMsg ($errorCode)"
                    updateTime = LocalDateTime.now()
                }
                postBrokerEvent(BrokerEventType.ORDER_STATUS, order.deepCopy())
            })
        }

        /**
         * 撤单请求响应，仅本 session 发出的撤单请求发生错误时会触发此回调
         */
        override fun OnRspOrderAction(
            pInputOrderAction: CThostFtdcInputOrderActionField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            checkRspInfo(pRspInfo, {}, { errorCode, errorMsg ->
                if (pInputOrderAction == null) return
                val orderId = "${pInputOrderAction.frontID}_${pInputOrderAction.sessionID}_${pInputOrderAction.orderRef}"
                val order = todayOrders[pInputOrderAction.orderRef] ?: todayOrders[orderId] ?: return
                if (orderId != order.orderId) return
                order.apply {
                    statusMsg = "$errorMsg ($errorCode)"
                    updateTime = LocalDateTime.now()
                }
                postBrokerEvent(BrokerEventType.CANCEL_FAILED, order.deepCopy())
            })
        }

        /**
         * 订单状态更新回调。该回调会收到该账户下所有 session 的订单回报，因此需要与本 session 的订单区分处理
         */
        override fun OnRtnOrder(pOrder: CThostFtdcOrderField) {
            val orderId = "${pOrder.frontID}_${pOrder.sessionID}_${pOrder.orderRef}"
            var order = todayOrders[pOrder.orderRef]
            // 如果不是本 session 发出的订单，找到或创建缓存的订单
            if (order == null || orderId != order.orderId) {
                // 首先检查是否已缓存过
                order = todayOrders[orderId]
                // 如果是第一次接收回报，则创建并缓存该订单，之后局部变量 order 不为 null
                if (order == null) {
                    val code = "${pOrder.exchangeID}.${pOrder.instrumentID}"
                    order = Converter.orderC2A(pOrder, instruments[code]?.volumeMultiple ?: 0) { e ->
                        postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnOrder】Order time 解析失败：${orderId}, $code, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
                    }
                    calculateOrder(order)
                    todayOrders[orderId] = order
                    when (order.status) {
                        OrderStatus.SUBMITTING,
                        OrderStatus.ACCEPTED,
                        OrderStatus.PARTIALLY_FILLED -> insertUnfinishedOrder(order)
                        else -> Unit
                    }
                }
            }
            // 更新 orderSysId
            if (pOrder.orderSysID.isNotEmpty()) {
                order.orderSysId = "${pOrder.exchangeID}_${pOrder.orderSysID}"
            }
            val oldStatus = order.status
            val oldCommission = order.commission
            // 判定订单目前的状态
            val newOrderStatus = when (pOrder.orderSubmitStatus) {
                THOST_FTDC_OSS_InsertRejected -> {
                    removeUnfinishedOrder(order)
                    OrderStatus.ERROR
                }
                THOST_FTDC_OSS_CancelRejected,
                THOST_FTDC_OSS_ModifyRejected -> {
                    order.status
                }
                else -> when (pOrder.orderStatus) {
                    THOST_FTDC_OST_Unknown -> OrderStatus.SUBMITTING
                    THOST_FTDC_OST_NoTradeQueueing -> {
                        // 计算报单费用
                        if (pOrder.exchangeID == ExchangeID.CFFEX) calculateOrder(order)
                        OrderStatus.ACCEPTED
                    }
                    THOST_FTDC_OST_PartTradedQueueing -> OrderStatus.PARTIALLY_FILLED
                    THOST_FTDC_OST_AllTraded -> {
                        removeUnfinishedOrder(order)
                        OrderStatus.FILLED
                    }
                    THOST_FTDC_OST_Canceled -> {
                        removeUnfinishedOrder(order)
                        cancelStatistics[order.code] = cancelStatistics.getOrDefault(order.code, 0) + 1
                        // 计算撤单费用
                        if (pOrder.exchangeID == ExchangeID.CFFEX) calculateOrder(order)
                        OrderStatus.CANCELED
                    }
                    else -> {
                        removeUnfinishedOrder(order)
                        OrderStatus.ERROR
                    }
                }
            }
            order.apply {
                status = newOrderStatus
                statusMsg = pOrder.statusMsg
            }
            // 如果有申报手续费，加到 position 的手续费统计中
            if (oldCommission != order.commission) {
                val position = queryCachedPosition(order.code, order.direction, order.offset != OrderOffset.OPEN)
                position?.apply {
                    todayCommission += order.commission - oldCommission
                }
            }
            if (newOrderStatus == OrderStatus.ERROR) {
                order.updateTime = LocalDateTime.now()
                postBrokerEvent(BrokerEventType.ORDER_STATUS, order.deepCopy())
            } else {
                // 仅发送与成交不相关的订单状态更新回报，成交相关的订单状态更新回报会在 OnRtnTrade 中发出，以确保成交回报先于状态回报
                if (newOrderStatus != oldStatus && newOrderStatus != OrderStatus.PARTIALLY_FILLED && newOrderStatus != OrderStatus.FILLED) {
                    // 如果是平仓，更新仓位冻结及剩余可平信息
                    if (order.offset != OrderOffset.OPEN) {
                        val position = queryCachedPosition(order.code, order.direction, true)
                        if (position != null) {
                            when (newOrderStatus) {
                                OrderStatus.ACCEPTED -> {
                                    position.frozenVolume += order.volume
                                }
                                OrderStatus.CANCELED,
                                OrderStatus.ERROR -> {
                                    val restVolume = order.volume - pOrder.volumeTraded
                                    position.frozenVolume -= restVolume
                                }
                                else -> Unit
                            }
                        }
                    }
                    val updateTime = try {
                        if (newOrderStatus == OrderStatus.CANCELED) {
                            LocalTime.parse(pOrder.cancelTime).atDate(LocalDate.now())
                        } else {
                            val date = pOrder.insertDate
                            LocalDateTime.parse("${date.slice(0..3)}-${date.slice(4..5)}-${date.slice(6..7)}T${pOrder.insertTime}")
                        }
                    } catch (e: Exception) {
                        postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnOrder】Order updateTime 解析失败：${order.orderId}, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
                        LocalDateTime.now()
                    }
                    order.updateTime = updateTime
                    postBrokerEvent(BrokerEventType.ORDER_STATUS, order.deepCopy())
                }
            }
        }

        /**
         * 成交回报回调。该回调会收到该账户下所有 session 的成交回报，因此需要与本 session 的成交回报区分处理
         */
        override fun OnRtnTrade(pTrade: CThostFtdcTradeField) {
            var order = todayOrders[pTrade.orderRef]
            val orderSysId = "${pTrade.exchangeID}_${pTrade.orderSysID}"
            if (order == null || order.orderSysId != orderSysId) {
                order = todayOrders.values.find { it.orderSysId == orderSysId }
                if (order == null) {
                    postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRtnTrade】收到未知订单的成交回报：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}")
                }
            }
            val trade = Converter.tradeC2A(pTrade, order?.orderId ?: orderSysId) { e ->
                postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnTrade】Trade tradeTime 解析失败：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}, ${pTrade.tradeDate}T${pTrade.tradeTime}, $e")
            }
            val instrument = instruments[trade.code] ?: return
            trade.turnover = trade.volume * trade.price * instrument.volumeMultiple
            // 更新仓位信息，并判断是平今还是平昨
            val biPosition = positions.getOrPut(trade.code) { BiPosition() }
            val position: Position? = when {
                trade.direction == Direction.LONG && trade.offset == OrderOffset.OPEN ||
                        trade.direction == Direction.SHORT && trade.offset != OrderOffset.OPEN -> {
                    if (biPosition.long == null) {
                        biPosition.long = Position(
                            config.investorId, tradingDate,
                            trade.code, Direction.LONG, 0, 0, 0, 0, 0, 0,
                            0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                        )
                    }
                    biPosition.long
                }
                trade.direction == Direction.SHORT && trade.offset == OrderOffset.OPEN ||
                        trade.direction == Direction.LONG && trade.offset != OrderOffset.OPEN -> {
                    if (biPosition.short == null) {
                        biPosition.short = Position(
                            config.investorId, tradingDate,
                            trade.code, Direction.SHORT, 0, 0, 0,  0, 0, 0,
                            0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                        )
                    }
                    biPosition.short
                }
                else -> null
            }
            if (position != null) {
                // 如果是开仓，那么很简单，直接更新仓位
                if (trade.offset == OrderOffset.OPEN) {
                    position.todayVolume += trade.volume
                    position.volume += trade.volume
                    position.todayOpenVolume += trade.volume
                    position.openCost += trade.turnover
                } else { // 如果不是开仓，则判断是平今还是平昨，上期所按 order 指令，其它三所涉及平今手续费减免时优先平今，否则优先平昨
                    var todayClosed = 0
                    var yesterdayClosed = 0
                    val com = getOrQueryCommissionRate(instrument)
                    when (pTrade.exchangeID) {
                        ExchangeID.SHFE, ExchangeID.INE -> {
                            trade.offset = order?.offset ?: trade.offset
                            if (trade.offset == OrderOffset.CLOSE) {
                                trade.offset = OrderOffset.CLOSE_YESTERDAY
                            }
                            when (trade.offset) {
                                OrderOffset.CLOSE_TODAY -> todayClosed = trade.volume
                                OrderOffset.CLOSE_YESTERDAY -> yesterdayClosed = trade.volume
                                else -> Unit
                            }
                        }
                        else -> {
                            // 依据手续费率判断是否优先平今
                            var todayFirst = false
                            if (com != null && (com.closeTodayRatioByVolume < com.closeRatioByVolume || com.closeTodayRatioByMoney < com.closeRatioByMoney)) {
                                todayFirst = true
                            }
                            // 依据仓位及是否优先平今判断是否实际平今
                            if (todayFirst) {
                                when {
                                    // 全部平今
                                    trade.volume <= position.todayVolume -> {
                                        trade.offset = OrderOffset.CLOSE_TODAY
                                        todayClosed = trade.volume
                                    }
                                    // 全部平昨
                                    position.todayVolume == 0 && position.yesterdayVolume >= trade.volume -> {
                                        trade.offset = OrderOffset.CLOSE_YESTERDAY
                                        yesterdayClosed = trade.volume
                                    }
                                    // 部分平今部分平昨
                                    trade.volume <= position.volume -> {
                                        trade.offset = OrderOffset.CLOSE
                                        todayClosed = position.todayVolume
                                        yesterdayClosed = trade.volume - position.todayVolume
                                    }
                                }
                            } else {
                                when {
                                    // 全部平昨
                                    trade.volume <= position.yesterdayVolume -> {
                                        trade.offset = OrderOffset.CLOSE_YESTERDAY
                                        yesterdayClosed = trade.volume
                                    }
                                    // 全部平今
                                    position.yesterdayVolume == 0 && trade.volume <= position.todayVolume -> {
                                        trade.offset = OrderOffset.CLOSE_TODAY
                                        todayClosed = trade.volume
                                    }
                                    // 部分平今部分平昨
                                    trade.volume <= position.volume -> {
                                        trade.offset = OrderOffset.CLOSE
                                        yesterdayClosed = position.yesterdayVolume
                                        todayClosed = trade.volume - yesterdayClosed
                                    }
                                }
                            }
                        }
                    }
                    val totalClosed = todayClosed + yesterdayClosed
                    position.volume -= totalClosed
                    position.todayVolume -= todayClosed
                    position.todayCloseVolume += totalClosed
                    position.frozenVolume -= totalClosed
                    // 由于未知持仓明细，因此此处只按开仓均价减去对应开仓成本，保持开仓均价不变，为此查询持仓明细太麻烦了
                    position.openCost -= position.avgOpenPrice * totalClosed * instrument.volumeMultiple
                    // 部分平今部分平昨，则本地计算手续费
                    if (trade.offset == OrderOffset.CLOSE && com !=null) {
                        val todayClosedTurnover = todayClosed * trade.turnover / totalClosed
                        val yesterdayClosedTurnover = yesterdayClosed * trade.turnover / totalClosed
                        trade.commission = yesterdayClosedTurnover * com.closeRatioByMoney + yesterdayClosed * com.closeRatioByVolume +
                                todayClosedTurnover * com.closeTodayRatioByMoney + todayClosed * com.closeTodayRatioByVolume
                    }
                }
            }
            calculateTrade(trade)
            if (position != null) {
                position.todayCommission += trade.commission
            }
            todayTrades.add(trade)
            postBrokerEvent(BrokerEventType.TRADE_REPORT, trade.deepCopy())
            // 更新 order 信息
            if (order != null) {
                order.filledVolume += trade.volume
                order.turnover += trade.turnover
                order.commission += trade.commission
                order.updateTime = trade.time
                order.status = if (order.filledVolume < order.volume) {
                    OrderStatus.PARTIALLY_FILLED
                } else {
                    OrderStatus.FILLED
                }
                calculateOrder(order)
                postBrokerEvent(BrokerEventType.ORDER_STATUS, order.deepCopy())
            }
        }

        /**
         * 订单查询请求响应
         */
        override fun OnRspQryOrder(
            pOrder: CThostFtdcOrderField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                val reqData = request.data as QueryOrdersData
                if (pOrder != null) {
                    val code = "${pOrder.exchangeID}.${pOrder.instrumentID}"
                    val order = Converter.orderC2A(pOrder, instruments[code]?.volumeMultiple ?: 0) { e ->
                        postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryOrder】Order time 解析失败：${"${pOrder.frontID}_${pOrder.sessionID}_${pOrder.orderRef}"}, $code, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
                    }
                    reqData.results.add(order)
                }
                if (bIsLast) {
                    if (reqData.orderId != null) {  // 查询单个订单
                        val order = reqData.results.find { it.orderId == reqData.orderId }
                        (request.continuation as Continuation<Order?>).resume(order)
                        requestMap.remove(nRequestID)
                    } else {  // 查询多个订单
                        if (reqData.code != null) {
                            reqData.results.removeAll { it.code != reqData.code }
                        }
                        if (reqData.onlyUnfinished) {
                            val finishedStatus = setOf(OrderStatus.CANCELED, OrderStatus.FILLED, OrderStatus.ERROR)
                            reqData.results.removeAll { it.status in finishedStatus }
                        }
                        (request.continuation as Continuation<List<Order>>).resume(reqData.results)
                        requestMap.remove(nRequestID)
                    }
                }
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         * 成交记录查询请求响应
         */
        override fun OnRspQryTrade(
            pTrade: CThostFtdcTradeField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                val reqData = request.data as QueryTradesData
                if (pTrade != null) {
                    val orderSysId = "${pTrade.exchangeID}_${pTrade.orderSysID}"
                    var order = todayOrders[pTrade.orderRef]
                    if (order == null || order.orderSysId != orderSysId) {
                        order = todayOrders.values.find { it.orderSysId == orderSysId }
                        if (order == null) {
                            postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRspQryTrade】未找到对应订单：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}")
                        }
                    }
                    val trade = Converter.tradeC2A(pTrade, order?.orderId ?: orderSysId) { e ->
                        postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryTrade】Trade tradeTime 解析失败：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}, ${pTrade.tradeDate}T${pTrade.tradeTime}, $e")
                    }
                    reqData.results.add(trade)
                }
                if (bIsLast) {
                    if (reqData.tradeId != null) {
                        val trade = reqData.results.find { it.tradeId == reqData.tradeId }
                        (request.continuation as Continuation<Trade?>).resume(trade)
                        requestMap.remove(nRequestID)
                    } else {
                        if (reqData.code != null) {
                            reqData.results.removeAll { it.code != reqData.code }
                        }
                        if (reqData.orderSysId != null) {
                            reqData.results.removeAll { it.orderId != reqData.orderSysId }
                        }
                        (request.continuation as Continuation<List<Trade>>).resume(reqData.results)
                        requestMap.remove(nRequestID)
                    }
                }
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         *  Tick 查询请求响应
         */
        override fun OnRspQryDepthMarketData(
            pDepthMarketData: CThostFtdcDepthMarketDataField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            val reqCode = request.data as String
            checkRspInfo(pRspInfo, {
                if (pDepthMarketData == null) {
                    (request.continuation as Continuation<Tick?>).resume(null)
                    requestMap.remove(nRequestID)
                    return
                }
                val code = "${pDepthMarketData.exchangeID}.${pDepthMarketData.instrumentID}"
                if (code == reqCode) {
                    val tick = Converter.tickC2A(code, pDepthMarketData, volumeMultiple = instruments[code]?.volumeMultiple, marketStatus = getInstrumentStatus(code)) { e ->
                        postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryDepthMarketData】Tick updateTime 解析失败：${request.data}, ${pDepthMarketData.updateTime}.${pDepthMarketData.updateMillisec}, $e")
                    }
                    cachedTickMap[code] = tick
                    (request.continuation as Continuation<Tick?>).resume(tick)
                    requestMap.remove(nRequestID)
                } else {
                    if (bIsLast) {
                        (request.continuation as Continuation<Tick?>).resume(null)
                        requestMap.remove(nRequestID)
                    }
                }
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         * 合约信息查询请求响应
         */
        override fun OnRspQryInstrument(
            pInstrument: CThostFtdcInstrumentField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            val reqData = request.data
            val instrument = pInstrument?.let {
                Converter.securityC2A(pInstrument) { e ->
                    postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryInstrument】Instrument 解析失败(${pInstrument.exchangeID}.${pInstrument.instrumentID})：$e")
                }
            }
            checkRspInfo(pRspInfo, {
                // 如果是查询单个合约
                if (reqData is String) {
                    val con = request.continuation as Continuation<SecurityInfo?>
                    if (instrument == null) {
                        con.resume(null)
                        requestMap.remove(nRequestID)
                        return
                    }
                    if (reqData == instrument.code) {
                        con.resume(instrument)
                        requestMap.remove(nRequestID)
                    } else {
                        if (bIsLast) {
                            con.resume(null)
                            requestMap.remove(nRequestID)
                        }
                    }
                } else { // 如果是查询多个合约
                    val insList = request.data as MutableList<SecurityInfo>
                    if (instrument != null) insList.add(instrument)
                    if (bIsLast) {
                        (request.continuation as Continuation<List<SecurityInfo>>).resume(insList)
                        requestMap.remove(nRequestID)
                    }
                }
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         * 账户资金查询请求响应
         */
        override fun OnRspQryTradingAccount(
            pTradingAccount: CThostFtdcTradingAccountField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (pTradingAccount == null) {
                    request.continuation.resumeWithException(Exception("pTradingAccount 为 null"))
                    requestMap.remove(nRequestID)
                    return
                }
                (request.continuation as Continuation<Assets>).resume(Converter.assetsC2A(tradingDate, pTradingAccount))
                requestMap.remove(nRequestID)
                lastQueryAssetsTime = System.currentTimeMillis()
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         * 账户持仓查询请求响应，会自动合并上期所的昨仓和今仓
         */
        override fun OnRspQryInvestorPosition(
            pInvestorPosition: CThostFtdcInvestorPositionField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                val posList = request.data as MutableList<Position>
                if (pInvestorPosition != null) {
                    val direction = Converter.directionC2A(pInvestorPosition.posiDirection)
                    val code = "${pInvestorPosition.exchangeID}.${pInvestorPosition.instrumentID}"
                    var mergePosition: Position? = null
                    if (pInvestorPosition.exchangeID == ExchangeID.SHFE || pInvestorPosition.exchangeID == ExchangeID.INE) {
                        mergePosition = posList.find { it.code == code && it.direction == direction }
                    }
                    if (mergePosition == null) {
                        posList.add(Converter.positionC2A(tradingDate, pInvestorPosition))
                    } else {
                        val frozenVolume =  when (direction) {
                            Direction.LONG -> pInvestorPosition.shortFrozen
                            Direction.SHORT -> pInvestorPosition.longFrozen
                            else -> 0
                        }
                        mergePosition.apply {
                            volume += pInvestorPosition.position
                            this.frozenVolume += frozenVolume
                            todayCloseVolume += pInvestorPosition.closeVolume
                            todayCommission += pInvestorPosition.commission
                            openCost += pInvestorPosition.openCost
                            when (pInvestorPosition.positionDate) {
                                THOST_FTDC_PSD_Today -> {
                                    todayVolume += pInvestorPosition.todayPosition
                                    todayOpenVolume += pInvestorPosition.openVolume
                                }
                                THOST_FTDC_PSD_History -> Unit
                            }
                        }
                    }
                }
                if (bIsLast) {
                    // 不计算保证金，只计算 avgOpenPrice, lastPrice, pnl
                    posList.forEach { calculatePosition(it, false) }
                    // 如果是查询总持仓，更新持仓缓存
                    if (request.tag == "") {
                        positions.clear()
                        posList.forEach {
                            val biPosition = positions.getOrPut(it.code) { BiPosition() }
                            when (it.direction) {
                                Direction.LONG -> biPosition.long = it
                                Direction.SHORT -> biPosition.short = it
                                else -> postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRspQryInvestorPosition】查询到未知的持仓方向（${it.code}, ${it.direction}）")
                            }
                        }
                        (request.continuation as Continuation<List<Position>>).resume(posList)
                        requestMap.remove(nRequestID)
                    } else { // 查询单合约持仓
                        when (request.tag) {
                            Direction.LONG.name -> {
                                (request.continuation as Continuation<Position?>).resume(posList.find { it.direction == Direction.LONG })
                                requestMap.remove(nRequestID)
                            }
                            Direction.SHORT.name -> {
                                (request.continuation as Continuation<Position?>).resume(posList.find { it.direction == Direction.SHORT })
                                requestMap.remove(nRequestID)
                            }
                            else -> {
                                (request.continuation as Continuation<List<Position>>).resume(posList)
                                requestMap.remove(nRequestID)
                            }
                        }
                    }
                }
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         * 持仓明细查询请求响应
         */
        override fun OnRspQryInvestorPositionDetail(
            pInvestorPositionDetail: CThostFtdcInvestorPositionDetailField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                val reqData = request.data as QueryPositionDetailsData
                if (pInvestorPositionDetail != null) {
                    val code = "${pInvestorPositionDetail.exchangeID}.${pInvestorPositionDetail.instrumentID}"
                    val direction = Converter.directionC2A(pInvestorPositionDetail.direction)
                    var details = reqData.results.find { it.code == code && it.direction == direction }
                    if (details == null) {
                        details = PositionDetails(pInvestorPositionDetail.investorID, code, direction)
                        reqData.results.add(details)
                    }
                    val index = details.details.binarySearch { sign(it.price - pInvestorPositionDetail.openPrice).toInt() }
                    val detail = if (index >= 0) {
                        details.details[index]
                    } else {
                        val newDetail = PositionDetail(
                            accountId = pInvestorPositionDetail.investorID,
                            code = code,
                            direction = direction,
                            price = pInvestorPositionDetail.openPrice,
                        )
                        details.details.add(-index - 1, newDetail)
                        newDetail
                    }
                    val openDate = LocalDateTime.of(Converter.dateC2A(pInvestorPositionDetail.openDate), LocalTime.MIDNIGHT)
                    detail.volume += pInvestorPositionDetail.volume
                    if (detail.updateTime.isBefore(openDate)) {
                        detail.updateTime = openDate
                    }
                    if (pInvestorPositionDetail.openDate == tradingDay) {
                        detail.todayVolume += pInvestorPositionDetail.volume
                    }
                }
                if (bIsLast) {
                    if (reqData.code == null || reqData.direction == null) {  // 查询多个
                        (request.continuation as Continuation<List<PositionDetails>>).resume(reqData.results)
                        requestMap.remove(nRequestID)
                    } else {  // 查询单个
                        val details = reqData.results.find { it.code == reqData.code && it.direction == reqData.direction }
                        (request.continuation as Continuation<PositionDetails?>).resume(details)
                        requestMap.remove(nRequestID)
                    }
                }
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         * 保证金价格类型查询请求响应
         */
        override fun OnRspQryBrokerTradingParams(
            pBrokerTradingParams: CThostFtdcBrokerTradingParamsField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (pBrokerTradingParams != null) {
                    futuresMarginPriceType = Converter.marginPriceTypeC2A(pBrokerTradingParams.marginPriceType)
                    optionsMarginPriceType = Converter.marginPriceTypeC2A(pBrokerTradingParams.optionRoyaltyPriceType)
                }
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 期货保证金率查询请求响应，会自动更新 [instruments] 中对应的保证金率信息
         */
        override fun OnRspQryInstrumentMarginRate(
            pMarginRate: CThostFtdcInstrumentMarginRateField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (pMarginRate != null) {
                    // pMarginRate.exchangeID 为空
                    val code = mdApi.codeMap[pMarginRate.instrumentID]
                    if (code != null) {
                        val instrument = instruments[code]
                        if (instrument != null && instrument.marginRate == null) {
                            instrument.marginRate = Converter.futuresMarginRateC2A(pMarginRate, code)
                        }
                    }
                }
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 期货手续费率查询请求响应，会自动更新 [instruments] 中对应的手续费率信息
         */
        override fun OnRspQryInstrumentCommissionRate(
            pCommissionRate: CThostFtdcInstrumentCommissionRateField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (pCommissionRate != null) {
                    // code 可能为具体合约代码（"SHFE.ru2109"），也可能为品种代码（"ru"）。注意 pCommissionRate.exchangeID 为空
                    val code = mdApi.codeMap[pCommissionRate.instrumentID] ?: pCommissionRate.instrumentID
                    val commissionRate = Converter.futuresCommissionRateC2A(pCommissionRate, code)
                    val instrument = instruments[code]
                    var standardCode = ""
                    // 如果是品种代码，更新 instruments 中所有该品种的手续费
                    if (instrument == null) {
                        val instrumentList = instruments.values.filter {
                            if (it.type != SecurityType.FUTURES) return@filter false
                            if (it.commissionRate != null) return@filter false
                            return@filter it.productId == code
                        }
                        if (instrumentList.isNotEmpty()) {
                            instrumentList.forEach { it.commissionRate = commissionRate }
                            standardCode = instrumentList.first().code
                        }
                    } else { // 如果是合约代码，直接更新合约
                        if (instrument.commissionRate == null) {
                            instrument.commissionRate = commissionRate
                            standardCode = instrument.code
                        }
                    }
                    // 如果是中金所期货，那么查询申报手续费
                    if (standardCode.startsWith(ExchangeID.CFFEX)) {
                        val job = scope.launch {
                            runWithRetry({ queryFuturesOrderCommissionRate(standardCode) }) { e ->
                                postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryInstrumentCommissionRate】查询期货申报手续费失败：$standardCode, $e")
                            }
                        }
                        (request.data as MutableList<Job>).add(job)
                    }
                }
                if (bIsLast) {
                    (request.continuation as Continuation<List<Job>>).resume(request.data as MutableList<Job>)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 期货申报手续费率查询，仅限中金所
         */
        override fun OnRspQryInstrumentOrderCommRate(
            pOrderCommRate: CThostFtdcInstrumentOrderCommRateField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                // 目前只有 IC, IH, IF 能查到挂单撤单手续费，并且每个品种会返回 3 条 hedgeFlag 分别为 1, 3, 2 的其余字段相同的记录
                if (pOrderCommRate != null && pOrderCommRate.hedgeFlag == THOST_FTDC_HF_Speculation) {
                    val commissionList = instruments.values.filter {
                        if (it.type != SecurityType.FUTURES) return@filter false
                        if (it.commissionRate == null) return@filter false
                        return@filter it.productId == pOrderCommRate.instrumentID
                    }.map { it.commissionRate!! }
                    commissionList.forEach {
                        it.orderInsertFeeByTrade = pOrderCommRate.orderCommByTrade
                        it.orderInsertFeeByVolume = pOrderCommRate.orderCommByVolume
                        it.orderCancelFeeByTrade = pOrderCommRate.orderActionCommByTrade
                        it.orderCancelFeeByVolume = pOrderCommRate.orderActionCommByVolume
                    }
                }
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 期权手续费率查询请求响应，会自动更新 [instruments] 中对应的手续费率信息
         */
        override fun OnRspQryOptionInstrCommRate(
            pCommissionRate: CThostFtdcOptionInstrCommRateField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (pCommissionRate != null) {
                    // pCommissionRate.exchangeID 为空，pCommissionRate.instrumentID 为期货品种代码 (productId)
                    val commissionRate = Converter.optionsCommissionRateC2A(pCommissionRate)
                    val instrumentList = instruments.values.filter {
                        if (it.type != SecurityType.OPTIONS) return@filter false
                        if (it.commissionRate != null) return@filter false
                        return@filter it.productId == pCommissionRate.instrumentID
                    }
                    instrumentList.forEach { it.commissionRate = commissionRate }
                }
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 期货保证金率查询请求响应，会自动更新 [instruments] 中对应的保证金率信息
         */
        override fun OnRspQryOptionInstrTradeCost(
            pOptionMargin: CThostFtdcOptionInstrTradeCostField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            val request = requestMap[nRequestID] ?: return
            checkRspInfo(pRspInfo, {
                if (pOptionMargin != null) {
                    // pOptionMargin.exchangeID 为空，pOptionMargin.instrumentID 为具体合约代码
                    val code = mdApi.codeMap[pOptionMargin.instrumentID] ?: pOptionMargin.instrumentID
                    val instrument = instruments[code]
                    if (instrument != null && instrument.marginRate == null) {
                        instrument.marginRate = Converter.optionsMarginC2A(pOptionMargin, code)
                    }
                }
                if (bIsLast) {
                    (request.continuation as Continuation<Unit>).resume(Unit)
                    requestMap.remove(nRequestID)
                }
            }) { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            }
        }
    }
}