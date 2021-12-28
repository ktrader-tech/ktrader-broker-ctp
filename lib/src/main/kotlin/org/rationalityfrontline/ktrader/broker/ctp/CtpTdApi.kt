@file:Suppress("UNCHECKED_CAST", "UNUSED_PARAMETER", "MemberVisibilityCanBePrivate")

package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.*
import org.rationalityfrontline.jctp.*
import org.rationalityfrontline.jctp.jctpConstants.*
import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.api.broker.*
import org.rationalityfrontline.ktrader.api.datatype.*
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
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    /**
     * 上次更新的交易日。当 [connected] 处于 false 状态时可能因过期而失效
     */
    private var tradingDay = ""
        set(value) {
            field = value
            tradingDate = Converter.dateC2A(value)
        }
    var tradingDate: LocalDate = LocalDate.now()
        private set
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
    /** 当前交易日已查询过保证金费率的证券代码 */
    private val marginRateQueriedCodes: MutableSet<String> = mutableSetOf()
    /** 当前交易日已查询过手续费率的证券代码 */
    private val commissionRateQueriedCodes: MutableSet<String> = mutableSetOf()
    /** 当前交易日已查询过日级价格信息（涨跌停价、昨收昨结昨仓）的证券代码 */
    private val dayPriceInfoQueriedCodes: MutableSet<String> = mutableSetOf()
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
    private var assets: Assets = Assets(config.investorId)
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
    /**
     * 是否正处于测试 TickToTrade 状态
     */
    var isTestingTickToTrade: Boolean = false

    init {
        val cachePath = config.cachePath.ifBlank { "./data/ctp/" }
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
     * 判断是否存在标签为 [tag] 的未完成的协程请求
     */
    private fun hasRequest(tag: String): Boolean {
        return requestMap.values.find { it.tag == tag } != null
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
        minVolume: Int = 0,
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
        // 用于测试 TickToTrade 的订单插入时间
        var insertTime: Long? = null
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
                        if (minVolume != 0) {
                            volumeCondition = THOST_FTDC_VC_MV
                            this.minVolume = minVolume
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
                runWithResultCheck({ tdApi.ReqOrderInsert(reqField, nextRequestId()) }, {
                    if (isTestingTickToTrade) insertTime = System.nanoTime()
                })
            }
        }
        // 构建返回的 order 对象
        val now = LocalDateTime.now()
        val order = Order(
            config.investorId, "${frontId}_${sessionId}_${orderRef}", tradingDate,
            code, instruments[code]?.name ?: code, price, -1.0, volume, minVolume, direction, offset, orderType,
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
            if (isTestingTickToTrade) {
                order.tttTime = insertTime!!
            }
            ensureFullSecurityInfo(code)
            getOrQueryTick(code).first?.calculateOrderFrozenCash(order)
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
     * 确保证券 [code] 对应的 [SecurityInfo] 信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓））
     */
    suspend fun ensureFullSecurityInfo(code: String, throwException: Boolean = true) {
        val info = instruments[code] ?: return
        fun handleException(e: Exception, msg: String) {
            if (throwException){
                throw e
            } else {
                postBrokerLogEvent(LogLevel.ERROR, msg)
            }
        }
        when (info.type) {
            SecurityType.FUTURES -> {
                if (info.marginRateLong == 0.0) {
                    postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期货保证金率：$code")
                    runWithRetry({ queryFuturesMarginRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期货保证金率出错：$code, $e")
                    }
                }
                if (info.openCommissionRate == 0.0) {
                    postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期货手续费率：$code")
                    runWithRetry({ queryFuturesCommissionRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期货手续费率出错：$code, $e")
                    }
                }
                if (info.todayHighLimitPrice == 0.0 && code !in dayPriceInfoQueriedCodes) {
                    postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期货最新 Tick：$code")
                    runWithRetry({ queryLastTick(code, false) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期货最新 Tick出错：$code, $e")
                    }
                }
            }
            SecurityType.OPTIONS -> {
                if (info.marginRateLong == 0.0) {
                    postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期权保证金率：$code")
                    runWithRetry({ queryOptionsMargin(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期保证金出错：$code, $e")
                    }
                }
                if (info.openCommissionRate == 0.0) {
                    postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期权手续费率：$code")
                    runWithRetry({ queryOptionsCommissionRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期权手续费率出错：$code, $e")
                    }
                }
                if (info.todayHighLimitPrice == 0.0 && code !in dayPriceInfoQueriedCodes) {
                    postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期权最新 Tick：$code")
                    runWithRetry({ queryLastTick(code, false) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期权最新 Tick出错：$code, $e")
                    }
                }
            }
            else -> Unit
        }
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
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        } else {
            val info = instruments[code]
            if (info != null && info.type == SecurityType.FUTURES && code !in marginRateQueriedCodes) {
                val qryField = CThostFtdcQryInstrumentMarginRateField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    hedgeFlag = THOST_FTDC_HF_Speculation
                    instrumentID = parseCode(code).second
                }
                val requestId = nextRequestId()
                runWithResultCheck<Unit>({ tdApi.ReqQryInstrumentMarginRate(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        } else {
            val info = instruments[code]
            if (info != null && info.type == SecurityType.OPTIONS && code !in marginRateQueriedCodes) {
                val qryField = CThostFtdcQryOptionInstrTradeCostField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    hedgeFlag = THOST_FTDC_HF_Speculation
                    instrumentID = parseCode(code).second
                }
                val requestId = nextRequestId()
                runWithResultCheck<Unit>({ tdApi.ReqQryOptionInstrTradeCost(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, data = mutableListOf<Job>())
                }
            }).forEach { it.join() }
        } else {
            val info = instruments[code]
            if (info != null && info.type == SecurityType.FUTURES && code !in commissionRateQueriedCodes) {
                val qryField = CThostFtdcQryInstrumentCommissionRateField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    val (excId, insId) = parseCode(code)
                    exchangeID = excId
                    instrumentID = insId
                }
                val requestId = nextRequestId()
                runWithResultCheck<List<Job>>({ tdApi.ReqQryInstrumentCommissionRate(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation)
                }
            })
        } else {
            val instrument = instruments[code]
            if (instrument != null && instrument.type == SecurityType.OPTIONS && code !in commissionRateQueriedCodes) {
                val qryField = CThostFtdcQryOptionInstrCommRateField().apply {
                    brokerID = config.brokerId
                    investorID = config.investorId
                    val (excId, insId) = parseCode(code)
                    exchangeID = excId
                    instrumentID = insId
                }
                val requestId = nextRequestId()
                runWithResultCheck<Unit>({ tdApi.ReqQryOptionInstrCommRate(qryField, requestId) }, {
                    suspendCoroutineWithTimeout(config.timeout) { continuation ->
                        requestMap[requestId] = RequestContinuation(requestId, continuation)
                    }
                })
            }
        }
    }

    /**
     * 查询最新 [Tick]。如果对应的 [instruments] 无当日价格信息（涨跌停价、昨收昨结昨仓），则自动将当日价格信息写入其中
     */
    suspend fun queryLastTick(code: String, useCache: Boolean, extras: Map<String, String>? = null): Tick? {
        var resultTick: Tick? = null
        if (useCache) {
            val cachedTick = mdApi.lastTicks[code]
            if (cachedTick != null) {
                cachedTick.status = getInstrumentStatus(code)
                cachedTickMap[code] = cachedTick
                resultTick =  cachedTick
            }
        } else {
            val qryField = CThostFtdcQryDepthMarketDataField().apply {
                instrumentID = parseCode(code).second
            }
            val requestId = nextRequestId()
            resultTick = runWithResultCheck({ tdApi.ReqQryDepthMarketData(qryField, requestId) }, {
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, data = code)
                }
            })
        }
        val info = instruments[code]
        if (info?.type == SecurityType.OPTIONS && resultTick != null) {
            resultTick.optionsUnderlyingPrice = getOrQueryTick(info.optionsUnderlyingCode).first?.price ?: 0.0
        }
        return resultTick
    }

    /**
     * 获取缓存的最新/旧的 [Tick]，如果都没有，那么试图查询最新的并缓存，并且自动订阅行情
     * @return [Pair.first] 为查询到的 [Tick]，可能为 null。[Pair.second] 为 [Boolean]，表示查询到的 Tick 是否是实时最新的
     */
    private suspend fun getOrQueryTick(code: String): Pair<Tick?, Boolean> {
        var tick: Tick? = null
        var isLatestTick = false // 查询到的 tick 是否是最新的
        // 如果行情已连接，则优先尝试获取行情缓存的最新 tick
        if (mdApi.connected) {
            tick = mdApi.lastTicks[code]
            // 如果缓存的 tick 为空，说明未订阅该合约，那么订阅该合约以方便后续计算
            if (tick == null) {
                try {
                    postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.getOrQueryTick】自动订阅行情：$code")
                    mdApi.subscribeMarketData(listOf(code))
                } catch (e: Exception) {
                    postBrokerLogEvent(LogLevel.ERROR, "【CtpTdApi.getOrQueryTick】自动订阅合约行情失败：$code, $e")
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
                postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.getOrQueryTick】自动查询并缓存最新 Tick：$code")
                tick = runWithRetry({ queryLastTick(code, useCache = false) }) { e->
                    postBrokerLogEvent(LogLevel.ERROR, "【CtpTdApi.getOrQueryTick】查询合约最新 Tick 失败：$code, $e")
                    null
                }
                if (tick != null) isLatestTick = true
            }
        }
        return Pair(tick, isLatestTick)
    }

    /**
     * 查询某一特定合约的信息。[extras.ensureFullInfo: Boolean = false]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），如果之前没查过，会耗时。当 useCache 为 false 时无效】
     */
    suspend fun querySecurity(code: String, useCache: Boolean = true, extras: Map<String, String>? = null): SecurityInfo? {
        if (useCache) {
            val cachedInstrument = instruments[code]
            if (cachedInstrument != null) {
                if (extras?.get("ensureFullInfo") == "true") {
                    ensureFullSecurityInfo(code)
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
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = code)
            }
        })
    }

    /**
     * 查询全市场合约的信息。[extras.ensureFullInfo: Boolean = false]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），如果之前没查过，会很耗时。当 useCache 为 false 时无效】
     */
    suspend fun queryAllSecurities(useCache: Boolean = true, extras: Map<String, String>? = null): List<SecurityInfo> {
        if (useCache && instruments.isNotEmpty()) {
            if (extras?.get("ensureFullInfo") == "true") {
                instruments.keys.forEach {
                    ensureFullSecurityInfo(it)
                }
            }
            return instruments.values.toList()
        }
        val qryField = CThostFtdcQryInstrumentField()
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInstrument(qryField, requestId) }, {
            suspendCoroutineWithTimeout(config.timeout * 3) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = mutableListOf<SecurityInfo>())
            }
        })
    }

    /**
     * 按 [SecurityInfo.productId] 查询证券信息。[extras.ensureFullInfo: Boolean = false]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），如果之前没查过，会耗时。当 useCache 为 false 时无效】
     * @param productId 证券品种
     */
    suspend fun querySecurities(
        productId: String,
        useCache: Boolean,
        extras: Map<String, String>?
    ): List<SecurityInfo> {
        if (useCache && instruments.isNotEmpty()) {
            val results = instruments.values.filter { it.productId == productId }
            if (extras?.get("ensureFullInfo") == "true") {
                results.forEach {
                    ensureFullSecurityInfo(it.code)
                }
            }
            return results
        }
        return queryAllSecurities(useCache = false).filter { it.productId == productId }
    }

    /**
     * 按标的物代码查询期权信息。[extras.ensureFullInfo: Boolean = false]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），如果之前没查过，会耗时。当 useCache 为 false 时无效】
     * @param underlyingCode 期权标的物的代码
     * @param type 期权的类型，默认为 null，表示返回所有类型的期权
     */
    suspend fun queryOptions(
        underlyingCode: String,
        type: OptionsType? = null,
        useCache: Boolean = true,
        extras: Map<String, String>? = null
    ): List<SecurityInfo> {
        if (useCache && instruments.isNotEmpty()) {
            val results = instruments.values.filter { it.type == SecurityType.OPTIONS && it.optionsUnderlyingCode == underlyingCode }
            if (extras?.get("ensureFullInfo") == "true") {
                results.forEach {
                    ensureFullSecurityInfo(it.code)
                }
            }
            return results
        }
        return queryAllSecurities(useCache = false).filter { it.type == SecurityType.OPTIONS && it.optionsUnderlyingCode == underlyingCode }
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
                ensureFullSecurityInfo(order.code)
                getOrQueryTick(order.code).first?.calculateOrderFrozenCash(order)
                return order
            }
        }
        val qryField = CThostFtdcQryOrderField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
        }
        val requestId = nextRequestId()
        return runWithResultCheck<Order?>({ tdApi.ReqQryOrder(qryField, requestId) }, {
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryOrdersData(orderId))
            }
        })?.apply {
            ensureFullSecurityInfo(code)
            getOrQueryTick(code).first?.calculateOrderFrozenCash(this)
        }
    }

    /**
     * 查询订单
     */
    suspend fun queryOrders(code: String? = null, onlyUnfinished: Boolean = true, useCache: Boolean = true, extras: Map<String, String>? = null): List<Order> {
        val results = if (useCache) {
            var orders: List<Order> = if (onlyUnfinished) {
                mutableListOf<Order>().apply {
                    addAll(unfinishedLongOrders)
                    addAll(unfinishedShortOrders)
                }
            } else todayOrders.values.toList()
            if (code != null) {
                orders = orders.filter { it.code == code }
            }
            orders
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
            runWithResultCheck({ tdApi.ReqQryOrder(qryField, requestId) }, {
                suspendCoroutineWithTimeout(config.timeout * 3) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryOrdersData(null, code, onlyUnfinished))
                }
            })
        }
        return results.onEach {
            if (it.offset == OrderOffset.OPEN && it.volume > it.filledVolume) {
                ensureFullSecurityInfo(it.code)
                getOrQueryTick(it.code).first?.calculateOrderFrozenCash(it)
            }
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
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryTradesData(tradeId))
            }
        })?.apply {
            ensureFullSecurityInfo(code)
            getOrQueryTick(code).first?.calculateTrade(this)
        }
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
                suspendCoroutineWithTimeout(config.timeout * 3) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, data = reqData)
                }
            }).onEach {
                ensureFullSecurityInfo(it.code)
                getOrQueryTick(it.code).first?.calculateTrade(it)
            }
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
        val rawAssets = runWithResultCheck<Assets>({ tdApi.ReqQryTradingAccount(qryField, requestId) }, {
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation)
            }
        })
        assets = rawAssets
        lastQueryAssetsTime = System.currentTimeMillis()
        // 计算持仓盈亏
        assets.positionPnl = 0.0
        positions.forEach { (code, bi) ->
            ensureFullSecurityInfo(code)
            val lastTick = getOrQueryTick(code).first
            if (lastTick != null) {
                bi.long?.let {
                    lastTick.calculatePosition(it, calculateValue = false)
                    assets.positionPnl += it.pnl()
                }
                bi.short?.let {
                    lastTick.calculatePosition(it, calculateValue = false)
                    assets.positionPnl += it.pnl()
                }
            }
        }
        return assets.deepCopy()
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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
                positions.forEach { (c, biPosition) ->
                    ensureFullSecurityInfo(c)
                    biPosition.long?.let { positionList.add(it) }
                    biPosition.short?.let { positionList.add(it) }
                }
            } else { // 查询单合约持仓
                positions[code]?.let { biPosition ->
                    ensureFullSecurityInfo(code)
                    biPosition.long?.let { positionList.add(it) }
                    biPosition.short?.let { positionList.add(it) }
                }
            }
            positionList.forEach {
                val lastTick = getOrQueryTick(it.code).first
                if (lastTick != null) {
                    val marginPriceType = when (lastTick.info?.type) {
                        SecurityType.FUTURES -> futuresMarginPriceType
                        SecurityType.OPTIONS -> optionsMarginPriceType
                        else -> MarginPriceType.PRE_SETTLEMENT_PRICE
                    }
                    lastTick.calculatePosition(it, marginPriceType, calculateValue = true)
                }
            }
            return positionList.map { it.deepCopy() }
        } else {
            val qryField = CThostFtdcQryInvestorPositionField().apply {
                if (!code.isNullOrEmpty()) instrumentID = parseCode(code).second
            }
            val requestId = nextRequestId()
            return runWithResultCheck<List<Position>>({ tdApi.ReqQryInvestorPosition(qryField, requestId) }, {
                suspendCoroutineWithTimeout(config.timeout * 3) { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, tag = code ?: "", data = mutableListOf<Position>())
                }
            }).onEach {
                val lastTick = getOrQueryTick(it.code).first
                lastTick?.calculatePosition(it, calculateValue = false)
            }
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
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = QueryPositionDetailsData(code))
            }
        })
    }

    /**
     * Ctp TdApi 的回调类
     */
    private inner class CtpTdSpi : CThostFtdcTraderSpi() {

        /**
         * 请求客户端认证。由于可能是交易日结束后断线自动重连，此时经纪商服务器刚启动，响应时间较长，
         * 容易因请求超时而自动重连失败，因此设置了 [timeout] 参数用于在这种情况下延长超时时间（单位：ms）
         */
        private suspend fun reqAuthenticate(timeout: Long = config.timeout) {
            val reqField = CThostFtdcReqAuthenticateField().apply {
                appID = config.appId
                authCode = config.authCode
                userProductInfo = config.userProductInfo
                userID = config.investorId
                brokerID = config.brokerId
            }
            val requestId = nextRequestId()
            runWithResultCheck<Unit>({ tdApi.ReqAuthenticate(reqField, requestId) }, {
                suspendCoroutineWithTimeout(timeout) { continuation ->
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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
                fun resumeConnectWithException(errorInfo: String, e: Exception) {
                    e.printStackTrace()
                    postBrokerLogEvent(LogLevel.ERROR, errorInfo)
                    resumeRequestsWithException("connect", errorInfo)
                }
                try {
                    // 请求客户端认证
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】客户端认证...")
                    try {
                        if (hasRequest("connect")) {
                            reqAuthenticate()
                        } else {
                            reqAuthenticate(60000)
                        }
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】客户端认证成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("【交易接口登录】请求客户端认证失败：$e", e)
                    }
                    // 请求用户登录
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】资金账户登录...")
                    try {
                        reqUserLogin()
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】资金账户登录成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("【交易接口登录】请求用户登录失败：$e", e)
                    }
                    // 请求结算单确认
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】结算单确认...")
                    try {
                        reqSettlementInfoConfirm()
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】结算单确认成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("【交易接口登录】请求结算单确认失败：$e", e)
                    }
                    // 查询全市场合约
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询全市场合约...")
                    runWithRetry({
                        val allInstruments = queryAllSecurities(false, null)
                        allInstruments.forEach {
                            instruments[it.code] = it
                            codeProductMap[it.code] = it.productId
                            mdApi.codeMap[it.code.split('.', limit = 2)[1]] = it.code
                        }
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询全市场合约成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询全市场合约失败：$e", e)
                    }
                    // 查询保证金价格类型、持仓合约的保证金率及手续费率（如果未禁止费用计算）
                    // 查询保证金价格类型
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询保证金价格类型...")
                    runWithRetry({
                        queryMarginPriceType()
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询保证金价格类型成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询保证金价格类型失败：$e", e)
                    }
                    // 查询持仓合约的手续费率及保证金率
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询持仓合约手续费率及保证金率...")
                    try {
                        runWithRetry({ queryFuturesCommissionRate() })
                        runWithRetry({ queryFuturesMarginRate() })
                        runWithRetry({ queryOptionsCommissionRate() })
                        runWithRetry({ queryOptionsMargin() })
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询持仓合约手续费率及保证金率成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("【交易接口登录】查询持仓合约手续费率及保证金率失败：$e", e)
                    }
                    // 查询账户持仓
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询账户持仓...")
                    runWithRetry({
                        queryPositions(useCache = false)
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询账户持仓成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询账户持仓失败：$e", e)
                    }
                    // 订阅持仓合约行情（如果行情可用且未禁止自动订阅）
                    if (mdApi.connected) {
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】订阅持仓合约行情...")
                        try {
                            mdApi.subscribeMarketData(positions.keys)
                            postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】订阅持仓合约行情成功")
                        } catch (e: Exception) {
                            resumeConnectWithException("【交易接口登录】订阅持仓合约行情失败：$e", e)
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
                        resumeConnectWithException("【交易接口登录】查询当日订单失败：$e", e)
                    }
                    // 查询当日成交记录
                    postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日成交记录...")
                    runWithRetry({
                        val trades = queryTrades(useCache = false)
                        todayTrades.addAll(trades)
                        postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日成交记录成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询当日成交记录失败：$e", e)
                    }
                    // 登录操作完成
                    connected = true
                    postBrokerConnectionEvent(ConnectionEventType.TD_LOGGED_IN)
                    resumeRequests("connect", Unit)
                } catch (e: Exception) {  // 登录操作失败
                    e.printStackTrace()
                    postBrokerLogEvent(LogLevel.ERROR, "【交易接口登录】发生预期外的异常：$e")
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
                    marginRateQueriedCodes.clear()
                    commissionRateQueriedCodes.clear()
                    dayPriceInfoQueriedCodes.clear()
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
            scope.launch {
                val orderId = "${pOrder.frontID}_${pOrder.sessionID}_${pOrder.orderRef}"
                var order = todayOrders[pOrder.orderRef]
                // 如果不是本 session 发出的订单，找到或创建缓存的订单
                if (order == null || orderId != order.orderId) {
                    // 首先检查是否已缓存过
                    order = todayOrders[orderId]
                    // 如果是第一次接收回报，则创建并缓存该订单，之后局部变量 order 不为 null
                    if (order == null) {
                        val code = "${pOrder.exchangeID}.${pOrder.instrumentID}"
                        order = Converter.orderC2A(tradingDate, pOrder, instruments[code]) { e ->
                            postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnOrder】Order time 解析失败：${orderId}, $code, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
                        }
                        ensureFullSecurityInfo(code)
                        getOrQueryTick(code).first?.calculateOrderFrozenCash(order)
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
                        THOST_FTDC_OST_NoTradeQueueing -> OrderStatus.ACCEPTED
                        THOST_FTDC_OST_PartTradedQueueing -> OrderStatus.PARTIALLY_FILLED
                        THOST_FTDC_OST_AllTraded -> {
                            removeUnfinishedOrder(order)
                            OrderStatus.FILLED
                        }
                        THOST_FTDC_OST_Canceled -> {
                            removeUnfinishedOrder(order)
                            cancelStatistics[order.code] = cancelStatistics.getOrDefault(order.code, 0) + 1
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
                    // 如果是中金所，那么计算报单/撤单手续费
                    if (pOrder.exchangeID == ExchangeID.CFFEX && instruments[code]?.type == SecurityType.FUTURES) {
                        when (status) {
                            OrderStatus.ACCEPTED,
                            OrderStatus.PARTIALLY_FILLED,
                            OrderStatus.FILLED -> {
                                if (!insertFeeCalculated) {
                                    commission += volume
                                    insertFeeCalculated = true
                                }
                            }
                            OrderStatus.CANCELED -> {
                                if (!cancelFeeCalculated) {
                                    commission += volume
                                    cancelFeeCalculated = true
                                }
                            }
                            else -> Unit
                        }
                        // 如果有申报手续费，加到 position 的手续费统计中
                        if (oldCommission != commission) {
                            val position = queryCachedPosition(order.code, order.direction, order.offset != OrderOffset.OPEN)
                            position?.apply {
                                todayCommission += commission - oldCommission
                            }
                        }
                    }
                }
                // 仅发送与成交不相关的订单状态更新回报，成交相关的订单状态更新回报会在 OnRtnTrade 中发出，以确保成交回报先于状态回报
                if (newOrderStatus != OrderStatus.PARTIALLY_FILLED && newOrderStatus != OrderStatus.FILLED) {
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
                                    if (oldStatus != OrderStatus.ERROR && oldStatus != OrderStatus.CANCELED) {
                                        val restVolume = order.volume - pOrder.volumeTraded
                                        position.frozenVolume -= restVolume
                                    }
                                }
                                else -> Unit
                            }
                        }
                    }
                    if (newOrderStatus == OrderStatus.ERROR) {
                        order.updateTime = LocalDateTime.now()
                    } else {
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
                    }
                    if (oldStatus != newOrderStatus || newOrderStatus == OrderStatus.ERROR) {
                        postBrokerEvent(BrokerEventType.ORDER_STATUS, order.deepCopy())
                    }
                }
            }
        }

        /**
         * 成交回报回调。该回调会收到该账户下所有 session 的成交回报，因此需要与本 session 的成交回报区分处理
         */
        override fun OnRtnTrade(pTrade: CThostFtdcTradeField) {
            scope.launch {
                var order = todayOrders[pTrade.orderRef]
                val orderSysId = "${pTrade.exchangeID}_${pTrade.orderSysID}"
                if (order == null || order.orderSysId != orderSysId) {
                    order = todayOrders.values.find { it.orderSysId == orderSysId }
                    if (order == null) {
                        postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRtnTrade】收到未知订单的成交回报：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}")
                    }
                }
                val code = "${pTrade.exchangeID}.${pTrade.instrumentID}"
                ensureFullSecurityInfo(code)
                val lastTick = getOrQueryTick(code).first
                val info = lastTick?.info ?: return@launch
                val trade = Converter.tradeC2A(tradingDate, pTrade, order?.orderId ?: orderSysId, info.name) { e ->
                    postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnTrade】Trade tradeTime 解析失败：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}, ${pTrade.tradeDate}T${pTrade.tradeTime}, $e")
                }
                trade.turnover = trade.volume * trade.price * info.volumeMultiple
                // 更新仓位信息，并判断是平今还是平昨
                val biPosition = positions.getOrPut(trade.code) { BiPosition() }
                val position: Position? = when {
                    trade.direction == Direction.LONG && trade.offset == OrderOffset.OPEN ||
                            trade.direction == Direction.SHORT && trade.offset != OrderOffset.OPEN -> {
                        if (biPosition.long == null) {
                            biPosition.long = Position(
                                config.investorId, tradingDate,
                                trade.code, Direction.LONG, 0, 0, 0, 0,
                                0, 0,0, 0.0, 0.0
                            ).apply {
                                ensureFullSecurityInfo(code)
                                info.copyFieldsToPosition(this)
                            }
                        }
                        biPosition.long
                    }
                    trade.direction == Direction.SHORT && trade.offset == OrderOffset.OPEN ||
                            trade.direction == Direction.LONG && trade.offset != OrderOffset.OPEN -> {
                        if (biPosition.short == null) {
                            biPosition.short = Position(
                                config.investorId, tradingDate,
                                trade.code, Direction.SHORT, 0, 0, 0, 0,
                                0, 0, 0, 0.0, 0.0
                            ).apply {
                                ensureFullSecurityInfo(code)
                                info.copyFieldsToPosition(this)
                            }
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
                                val todayFirst = info.closeTodayCommissionRate < info.closeCommissionRate
                                // 依据仓位及是否优先平今判断是否实际平今
                                val yesterdayVolume = position.yesterdayVolume()
                                if (todayFirst) {
                                    when {
                                        // 全部平今
                                        trade.volume <= position.todayVolume -> {
                                            trade.offset = OrderOffset.CLOSE_TODAY
                                            todayClosed = trade.volume
                                        }
                                        // 全部平昨
                                        position.todayVolume == 0 && yesterdayVolume >= trade.volume -> {
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
                                        trade.volume <= yesterdayVolume -> {
                                            trade.offset = OrderOffset.CLOSE_YESTERDAY
                                            yesterdayClosed = trade.volume
                                        }
                                        // 全部平今
                                        yesterdayVolume == 0 && trade.volume <= position.todayVolume -> {
                                            trade.offset = OrderOffset.CLOSE_TODAY
                                            todayClosed = trade.volume
                                        }
                                        // 部分平今部分平昨
                                        trade.volume <= position.volume -> {
                                            trade.offset = OrderOffset.CLOSE
                                            yesterdayClosed = yesterdayVolume
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
                        position.openCost -= position.avgOpenPrice() * totalClosed * info.volumeMultiple
                        // 部分平今部分平昨，则本地计算手续费
                        if (trade.offset == OrderOffset.CLOSE) {
                            val todayClosedTurnover = todayClosed * trade.turnover / totalClosed
                            val yesterdayClosedTurnover = yesterdayClosed * trade.turnover / totalClosed
                            trade.commission = lastTick.calculateCommission(OrderOffset.CLOSE_TODAY, todayClosedTurnover, todayClosed) +
                                    lastTick.calculateCommission(OrderOffset.CLOSE_YESTERDAY, yesterdayClosedTurnover, yesterdayClosed)
                        }
                    }
                }
                if (trade.commission == 0.0) {
                    lastTick.calculateTrade(trade)
                }
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
                    lastTick.calculateOrderFrozenCash(order)
                    postBrokerEvent(BrokerEventType.ORDER_STATUS, order.deepCopy())
                }
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
                    val order = Converter.orderC2A(Converter.dateC2A(pOrder.tradingDay), pOrder, instruments[code]) { e ->
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
                    val code = "${pTrade.exchangeID}.${pTrade.instrumentID}"
                    val trade = Converter.tradeC2A(Converter.dateC2A(pTrade.tradingDay), pTrade, order?.orderId ?: orderSysId, instruments[code]?.name ?: code) { e ->
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
                    val info = instruments[code]
                    if (info != null && info.todayHighLimitPrice == 0.0) {
                        info.preClosePrice = Converter.formatDouble(pDepthMarketData.preClosePrice)
                        info.preSettlementPrice = Converter.formatDouble(pDepthMarketData.preSettlementPrice)
                        info.preOpenInterest = pDepthMarketData.preOpenInterest.toInt()
                        info.todayHighLimitPrice = Converter.formatDouble(pDepthMarketData.upperLimitPrice)
                        info.todayLowLimitPrice = Converter.formatDouble(pDepthMarketData.lowerLimitPrice)
                        dayPriceInfoQueriedCodes.add(code)
                    }
                    val tick = Converter.tickC2A(code, Converter.dateC2A(pDepthMarketData.tradingDay), pDepthMarketData, info = info, marketStatus = getInstrumentStatus(code)) { e ->
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
                Converter.securityC2A(tradingDate, pInstrument) { e ->
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
                (request.continuation as Continuation<Assets>).resume(Converter.assetsC2A(tradingDate, pTradingAccount, futuresMarginPriceType, optionsMarginPriceType))
                requestMap.remove(nRequestID)
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
                        posList.add(Converter.positionC2A(tradingDate, pInvestorPosition, instruments[code]))
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
                    val openDate = Converter.dateC2A(pInvestorPositionDetail.openDate)
                    detail.volume += pInvestorPositionDetail.volume
                    if (detail.tradingDay.isBefore(openDate)) {
                        detail.tradingDay = openDate
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
                        if (instrument != null) {
                            val marginRate = Converter.futuresMarginRateC2A(pMarginRate, code)
                            marginRate.copyFieldsToSecurityInfo(instrument)
                            marginRateQueriedCodes.add(code)
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
//                    var standardCode = ""
                    // 如果是品种代码，更新 instruments 中所有该品种的手续费
                    if (instrument == null) {
                        val instrumentList = instruments.values.filter {
                            if (it.type != SecurityType.FUTURES) return@filter false
                            return@filter it.productId == code
                        }
                        if (instrumentList.isNotEmpty()) {
                            instrumentList.forEach {
                                commissionRate.copyFieldsToSecurityInfo(it)
                                commissionRateQueriedCodes.add(it.code)
                            }
//                            standardCode = instrumentList.first().code
                        }
                    } else { // 如果是合约代码，直接更新合约
                        commissionRate.copyFieldsToSecurityInfo(instrument)
                        commissionRateQueriedCodes.add(instrument.code)
//                        standardCode = instrument.code
                    }
                    // 如果是中金所期货，那么查询申报手续费
//                    if (standardCode.startsWith(ExchangeID.CFFEX)) {
//                        val job = scope.launch {
//                            runWithRetry({ queryFuturesOrderCommissionRate(standardCode) }) { e ->
//                                postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryInstrumentCommissionRate】查询期货申报手续费失败：$standardCode, $e")
//                            }
//                        }
//                        (request.data as MutableList<Job>).add(job)
//                    }
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
                        return@filter it.productId == pOrderCommRate.instrumentID
                    }.forEach {
//                        it.orderInsertFeeByTrade = pOrderCommRate.orderCommByTrade
//                        it.orderInsertFeeByVolume = pOrderCommRate.orderCommByVolume
//                        it.orderCancelFeeByTrade = pOrderCommRate.orderActionCommByTrade
//                        it.orderCancelFeeByVolume = pOrderCommRate.orderActionCommByVolume
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
                        return@filter it.productId == pCommissionRate.instrumentID
                    }
                    instrumentList.forEach {
                        commissionRate.copyFieldsToSecurityInfo(it)
                        commissionRateQueriedCodes.add(it.code)
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
                    if (instrument != null) {
                        val marginRate = Converter.optionsMarginC2A(pOptionMargin, code)
                        marginRate.copyFieldsToSecurityInfo(instrument)
                        marginRateQueriedCodes.add(code)
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