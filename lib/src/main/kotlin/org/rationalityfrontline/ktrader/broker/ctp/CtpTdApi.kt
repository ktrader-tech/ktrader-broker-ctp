@file:Suppress("UNCHECKED_CAST", "UNUSED_PARAMETER")

package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.*
import org.rationalityfrontline.jctp.*
import org.rationalityfrontline.jctp.jctpConstants.*
import org.rationalityfrontline.ktrader.api.broker.*
import org.rationalityfrontline.ktrader.api.datatype.*
import java.io.File
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.math.abs
import kotlin.math.sign

internal class CtpTdApi(val api: CtpBrokerApi) {
    val config: CtpConfig = api.config
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
        cacheFile.writeText("$lastTradingDay\n$tradingDay\n$nextOrderRef")
        return nextOrderRef
    }
    /** 当前交易日 */
    private var tradingDay = ""
        set(value) {
            field = value
            tradingDate = Converter.dateC2A(value)
        }
    /** 当前交易日 */
    var tradingDate: LocalDate = LocalDate.now()
        private set
    /** 上一个交易日 */
    private var lastTradingDay = ""
        set(value) {
            field = value
            lastTradingDate = Converter.dateC2A(value)
        }
    /** 上一个交易日 */
    private var lastTradingDate = tradingDate
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
    /** 发起登录的时间，用于判断在连接失败是是否需要重连（判断是否在前置机断线情况下进行连接） */
    var initTime = System.currentTimeMillis()
    /** 是否存在正在等待中的重连登录操作 */
    private var hasPendingConnectAction = false
    /**
     * 是否在 [connect] 时检测到交易日变更
     */
    private var newTradingDayOnConnect = false
    /**
     * 交易前置是否已连接
     */
    private var frontConnected: Boolean = false
    /**
     * 是否正在进行登录操作（[CtpTdSpi.doConnect]）
     */
    @Volatile private var doConnecting: Boolean = false
    /**
     * 是否已完成登录操作（即处于可用状态）
     */
    var connected: Boolean by api::tdConnected
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
    /** 正在执行 [ensureFullSecurityInfo] 网络查询的证券代码 */
    private val queryingFullSecurityInfoCodes: MutableSet<String> = mutableSetOf()
    /**
     * 品种代码表，key 为合约 code，value 为品种代码(productId)。用于从 code 快速映射到 [productStatusMap]
     */
    private val codeProductMap: MutableMap<String, String> = mutableMapOf()
    /**
     * 品种状态表，key 为品种代码，value 为品种状态
     */
    val productStatusMap: MutableMap<String, MarketStatus> = mutableMapOf()
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
    private fun maxLongPrice(code: String): Double {
        return unfinishedLongOrders.lastOrNull { it.code == code }?.price ?: Double.NEGATIVE_INFINITY
    }
    /**
     * 做空订单的最低挂单价，用于检测自成交
     */
    private fun minShortPrice(code: String): Double {
        return unfinishedShortOrders.firstOrNull { it.code == code }?.price ?: Double.POSITIVE_INFINITY
    }
    /**
     * 合约撤单次数统计，用于检测频繁撤单，key 为 code，value 为撤单次数
     */
    private val cancelStatistics: MutableMap<String, Int> = mutableMapOf()
    /** 用于记录某个订单的冻结仓位是否被计算过，主要用于针对下单立即成交的订单（只会回报一次 FILLED 订单回报） */
    private val orderFrozenVolumeCountedSet = mutableSetOf<String>()

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
     * 依据 [order] 的 direction 向未成交订单缓存中有序插入未成交订单（按挂单价从低到高）
     */
    private fun insertUnfinishedOrder(order: Order) {
        when (order.direction) {
            Direction.LONG -> unfinishedLongOrders.insert(order)
            Direction.SHORT -> unfinishedShortOrders.insert(order)
            Direction.UNKNOWN -> api.postBrokerLogEvent(LogLevel.WARNING, "【CtpTdApi.insertUnfinishedOrder】订单方向为 Direction.UNKNOWN（${order.code}, ${order.orderId}）")
        }
    }

    /**
     * 从未成交订单缓存中移除未成交订单
     */
    private fun removeUnfinishedOrder(order: Order) {
        when (order.direction) {
            Direction.LONG -> unfinishedLongOrders.remove(order)
            Direction.SHORT -> unfinishedShortOrders.remove(order)
            Direction.UNKNOWN -> api.postBrokerLogEvent(LogLevel.WARNING, "【CtpTdApi.removeUnfinishedOrder】订单方向为 Direction.UNKNOWN（${order.code}, ${order.orderId}）")
        }
    }

    /**
     * 判断是否存在标签为 [tag] 的未完成的协程请求
     */
    @Suppress("SameParameterValue")
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
     * 连接交易前置并自动完成登录（还会自动查询持仓、订单、成交记录等信息，详见 [CtpTdSpi.OnFrontConnected]）。在无法连接至前置的情况下可能会长久阻塞。
     * 该操作不可加超时限制，因为可能在双休日等非交易时间段启动程序。
     */
    suspend fun connect() {
        if (inited) return
        inited = true
        suspendCoroutine<Unit> { continuation ->
            val requestId = Int.MIN_VALUE // 因为 OnFrontConnected 中 requestId 会重置为 0，为防止 requestId 重复，取整数最小值
            requestMap[requestId] = RequestContinuation(requestId, continuation, "connect")
            api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】连接前置服务器...")
            tdApi.Init()
        }
    }

    /**
     * 关闭并释放资源
     */
    fun close() {
        if (frontConnected) tdSpi.OnFrontDisconnected(0)
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
     * 向交易所发送订单报单，会自动检查自成交
     */
    suspend fun insertOrder(
        code: String,
        price: Double,
        volume: Int,
        direction: Direction,
        offset: OrderOffset,
        closePositionPrice: Double,
        orderType: OrderType,
        minVolume: Int = 0,
        extras: Map<String, String>? = null
    ): Order {
        val (exchangeId, instrumentId) = parseCode(code)
        val orderRef = nextOrderRef().toString()
        // 检查是否存在自成交风险
        var errorInfo: String? = null
        val minShortPrice = minShortPrice(code)
        val maxLongPrice = maxLongPrice(code)
        when {
            direction == Direction.LONG && price >= minShortPrice -> {
                val lastTick = mdApi.lastTicks[code]
                val isSafe = lastTick != null && lastTick.askPrice.isNotEmpty() && lastTick.askPrice[0] < minShortPrice
                        && (lastTick.askVolume[0] >= volume || (minShortPrice - lastTick.askPrice[0]) / (lastTick.info?.priceTick ?: 100000000.0) > volume * 1.5 / lastTick.askVolume[0])
                if (!isSafe) {
                    errorInfo = "本地拒单：存在自成交风险（当前做多价格为 $price，最低做空价格为 ${minShortPrice}）"
                }
            }
            direction == Direction.SHORT && price <= maxLongPrice -> {
                val lastTick = mdApi.lastTicks[code]
                val isSafe = lastTick != null && lastTick.bidPrice.isNotEmpty() && lastTick.bidPrice[0] > maxLongPrice
                        && (lastTick.bidVolume[0] >= volume || (lastTick.bidPrice[0] - maxLongPrice) / (lastTick.info?.priceTick ?: 100000000.0) > volume * 1.5 / lastTick.bidVolume[0])
                if (!isSafe) {
                    errorInfo = "本地拒单：存在自成交风险（当前做空价格为 $price，最高做多价格为 ${maxLongPrice}）"
                }
            }
        }
        // 用于测试 TickToTrade 的订单插入时间
        var insertTime: Long? = null
        // 无自成交风险，执行下单操作
        var realOffset = offset
        var realVolume = volume
        if (errorInfo == null) {
            // 如果是平仓，那么判断具体是平昨还是平今
            if (offset == OrderOffset.CLOSE) {
                val position = queryCachedPosition(code, direction, true)
                if (position != null) {
                    val info = instruments[code]
                    if (info != null) {
                        // 依据手续费率判断是否优先平今
                        val todayFirst = info.closeTodayCommissionRate < info.closeCommissionRate
                        // 依据仓位及是否优先平今判断是否实际平今
                        val yesterdayVolume = position.yesterdayVolume()
                        if (todayFirst) {
                            when {
                                position.todayVolume >= volume -> realOffset = OrderOffset.CLOSE_TODAY  //全部平今
                                position.todayVolume == 0 && yesterdayVolume >= volume -> realOffset = OrderOffset.CLOSE_YESTERDAY  //全部平昨
                                position.volume >= volume && (code.startsWith(ExchangeID.SHFE) || code.startsWith(ExchangeID.INE)) -> {  //仅平今仓，昨仓会在下次下单时平掉
                                    realOffset = OrderOffset.CLOSE_TODAY
                                    realVolume = position.todayVolume
                                }
                            }
                        } else {
                            when {
                                yesterdayVolume >= volume -> realOffset = OrderOffset.CLOSE_YESTERDAY  //全部平昨
                                yesterdayVolume == 0 && position.todayVolume >= volume -> realOffset = OrderOffset.CLOSE_TODAY  //全部平今
                                position.volume >= volume && (code.startsWith(ExchangeID.SHFE) || code.startsWith(ExchangeID.INE)) -> {  //仅平昨仓，今仓会在下次下单时平掉
                                    realOffset = OrderOffset.CLOSE_YESTERDAY
                                    realVolume = yesterdayVolume
                                }
                            }
                        }
                    }
                }
            }
            val reqField = CThostFtdcInputOrderField().apply {
                this.orderRef = orderRef
                brokerID = config.brokerId
                investorID = config.investorId
                exchangeID = exchangeId
                instrumentID = instrumentId
                limitPrice = price
                this.direction = Converter.directionA2C(direction)
                volumeTotalOriginal = realVolume
                volumeCondition = THOST_FTDC_VC_AV
                combOffsetFlag = Converter.offsetA2C(realOffset)
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
                    if (api.isTestingTickToTrade) insertTime = System.nanoTime()
                })
            }
        }
        // 构建返回的 order 对象
        val now = LocalDateTime.now()
        val order = Order(
            config.investorId, "${frontId}_${sessionId}_${orderRef}", tradingDate,
            code, instruments[code]?.name ?: code, price, closePositionPrice, realVolume, minVolume, direction, realOffset, orderType,
            OrderStatus.SUBMITTING, "报单已提交",
            0, 0.0, 0.0, 0.0, 0.0,
            now, now,
            extras = if (extras != null) {
                mutableMapOf<String, String>().apply { putAll(extras) }
            } else null
        )
        if (errorInfo == null) {
            if (api.isTestingTickToTrade) {
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
            api.scope.launch {
                delay(1)
                api.postBrokerEvent(BrokerEventType.ORDER_STATUS, order)
            }
        }
        return order
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
            throw Exception("本地拒撤：未找到对应的订单记录: $orderId")
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
        val doQuery = info.marginRateLong == 0.0 || info.openCommissionRate == 0.0 || (info.todayHighLimitPrice == 0.0 && code !in dayPriceInfoQueriedCodes)
        if (doQuery) {
            if (code in queryingFullSecurityInfoCodes) {
                val requestId = nextRequestId()
                suspendCoroutine<Unit> { continuation ->
                    requestMap[requestId] = RequestContinuation(requestId, continuation, "ensureFullSecurityInfo-$code")
                }
                return
            } else {
                queryingFullSecurityInfoCodes.add(code)
            }
        } else return
        // 执行实际查询操作
        fun handleException(e: Exception, msg: String) {
            if (throwException){
                throw e
            } else {
                api.postBrokerLogEvent(LogLevel.ERROR, msg)
            }
        }
        when (info.type) {
            SecurityType.FUTURES -> {
                if (info.marginRateLong == 0.0) {
                    api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期货保证金率：$code")
                    runWithRetry({ queryFuturesMarginRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期货保证金率出错：$code, $e")
                    }
                }
                if (info.openCommissionRate == 0.0) {
                    api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期货手续费率：$code")
                    runWithRetry({ queryFuturesCommissionRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期货手续费率出错：$code, $e")
                    }
                }
                if (info.todayHighLimitPrice == 0.0 && code !in dayPriceInfoQueriedCodes) {
                    api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期货最新 Tick：$code")
                    runWithRetry({ queryLastTick(code, useCache = false, extras = mapOf("ensureFullInfo" to "false")) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期货最新 Tick出错：$code, $e")
                    }
                }
            }
            SecurityType.OPTIONS -> {
                if (info.marginRateLong == 0.0) {
                    api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期权保证金率：$code")
                    runWithRetry({ queryOptionsMargin(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期保证金出错：$code, $e")
                    }
                }
                if (info.openCommissionRate == 0.0) {
                    api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期权手续费率：$code")
                    runWithRetry({ queryOptionsCommissionRate(code) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期权手续费率出错：$code, $e")
                    }
                }
                if (info.todayHighLimitPrice == 0.0 && code !in dayPriceInfoQueriedCodes) {
                    api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.ensureFullSecurityInfo】自动查询期权最新 Tick：$code")
                    runWithRetry({ queryLastTick(code, useCache = false, extras = mapOf("ensureFullInfo" to "false")) }) { e ->
                        handleException(e, "【CtpTdApi.ensureFullSecurityInfo】查询期权最新 Tick出错：$code, $e")
                    }
                }
            }
            else -> Unit
        }
        queryingFullSecurityInfoCodes.remove(code)
        resumeRequests("ensureFullSecurityInfo-$code", Unit)
    }

    /** 确保如果是中金所，那么计算报单/撤单手续费（1.0元每笔） */
    private fun ensureOrderActionCommission(order: Order) {
        if (order.code.startsWith(ExchangeID.CFFEX) && instruments[order.code]?.type == SecurityType.FUTURES) {
            when (order.status) {
                OrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED -> {
                    if (!order.insertFeeCalculated) {
                        order.commission += 1.0
                        order.insertFeeCalculated = true
                    }
                }
                OrderStatus.CANCELED -> {
                    if (!order.insertFeeCalculated) {
                        order.commission += 1.0
                        order.insertFeeCalculated = true
                    }
                    if (!order.cancelFeeCalculated) {
                        order.commission += 1.0
                        order.cancelFeeCalculated = true
                    }
                }
                else -> Unit
            }
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
                    requestMap[requestId] = RequestContinuation(requestId, continuation, tag = "", data = mutableListOf<Job>())
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
                        requestMap[requestId] = RequestContinuation(requestId, continuation, tag = code, data = mutableListOf<Job>())
                    }
                }).forEach { it.join() }
            }
        }
    }

    /**
     * 查询期货申报手续费，仅限中金所。
     * 已查过手续费的依然会再次查询。查询到的结果会自动更新到对应的 [instruments] 中
     */
//    private suspend fun queryFuturesOrderCommissionRate(code: String) {
//        val qryField = CThostFtdcQryInstrumentOrderCommRateField().apply {
//            brokerID = config.brokerId
//            investorID = config.investorId
//            instrumentID = parseCode(code).second
//        }
//        val requestId = nextRequestId()
//        runWithResultCheck<Unit>({ tdApi.ReqQryInstrumentOrderCommRate(qryField, requestId) }, {
//            suspendCoroutineWithTimeout(config.timeout) { continuation ->
//                requestMap[requestId] = RequestContinuation(requestId, continuation)
//            }
//        })
//    }

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
     * 查询最新 [Tick]。如果对应的 [instruments] 无当日价格信息（涨跌停价、昨收昨结昨仓），则自动将当日价格信息写入其中（见 [CtpTdSpi.OnRspQryDepthMarketData]）
     * [extras.ensureFullInfo: Boolean = true]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），
     * 如果之前没查过，会很耗时。当 useCache 为 false 时无效，且返回的 [SecurityInfo] 信息不完整】
     */
    suspend fun queryLastTick(code: String, useCache: Boolean, extras: Map<String, String>? = null): Tick? {
        var resultTick: Tick? = null
        if (useCache) {
            val cachedTick = mdApi.lastTicks[code]
            if (cachedTick != null) {
                cachedTickMap[code] = cachedTick
                resultTick = cachedTick
            }
        }
        if (resultTick == null && !code.contains(' ')) {
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
        if (resultTick != null) {
            val info = instruments[code]
            if (info?.type == SecurityType.OPTIONS && !info.optionsUnderlyingCode.startsWith("CFFEX.IO")) {
                resultTick.optionsUnderlyingPrice = getOrQueryTick(info.optionsUnderlyingCode).first?.price ?: 0.0
            }
            if (info != null && extras?.get("ensureFullInfo") != "false") {
                ensureFullSecurityInfo(code)
            }
        }
        return resultTick?.apply {
            resultTick.info?.productId?.let { productID -> productStatusMap[productID]?.let { status = it } }
        }
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
            if (tick == null && code !in mdApi.subscriptions) {
                try {
                    api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.getOrQueryTick】自动订阅行情：$code")
                    mdApi.subscribeMarketData(listOf(code))
                } catch (e: Exception) {
                    api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdApi.getOrQueryTick】自动订阅合约行情失败：$code, $e")
                }
            } else {
                isLatestTick = true
            }
        }
        // 如果未从行情 API 中获得最新 tick，尝试从本地缓存中获取旧的 tick
        if (tick == null) {
            tick = cachedTickMap[code]
            // 如果未从本地缓存中获得旧的 tick，查询最新 tick（查询操作会自动缓存 tick 至本地缓存中）
            if (tick == null && connected) {
                api.postBrokerLogEvent(LogLevel.INFO, "【CtpTdApi.getOrQueryTick】自动查询并缓存最新 Tick：$code")
                tick = runWithRetry({ queryLastTick(code, useCache = false) }) { e->
                    api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdApi.getOrQueryTick】查询合约最新 Tick 失败：$code, $e")
                    null
                }
                if (tick != null) isLatestTick = true
            }
        }
        return Pair(tick, isLatestTick)
    }

    /**
     * 查询某一特定合约的信息。
     * [extras.ensureFullInfo: Boolean = true]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），
     * 如果之前没查过，会耗时。useCache 参数无效，总是使用缓存】
     */
    suspend fun querySecurity(code: String, useCache: Boolean = true, extras: Map<String, String>? = null): SecurityInfo? {
        val cachedInstrument = instruments[code]
        if (cachedInstrument != null) {
            if (extras?.get("ensureFullInfo") != "false") {
                ensureFullSecurityInfo(code)
            }
            return cachedInstrument
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
     * 查询全市场合约的信息。
     * [extras.ensureFullInfo: Boolean = true]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），
     * 如果之前没查过，会很耗时。当 useCache 为 false 时无效，且返回的 [SecurityInfo] 信息不完整】
     */
    suspend fun queryAllSecurities(useCache: Boolean = true, extras: Map<String, String>? = null): List<SecurityInfo> {
        if (useCache && instruments.isNotEmpty()) {
            if (extras?.get("ensureFullInfo") != "false") {
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
     * 按 [SecurityInfo.productId] 查询证券信息。
     * [extras.ensureFullInfo: Boolean = true]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），
     * 如果之前没查过，会耗时。当 useCache 为 false 时无效，且返回的 [SecurityInfo] 信息不完整】
     * @param productId 证券品种
     */
    suspend fun querySecurities(
        productId: String,
        useCache: Boolean,
        extras: Map<String, String>?
    ): List<SecurityInfo> {
        if (useCache && instruments.isNotEmpty()) {
            val results = instruments.values.filter { it.productId == productId }
            if (extras?.get("ensureFullInfo") != "false") {
                results.forEach {
                    ensureFullSecurityInfo(it.code)
                }
            }
            return results
        }
        return queryAllSecurities(useCache = false).filter { it.productId == productId }
    }

    /**
     * 按标的物代码查询期权信息。
     * [extras.ensureFullInfo: Boolean = true]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），
     * 如果之前没查过，会耗时。当 useCache 为 false 时无效，且返回的 [SecurityInfo] 信息不完整】
     * @param underlyingCode 期权标的物的代码
     * @param type 期权的类型，默认为 [OptionsType.UNKNOWN]，表示返回所有类型的期权
     * @param strikePrice 行权价，默认为 null，表示返回所有行权价的期权。如果不存在该行权价的合约，则返回最接近该行权价的合约
     */
    suspend fun queryOptions(
        underlyingCode: String,
        type: OptionsType = OptionsType.UNKNOWN,
        strikePrice: Double?,
        useCache: Boolean = true,
        extras: Map<String, String>? = null
    ): List<SecurityInfo> {
        val predicate = if (type == OptionsType.UNKNOWN) {
            { info: SecurityInfo -> info.type == SecurityType.OPTIONS && info.optionsUnderlyingCode == underlyingCode }
        } else {
            { info: SecurityInfo -> info.type == SecurityType.OPTIONS && info.optionsUnderlyingCode == underlyingCode && info.optionsType == type }
        }
        val results = if (useCache && instruments.isNotEmpty()) {
            val results = instruments.values.filter(predicate)
            if (extras?.get("ensureFullInfo") != "false") {
                results.forEach {
                    ensureFullSecurityInfo(it.code)
                }
            }
            results
        } else queryAllSecurities(useCache = false).filter(predicate)
        return if (strikePrice == null || results.isEmpty()) results else {
            val finalResults = mutableListOf<SecurityInfo>()
            val sortedResults = results.sortedBy { it.optionsStrikePrice }
            val index = sortedResults.binarySearchBy(strikePrice) { it.optionsStrikePrice }
            if (index >= 0) {
                finalResults.add(sortedResults[index])
            } else {
                val insertIndex = -1 - index
                val leftIndex = insertIndex - 1
                val leftInfo = sortedResults.getOrNull(leftIndex)
                val rightInfo = sortedResults.getOrNull(insertIndex)
                if (leftInfo != null && rightInfo != null) {
                    val leftDelta = abs(strikePrice - leftInfo.optionsStrikePrice)
                    val rightDelta = abs(strikePrice - leftInfo.optionsStrikePrice)
                    when {
                        leftDelta > rightDelta -> finalResults.add(rightInfo)
                        leftDelta < rightDelta -> finalResults.add(leftInfo)
                        leftDelta == rightDelta -> finalResults.addAll(arrayOf(leftInfo, rightInfo))
                    }
                } else if (leftInfo != null) {
                    finalResults.add(leftInfo)
                } else if (rightInfo != null) {
                    finalResults.add(rightInfo)
                }
            }
            finalResults
        }
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
     * 查询账户资金信息。[useCache] 参数无效
     */
    suspend fun queryAssets(useCache: Boolean = true, extras: Map<String, String>? = null): Assets {
        val qryField = CThostFtdcQryTradingAccountField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            currencyID = "CNY"
        }
        val requestId = nextRequestId()
        val assets = runWithResultCheck<Assets>({ tdApi.ReqQryTradingAccount(qryField, requestId) }, {
            suspendCoroutineWithTimeout(config.timeout) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation)
            }
        })
        // 计算持仓盈亏
        assets.positionPnl = 0.0
        positions.forEach { (code, bi) ->
            if (code.contains(' ')) return@forEach
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
        return assets
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
            queryCachedPosition(code, direction)
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
                if (it.code.contains(' ')) return@forEach
                val lastTick = getOrQueryTick(it.code).first
                if (lastTick != null) {
                    val marginPriceType = when (lastTick.info?.type) {
                        SecurityType.FUTURES -> futuresMarginPriceType
                        SecurityType.OPTIONS -> optionsMarginPriceType
                        else -> MarginPriceType.PRE_SETTLEMENT_PRICE
                    }
                    lastTick.calculatePosition(it, null, marginPriceType, calculateValue = true)
                }
            }
            return positionList
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
                if (it.code.contains(' ')) return@onEach
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
     * 账户连接操作因异常而中断失败
     */
    private class ConnectAbortedException(msg: String) : Exception(msg)

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
                suspendCoroutineWithTimeout(config.timeout) { continuation ->
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
            if (connected) {  // 避免处理刚连接上时重放的当日历史状态
                // 拷贝值，避免之后 pInstrumentStatus 的内部值在 C++ 中发生更改
                val rawEnterTime = pInstrumentStatus.enterTime
                val productID = pInstrumentStatus.instrumentID
                api.scope.launch {
                    val ticks = mdApi.lastTicks.values.filter { codeProductMap[it.code] == productID }
                    if (ticks.isNotEmpty()) {
                        try {
                            val enterTime = LocalTime.parse(rawEnterTime).atDate(LocalDate.now())
                            fun pushStatusTick(baseTick: Tick) {
                                val newTick = baseTick.copy(status = marketStatus, time = enterTime, volume = 0, turnover = 0.0, openInterestDelta = 0).apply {
                                    extras = (extras ?: mutableMapOf()).apply { put("isStatusTick", "true") }
                                }
                                mdApi.lastTicks[newTick.code] = newTick
                                api.postBrokerEvent(BrokerEventType.TICK, newTick)
                            }
                            ticks.forEach { tick ->
                                // 仅当状态推送比 Tick 推送先到达时进行纯状态 Tick 补发检查
                                if (tick.status != marketStatus && !tick.time.isAfter(enterTime)) {
                                    if (marketStatus == MarketStatus.AUCTION_ORDERING || marketStatus == MarketStatus.CONTINUOUS_MATCHING) {  // 如果是开始信号，立即推送
                                        pushStatusTick(tick)
                                    } else {  // 如果不是开始信号， 等待一会儿后再检查以避免错过对应状态的 Tick
                                        launch delayedCheck@{
                                            delay(config.statusTickDelay)
                                            val lastTick = mdApi.lastTicks[tick.code] ?: return@delayedCheck
                                            if (lastTick.status != marketStatus && !lastTick.time.isAfter(enterTime)) {
                                                pushStatusTick(lastTick)
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (e: Exception) {
                            api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnInstrumentStatus】解析 enterTime 失败：$e")
                        }
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
                    api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspError】$errorInfo")
                } else {
                    resumeRequestsWithException("connect", errorInfo)
                }
            } else {
                request.continuation.resumeWithException(Exception(pRspInfo.errorMsg))
                requestMap.remove(nRequestID)
            }
        }

        /** 进行实际的连接操作 */
        private fun doConnect() {
            api.scope.launch {
                if (doConnecting) return@launch //避免同一时间段重复连接
                doConnecting = true
                fun resumeConnectWithException(errorInfo: String, e: Exception, resumeConnect: Boolean = true) {
                    e.printStackTrace()
                    api.postBrokerLogEvent(LogLevel.ERROR, errorInfo)
                    if (resumeConnect) resumeRequestsWithException("connect", errorInfo)
                    throw ConnectAbortedException(errorInfo)
                }
                try {
                    // 请求客户端认证
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】客户端认证...")
                    try {
                        reqAuthenticate()
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】客户端认证成功")
                    } catch (e: Exception) {
                        if ((!hasRequest("connect") || System.currentTimeMillis() - initTime > 60000) && !hasPendingConnectAction) { //说明遇到了晚上 21:00 或早上 09:00 前断线重连时前置服务器尚未完全未初始化的问题
                            hasPendingConnectAction = true
                            launch {
                                delay(600000)
                                hasPendingConnectAction = false
                                if (frontConnected && !connected && !doConnecting) {
                                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】已等待10分钟，尝试重新登录...")
                                    // 刷新 initTime 以防止在登录参数错误时反复登录
                                    if (hasRequest("connect")) initTime = System.currentTimeMillis()
                                    doConnect()
                                }
                            }
                        }
                        resumeConnectWithException("【交易接口登录】请求客户端认证失败：$e", e, !hasPendingConnectAction)
                    }
                    // 请求用户登录
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】资金账户登录...")
                    try {
                        reqUserLogin()
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】资金账户登录成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("【交易接口登录】请求用户登录失败：$e", e)
                    }
                    // 请求结算单确认
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】结算单确认...")
                    try {
                        reqSettlementInfoConfirm()
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】结算单确认成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("【交易接口登录】请求结算单确认失败：$e", e)
                    }
                    // 查询全市场合约
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询全市场合约...")
                    runWithRetry({
                        val allInstruments = queryAllSecurities(false, null)
                        allInstruments.forEach {
                            instruments[it.code] = it
                            codeProductMap[it.code] = it.productId
                            mdApi.codeMap[it.code.split('.', limit = 2)[1]] = it.code
                        }
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询全市场合约成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询全市场合约失败：$e", e)
                    }
                    // 查询保证金价格类型
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询保证金价格类型...")
                    runWithRetry({
                        queryMarginPriceType()
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询保证金价格类型成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询保证金价格类型失败：$e", e)
                    }
                    // 查询持仓合约的手续费率及保证金率
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询持仓合约手续费率及保证金率...")
                    try {
                        runWithRetry({ queryFuturesCommissionRate() })
                        runWithRetry({ queryFuturesMarginRate() })
                        runWithRetry({ queryOptionsCommissionRate() })
                        runWithRetry({ queryOptionsMargin() })
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询持仓合约手续费率及保证金率成功")
                    } catch (e: Exception) {
                        resumeConnectWithException("【交易接口登录】查询持仓合约手续费率及保证金率失败：$e", e)
                    }
                    // 查询账户持仓
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询账户持仓...")
                    runWithRetry({
                        queryPositions(useCache = false).onEach {
                            it.leftPreVolume = it.preVolume
                            it.leftTodayOpenVolume = it.todayOpenVolume
                        }
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询账户持仓成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询账户持仓失败：$e", e)
                    }
                    // 订阅持仓合约行情
                    if (mdApi.connected) {
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】订阅持仓合约行情...")
                        try {
                            mdApi.subscribeMarketData(positions.keys)
                            api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】订阅持仓合约行情成功")
                        } catch (e: Exception) {
                            resumeConnectWithException("【交易接口登录】订阅持仓合约行情失败：$e", e)
                        }
                    }
                    // 查询当日订单
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日订单...")
                    runWithRetry({
                        val orders = queryOrders(onlyUnfinished = false, useCache = false)
                        orders.forEach {
                            todayOrders[it.orderId] = it
                            if (it.status.isOpen()) {
                                when (it.direction) {
                                    Direction.LONG -> unfinishedLongOrders.insert(it)
                                    Direction.SHORT -> unfinishedShortOrders.insert(it)
                                    else -> api.postBrokerLogEvent(LogLevel.WARNING, "【交易接口登录】查询到未知方向的订单（${it.code}, ${it.direction}）")
                                }
                            }
                            if (it.status == OrderStatus.CANCELED) {
                                cancelStatistics[it.code] = cancelStatistics.getOrDefault(it.code, 0) + 1
                            }
                            ensureOrderActionCommission(it)
                        }
                        orderFrozenVolumeCountedSet.clear()
                        orderFrozenVolumeCountedSet.addAll(todayOrders.keys)
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日订单成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询当日订单失败：$e", e)
                    }
                    // 查询当日成交记录
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日成交记录...")
                    runWithRetry({
                        val trades = queryTrades(useCache = false).sortedBy { it.time }
                        todayTrades.addAll(trades)
                        trades.forEach { trade ->
                            // 如果是平仓，那么判断是平今还是平昨，并重新计算手续费
                            if (trade.offset == OrderOffset.CLOSE) {
                                val biPosition = positions[trade.code]
                                val position = when (trade.direction) {
                                    Direction.SHORT -> biPosition?.long
                                    Direction.LONG -> biPosition?.short
                                    else -> null
                                }
                                if (position != null) {
                                    val info = instruments[trade.code]
                                    if (info != null) {
                                        // 依据手续费率判断是否优先平今
                                        val todayFirst = info.closeTodayCommissionRate < info.closeCommissionRate
                                        // 判断是平昨还是平今
                                        if (todayFirst) {
                                            when {
                                                position.leftTodayOpenVolume >= trade.volume -> {
                                                    position.leftTodayOpenVolume -= trade.volume
                                                    trade.offset = OrderOffset.CLOSE_TODAY
                                                }
                                                position.leftPreVolume >= trade.volume -> {
                                                    position.leftPreVolume -= trade.volume
                                                    trade.offset = OrderOffset.CLOSE_YESTERDAY
                                                }
                                                else -> Unit
                                            }
                                        } else {
                                            when {
                                                position.leftPreVolume >= trade.volume -> {
                                                    position.leftPreVolume -= trade.volume
                                                    trade.offset = OrderOffset.CLOSE_YESTERDAY
                                                }
                                                position.leftTodayOpenVolume >= trade.volume -> {
                                                    position.leftTodayOpenVolume -= trade.volume
                                                    trade.offset = OrderOffset.CLOSE_TODAY
                                                }
                                                else -> Unit
                                            }
                                        }
                                        info.calculateTrade(trade)
                                    }
                                }
                            }
                            todayOrders[trade.orderId]?.apply {
                                turnover += trade.turnover
                                avgFillPrice = turnover / filledVolume / instruments[trade.code]!!.volumeMultiple
                                commission += trade.commission
                                updateTime = maxOf(updateTime, trade.time)
                            }
                        }
                        positions.forEach { (_, bi) ->
                            bi.long?.extras?.apply {
                                remove("leftPreVolume")
                                remove("leftTodayOpenVolume")
                            }
                            bi.short?.extras?.apply {
                                remove("leftPreVolume")
                                remove("leftTodayOpenVolume")
                            }
                        }
                        api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】查询当日成交记录成功")
                    }) { e ->
                        resumeConnectWithException("【交易接口登录】查询当日成交记录失败：$e", e)
                    }
                    // 登录操作完成
                    api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】登录成功")
                    connected = true
                    resumeRequests("connect", Unit)
                    if (newTradingDayOnConnect) {
                        newTradingDayOnConnect = false
                        api.postBrokerEvent(BrokerEventType.NEW_TRADING_DAY, Converter.dateC2A(tradingDay))
                    }
                } catch (e: ConnectAbortedException) {  // 登录操作因已知原因中断，什么都不做
                } catch (e: Exception) {  // 登录操作因未知原因失败
                    e.printStackTrace()
                    api.postBrokerLogEvent(LogLevel.ERROR, "【交易接口登录】发生预期外的异常：$e")
                    resumeRequestsWithException("connect", e.message ?: e.toString())
                } finally {
                    doConnecting = false
                }
            }
        }

        /**
         * 行情前置连接时回调。会将 [requestId] 置为 0
         */
        override fun OnFrontConnected() {
            frontConnected = true
            requestId.set(0)
            api.postBrokerLogEvent(LogLevel.INFO, "【交易接口登录】前置服务器已连接")
            doConnect()
        }

        /**
         * 交易前置断开连接时回调。会将 [connected] 置为 false；异常完成所有的协程请求
         */
        override fun OnFrontDisconnected(nReason: Int) {
            if (!frontConnected) return //避免重复处理相同事件，这在使用 SIMNOW 账户时会发生
            val msg = "【CtpTdSpi.OnFrontDisconnected】前置服务器连接断开：${getDisconnectReason(nReason)} ($nReason)"
            api.postBrokerLogEvent(LogLevel.INFO, msg)
            frontConnected = false
            connected = false
            var toClear: List<RequestContinuation> = requestMap.values.toList()
            if (hasPendingConnectAction) toClear = toClear.filter { it.tag != "connect" }
            val e = Exception(msg)
            toClear.forEach {
                it.continuation.resumeWithException(e)
                requestMap.remove(it.requestId)
            }
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
                var lastMaxOrderRef = 10000
                if (cacheFile.exists()) {
                    val lines = cacheFile.readLines()
                    if (lines.size >= 2) {
                        lastTradingDay = lines[0]
                        tradingDay = lines[1]
                        lastMaxOrderRef = lines[2].toIntOrNull() ?: lastMaxOrderRef
                    }
                }
                frontId = pRspUserLogin.frontID
                sessionId = pRspUserLogin.sessionID
                // 如果交易日未变，则延续使用上一次的 maxOrderRef
                if (tradingDay == pRspUserLogin.tradingDay) {
                    orderRef.set(lastMaxOrderRef)
                } else { // 如果交易日变动，并将 orderRef 重置为 10000
                    if (tradingDay != "") lastTradingDay = tradingDay
                    tradingDay = pRspUserLogin.tradingDay
                    if (lastTradingDay == "") {
                        var date = tradingDate.minusDays(1)
                        while (date.dayOfWeek in setOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)) {
                            date = date.minusDays(1)
                        }
                        lastTradingDay = date.format(DateTimeFormatter.BASIC_ISO_DATE)
                    }
                    orderRef.set(9999)
                    nextOrderRef()
                    newTradingDayOnConnect = true
                }
                // 清空各种缓存
                instruments.clear()
                codeProductMap.clear()
                mdApi.codeMap.clear()
                unfinishedLongOrders.clear()
                unfinishedShortOrders.clear()
                todayOrders.clear()
                todayTrades.clear()
                positions.clear()
                cancelStatistics.clear()
                marginRateQueriedCodes.clear()
                commissionRateQueriedCodes.clear()
                dayPriceInfoQueriedCodes.clear()
                queryingFullSecurityInfoCodes.clear()
                cachedTickMap.clear()
                orderFrozenVolumeCountedSet.clear()
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

        /** 处理订单错误回报 */
        private fun handleOrderError(pInputOrder: CThostFtdcInputOrderField?, pRspInfo: CThostFtdcRspInfoField?) {
            checkRspInfo(pRspInfo, {}, { errorCode, errorMsg ->
                if (pInputOrder == null) return
                val order = todayOrders[pInputOrder.orderRef] ?: todayOrders.values.find {
                    it.orderId.endsWith(pInputOrder.orderRef) && it.code.endsWith(pInputOrder.instrumentID) && it.price == pInputOrder.limitPrice
                } ?: return
                order.apply {
                    status = OrderStatus.ERROR
                    statusMsg = "$errorMsg ($errorCode)"
                    updateTime = LocalDateTime.now()
                }
                removeUnfinishedOrder(order)
                api.postBrokerEvent(BrokerEventType.ORDER_STATUS, order)
            })
        }

        /**
         * 报单请求异常回报，交易所判断报单失败时会触发此回调（所有连接均能收到回调）
         */
        override fun OnErrRtnOrderInsert(pInputOrder: CThostFtdcInputOrderField?, pRspInfo: CThostFtdcRspInfoField?) {
            handleOrderError(pInputOrder, pRspInfo)
        }

        /**
         * 报单请求异常回报，CTP 本地判断报单失败时会触发此回调（仅本连接回调，其它连接无任何回调）
         */
        override fun OnRspOrderInsert(
            pInputOrder: CThostFtdcInputOrderField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            handleOrderError(pInputOrder, pRspInfo)
        }

        /** 处理撤单错误回报 */
        private fun handleOrderActionError(frontId: Int?, sessionId: Int?, orderRef: String?, pRspInfo: CThostFtdcRspInfoField?) {
            checkRspInfo(pRspInfo, {}, { errorCode, errorMsg ->
                if (orderRef == null) return
                val orderId = "${frontId}_${sessionId}_${orderRef}"
                val order = todayOrders[orderRef] ?: todayOrders[orderId] ?: return
                if (orderId != order.orderId) return
                order.apply {
                    statusMsg = "$errorMsg ($errorCode)"
                    updateTime = LocalDateTime.now()
                }
                api.postBrokerEvent(BrokerEventType.CANCEL_FAILED, order)
            })
        }

        /**
         * 撤单请求异常回报，交易所判断撤单失败时会触发此回调（所有连接均能收到回调）
         */
        override fun OnErrRtnOrderAction(pOrderAction: CThostFtdcOrderActionField?, pRspInfo: CThostFtdcRspInfoField?) {
            handleOrderActionError(pOrderAction?.frontID, pOrderAction?.sessionID, pOrderAction?.orderRef, pRspInfo)
        }

        /**
         * 撤单请求异常回报，CTP 本地判断撤单失败时会触发此回调（仅本连接回调，其它连接无任何回调）
         */
        override fun OnRspOrderAction(
            pInputOrderAction: CThostFtdcInputOrderActionField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            handleOrderActionError(pInputOrderAction?.frontID, pInputOrderAction?.sessionID, pInputOrderAction?.orderRef, pRspInfo)
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
                    order = Converter.orderC2A(tradingDate, lastTradingDate, pOrder, instruments[code]) { e ->
                        api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnOrder】Order time 解析失败：${orderId}, $code, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
                    }
                    api.scope.launch {
                        ensureFullSecurityInfo(code)
                    }
                    val tick = mdApi.lastTicks[code]
                    if (tick == null) {
                        instruments[code]?.calculateOrderFrozenCash(order)
                    } else {
                        tick.calculateOrderFrozenCash(order)
                    }
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
                if (!status.isOpen()) frozenCash = 0.0
            }
            ensureOrderActionCommission(order)
            // 如果有申报手续费，加到 position 的手续费统计中
            if (oldCommission != order.commission) {
                val position = queryCachedPosition(order.code, order.direction, order.offset != OrderOffset.OPEN)
                position?.apply {
                    todayCommission += order.commission - oldCommission
                }
            }
            // 更新仓位冻结信息
            val position = queryCachedPosition(order.code, order.direction, true)
            if (order.orderId !in orderFrozenVolumeCountedSet) {
                orderFrozenVolumeCountedSet.add(order.orderId)
                if (position != null) {
                    position.frozenVolume += order.volume
                }
            }
            if (newOrderStatus == OrderStatus.ERROR || newOrderStatus == OrderStatus.CANCELED) {
                if (oldStatus != OrderStatus.ERROR && oldStatus != OrderStatus.CANCELED) {
                    val restVolume = order.volume - pOrder.volumeTraded
                    if (position != null) {
                        position.frozenVolume -= restVolume
                    }
                }
            }
            // 仅发送与成交不相关的订单状态更新回报，成交相关的订单状态更新回报会在 OnRtnTrade 中发出，以确保成交回报先于状态回报
            if (newOrderStatus != OrderStatus.PARTIALLY_FILLED && newOrderStatus != OrderStatus.FILLED) {
                if (newOrderStatus == OrderStatus.ERROR) {
                    order.updateTime = LocalDateTime.now()
                } else {
                    val updateTime = try {
                        if (newOrderStatus == OrderStatus.CANCELED) {
                            LocalTime.parse(pOrder.cancelTime).atDate(LocalDate.now())
                        } else {
                            Converter.timeC2A(pOrder.insertTime, tradingDate, lastTradingDate)
                        }
                    } catch (e: Exception) {
                        api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnOrder】Order updateTime 解析失败：${order.orderId}, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
                        LocalDateTime.now()
                    }
                    order.updateTime = updateTime
                }
                if (oldStatus != newOrderStatus) {
                    api.postBrokerEvent(BrokerEventType.ORDER_STATUS, order)
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
                    api.postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRtnTrade】收到未知订单的成交回报：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}")
                }
            }
            val code = "${pTrade.exchangeID}.${pTrade.instrumentID}"
            val info = instruments[code]
            if (info == null) {
                api.postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRtnTrade】收到未知标的的成交回报：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}")
                return
            }
            api.scope.launch {
                ensureFullSecurityInfo(code)
            }
            val trade = Converter.tradeC2A(
                tradingDate,
                lastTradingDate,
                pTrade,
                order?.orderId ?: orderSysId,
                order?.closePositionPrice ?: 0.0,
                info.name,
            ) { e ->
                api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRtnTrade】Trade tradeTime 解析失败：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}, ${pTrade.tradeDate}T${pTrade.tradeTime}, $e")
            }
            val lastTick = mdApi.lastTicks[code]
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
                            0, 0, 0.0, 0.0
                        ).apply {
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
                            0, 0, 0.0, 0.0
                        ).apply {
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
                        trade.commission = info.calculateCommission(OrderOffset.CLOSE_TODAY, todayClosedTurnover, todayClosed) +
                                info.calculateCommission(OrderOffset.CLOSE_YESTERDAY, yesterdayClosedTurnover, yesterdayClosed)
                    }
                }
            }
            if (trade.commission == 0.0) {
                info.calculateTrade(trade)
            }
            if (position != null) {
                position.todayCommission += trade.commission
            }
            todayTrades.add(trade)
            api.postBrokerEvent(BrokerEventType.TRADE_REPORT, trade)
            // 更新 order 信息
            if (order != null) {
                order.filledVolume += trade.volume
                order.turnover += trade.turnover
                order.avgFillPrice = order.turnover / order.filledVolume / info.volumeMultiple
                order.commission += trade.commission
                order.updateTime = trade.time
                order.status = if (order.filledVolume < order.volume) {
                    OrderStatus.PARTIALLY_FILLED
                } else {
                    OrderStatus.FILLED
                }
                if (lastTick == null) {
                    info.calculateOrderFrozenCash(order)
                } else {
                    lastTick.calculateOrderFrozenCash(order)
                }
                api.postBrokerEvent(BrokerEventType.ORDER_STATUS, order)
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
                    val order = Converter.orderC2A(tradingDate, lastTradingDate, pOrder, instruments[code]) { e ->
                        api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryOrder】Order time 解析失败：${"${pOrder.frontID}_${pOrder.sessionID}_${pOrder.orderRef}"}, $code, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
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
                            reqData.results.removeAll { !it.status.isOpen() }
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
                            api.postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRspQryTrade】未找到对应订单：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}")
                        }
                    }
                    val code = "${pTrade.exchangeID}.${pTrade.instrumentID}"
                    val trade = Converter.tradeC2A(tradingDate, lastTradingDate, pTrade, order?.orderId ?: orderSysId, order?.closePositionPrice ?: 0.0, instruments[code]?.name ?: code) { e ->
                        api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryTrade】Trade tradeTime 解析失败：${pTrade.tradeID}, ${pTrade.orderRef}, $orderSysId, ${pTrade.exchangeID}.${pTrade.instrumentID}, ${pTrade.tradeDate}T${pTrade.tradeTime}, $e")
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
                    val tick = Converter.tickC2A(code, Converter.dateC2A(pDepthMarketData.tradingDay), pDepthMarketData, info = info, exchangeID = pDepthMarketData.exchangeID) { e ->
                        api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryDepthMarketData】Tick updateTime 解析失败：${request.data}, ${pDepthMarketData.updateTime}.${pDepthMarketData.updateMillisec}, $e")
                    }
                    correctTickDate(tick)
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
                    api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryInstrument】Instrument 解析失败(${pInstrument.exchangeID}.${pInstrument.instrumentID})：$e")
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
                                else -> api.postBrokerLogEvent(LogLevel.WARNING, "【CtpTdSpi.OnRspQryInvestorPosition】查询到未知的持仓方向（${it.code}, ${it.direction}）")
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
                    var instrument = instruments[code]
//                    var standardCode = ""
                    // 如果是品种代码，更新正确合约的手续费（返回品种代码时并不一定所有合约的手续费相同）
                    if (instrument == null) {
                        if (request.tag == "") { // 查询持仓合约手续费的情况
                            val instrumentList = instruments.values.filter {
                                if (it.type != SecurityType.FUTURES) return@filter false
                                return@filter it.productId == code && positions[it.code] != null
                            }
                            if (instrumentList.isNotEmpty()) {
                                instrumentList.forEach {
                                    commissionRate.copyFieldsToSecurityInfo(it)
                                    commissionRateQueriedCodes.add(it.code)
                                }
//                              standardCode = instrumentList.first().code
                            }
                        } else { // 查询具体合约的情况
                            instrument = instruments[request.tag]
                        }
                    }
                    if (instrument != null ){
                        commissionRate.copyFieldsToSecurityInfo(instrument)
                        commissionRateQueriedCodes.add(instrument.code)
//                        standardCode = instrument.code
                    }
                    // 如果是中金所期货，那么查询申报手续费
//                    if (standardCode.startsWith(ExchangeID.CFFEX)) {
//                        val job = api.scope.launch {
//                            runWithRetry({ queryFuturesOrderCommissionRate(standardCode) }) { e ->
//                                api.postBrokerLogEvent(LogLevel.ERROR, "【CtpTdSpi.OnRspQryInstrumentCommissionRate】查询期货申报手续费失败：$standardCode, $e")
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
//        override fun OnRspQryInstrumentOrderCommRate(
//            pOrderCommRate: CThostFtdcInstrumentOrderCommRateField?,
//            pRspInfo: CThostFtdcRspInfoField?,
//            nRequestID: Int,
//            bIsLast: Boolean
//        ) {
//            val request = requestMap[nRequestID] ?: return
//            checkRspInfo(pRspInfo, {
//                // 目前只有 IC, IH, IF 能查到挂单撤单手续费，并且每个品种会返回 3 条 hedgeFlag 分别为 1, 3, 2 的其余字段相同的记录
//                if (pOrderCommRate != null && pOrderCommRate.hedgeFlag == THOST_FTDC_HF_Speculation) {
//                    instruments.values.filter {
//                        if (it.type != SecurityType.FUTURES) return@filter false
//                        return@filter it.productId == pOrderCommRate.instrumentID
//                    }.forEach {
//                        it.orderInsertFeeByTrade = pOrderCommRate.orderCommByTrade
//                        it.orderInsertFeeByVolume = pOrderCommRate.orderCommByVolume
//                        it.orderCancelFeeByTrade = pOrderCommRate.orderActionCommByTrade
//                        it.orderCancelFeeByVolume = pOrderCommRate.orderActionCommByVolume
//                    }
//                }
//                if (bIsLast) {
//                    (request.continuation as Continuation<Unit>).resume(Unit)
//                    requestMap.remove(nRequestID)
//                }
//            }) { errorCode, errorMsg ->
//                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
//                requestMap.remove(nRequestID)
//            }
//        }

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