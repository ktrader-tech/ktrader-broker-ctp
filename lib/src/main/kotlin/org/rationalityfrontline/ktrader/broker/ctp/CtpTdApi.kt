@file:Suppress("UNCHECKED_CAST", "UNUSED_PARAMETER")

package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.*
import org.rationalityfrontline.jctp.*
import org.rationalityfrontline.jctp.jctpConstants.*
import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.broker.api.*
import java.io.File
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class CtpTdApi(val config: CtpConfig, val kEvent: KEvent, val sourceId: String) {
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
    private val scope = CoroutineScope(Dispatchers.Default)
    /**
     * 上次更新的交易日。不可用来作为当日交易日，因为 [connected] 可能处于 false 状态，此时该值可能因过期而失效
     */
    private var tradingDay = ""
    /**
     * 用于记录维护交易日及 orderRef 的缓存文件
     */
    private val cacheFile: File
    /**
     * 行情 Api 对象，用于获取最新 Tick，并在查询全市场合约时更新其 codeMap
     */
    lateinit var mdApi: CtpMdApi
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
     * 缓存的合约信息，key 为 code
     */
    val instruments: MutableMap<String, Instrument> = mutableMapOf()
    /**
     * 合约代码前缀表，key 为 code，value 为 code 的英文前缀部分（品种代码）。用于从 code 快速映射到 instrumentStatusMap
     */
    private val codePrefixMap: MutableMap<String, String> = mutableMapOf()
    /**
     * 合约状态表
     */
    private val instrumentStatusMap: MutableMap<String, MarketStatus> = mutableMapOf()
    /**
     * 缓存的订单
     */
    private val orders: MutableMap<String, Order> = mutableMapOf()

    init {
        val tdCachePath = "${config.cachePath.ifBlank { "./ctp_cache/" }}${config.investorId.ifBlank { "unknown" }}/td/"
        File(tdCachePath).mkdirs()
        cacheFile = File("${tdCachePath}cache.txt")
        tdApi = CThostFtdcTraderApi.CreateFtdcTraderApi(tdCachePath)
        tdSpi = CtpTdSpi()
        tdApi.apply {
            RegisterSpi(tdSpi)
            // 默认 RESUME 订阅私有流
            val resumeType = when (config.flowSubscribeType.uppercase(Locale.getDefault())) {
                "RESTART" -> THOST_TE_RESUME_TYPE.THOST_TERT_RESTART
                "RESUME" -> THOST_TE_RESUME_TYPE.THOST_TERT_RESUME
                "QUICK" -> THOST_TE_RESUME_TYPE.THOST_TERT_QUICK
                else -> THOST_TE_RESUME_TYPE.THOST_TERT_RESUME
            }
            SubscribePrivateTopic(resumeType)
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
        val codePrefix = codePrefixMap[code]
        return if (codePrefix == null) MarketStatus.UNKNOWN else instrumentStatusMap[codePrefix] ?: MarketStatus.UNKNOWN
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
     * 连接交易前置并自动完成认证、登录、结算单确认、全市场合约查询
     */
    suspend fun connect() {
        suspendCoroutine<Unit> { continuation ->
            val requestId = nextRequestId()
            requestMap[requestId] = RequestContinuation(requestId, continuation, "connect")
            tdApi.Init()
        }
    }

    /**
     * 关闭并释放资源，会发送一条 [BrokerEventType.TD_NET_DISCONNECTED] 信息
     */
    fun close() {
        tdSpi.OnFrontDisconnected(0)
        scope.cancel()
        instruments.clear()
        instrumentStatusMap.clear()
        codePrefixMap.clear()
        tdApi.Release()
        tdApi.delete()
    }

    /**
     * 获取当前交易日
     */
    fun getTradingDay(): String {
        return tdApi.GetTradingDay()
    }

    /**
     * 向交易所发送订单报单。[extras.minVolume: Int]【最小成交量。仅当 [orderType] 为 [OrderType.FAK] 时生效】
     */
    fun insertOrder(
        code: String,
        price: Double,
        volume: Int,
        direction: Direction,
        offset: OrderOffset,
        orderType: OrderType,
        extras: Map<String, Any>?
    ): Order {
        val (exchangeId, instrumentId) = parseCode(code)
        val reqField = CThostFtdcInputOrderField().apply {
            orderRef = nextOrderRef().toString()
            brokerID = config.brokerId
            investorID = config.investorId
            exchangeID = exchangeId
            instrumentID = instrumentId
            limitPrice = price
            this.direction = Translator.directionA2C(direction)
            volumeTotalOriginal = volume
            volumeCondition = THOST_FTDC_VC_AV
            combOffsetFlag = Translator.offsetA2C(offset)
            combHedgeFlag = Translator.THOST_FTDC_HF_Speculation
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
                    if (extras?.contains("minVolume") == true) {
                        volumeCondition = THOST_FTDC_VC_MV
                        minVolume = extras["minVolume"] as Int
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
                else -> throw IllegalArgumentException("未支持 $orderType 类型的订单")
            }
        }
        runBlocking {
            runWithResultCheck({ tdApi.ReqOrderInsert(reqField, nextRequestId()) }, {})
        }
        val now = LocalDateTime.now()
        val order = Order(
            "${frontId}_${sessionId}_${reqField.orderRef}_${reqField.instrumentID}",
            code, price, volume, direction, offset, orderType,
            OrderStatus.SUBMITTING,
            0, 0.0, 0.0, 0.0,
            now, now,
            extras = mutableMapOf()
        )
        orders[reqField.orderRef] = order
        return order
    }

    /**
     * 撤单。[orderId] 格式为 frontId_sessionId_orderRef_instrumentId
     */
    fun cancelOrder(orderId: String, extras: Map<String, Any>?) {
        val cancelReqField = CThostFtdcInputOrderActionField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            userID = config.investorId
            actionFlag = THOST_FTDC_AF_Delete
        }
        val splitResult = orderId.split('_')
        when (splitResult.size) {
            2 -> {
                cancelReqField.apply {
                    exchangeID = splitResult[0]
                    orderSysID = splitResult[1]
                }
            }
            4 -> {
                cancelReqField.apply {
                    frontID = splitResult[0].toInt()
                    sessionID = splitResult[1].toInt()
                    orderRef = splitResult[2]
                    instrumentID = splitResult[3]
                }
            }
            else -> {
                throw IllegalArgumentException("不合法的 orderId ($orderId)。正确格式为：frontId_sessionId_orderRef_instrumentId 或 exchangeId_orderSysId")
            }
        }
        runBlocking {
            runWithResultCheck({ tdApi.ReqOrderAction(cancelReqField, nextRequestId()) }, {})
        }
    }

    /**
     * 查询最新 [Tick]
     */
    suspend fun queryLastTick(code: String, useCache: Boolean, extras: Map<String, Any>?): Tick {
        if (useCache) {
            val cachedTick = mdApi.lastTicks[code]
            if (cachedTick != null) return cachedTick.apply { status = getInstrumentStatus(code) }
        }
        val instrumentId = parseCode(code).second
        val qryField = CThostFtdcQryDepthMarketDataField().apply {
            instrumentID = instrumentId
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryDepthMarketData(qryField, requestId) }, {
            suspendCoroutine { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = code)
            }
        })
    }

    /**
     * 查询某一特定合约的信息
     */
    suspend fun queryInstrument(code: String, useCache: Boolean, extras: Map<String, Any>?): Instrument {
        if (useCache) {
            val cachedInstrument = instruments[code]
            if (cachedInstrument != null) return cachedInstrument
        }
        val (exchangeId, instrumentId) = parseCode(code)
        val qryField = CThostFtdcQryInstrumentField().apply {
            exchangeID = exchangeId
            instrumentID = instrumentId
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInstrument(qryField, requestId) }, {
            suspendCoroutine { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = code)
            }
        })
    }

    /**
     * 查询全市场合约的信息
     */
    suspend fun queryAllInstruments(useCache: Boolean, extras: Map<String, Any>?): List<Instrument> {
        if (useCache && instruments.isNotEmpty()) return instruments.values.toList()
        val qryField = CThostFtdcQryInstrumentField()
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInstrument(qryField, requestId) }, {
            suspendCoroutine { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = mutableListOf<Instrument>())
            }
        })
    }

    suspend fun queryAssets(extras: Map<String, Any>?): Assets {
        val qryField = CThostFtdcQryTradingAccountField().apply {
            brokerID = config.brokerId
            investorID = config.investorId
            currencyID = "CNY"
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryTradingAccount(qryField, requestId) }, {
            suspendCoroutine { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation)
            }
        })
    }

    suspend fun queryPositions(code: String?, extras: Map<String, Any>?): List<Position> {
        val qryField = CThostFtdcQryInvestorPositionField().apply {
            if (code != null) instrumentID = code.split('.').last()
        }
        val requestId = nextRequestId()
        return runWithResultCheck({ tdApi.ReqQryInvestorPosition(qryField, requestId) }, {
            suspendCoroutine { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, data = mutableListOf<Position>())
            }
        })
    }

    /**
     * Ctp TdApi 的回调类
     */
    private inner class CtpTdSpi : CThostFtdcTraderSpi() {

        /**
         * 发生错误时回调。如果没有对应的协程请求，会发送一条 [BrokerEventType.TD_ERROR] 信息；有对应的协程请求时，会将其异常完成
         */
        override fun OnRspError(pRspInfo: CThostFtdcRspInfoField, nRequestID: Int, bIsLast: Boolean) {
            val request = requestMap[nRequestID]
            if (request == null) {
                val errorInfo = "${pRspInfo.errorMsg}, requestId=$nRequestID, isLast=$bIsLast"
                val connectRequests = requestMap.values.filter { it.tag == "connect" }
                if (connectRequests.isEmpty()) {
                    postBrokerEvent(BrokerEventType.TD_ERROR, errorInfo)
                } else {
                    resumeRequestsWithException("connect", errorInfo)
                }
            } else {
                request.continuation.resumeWithException(Exception(pRspInfo.errorMsg))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 当合约交易状态变化时回调。会更新 [instrumentStatusMap]
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
            instrumentStatusMap["${pInstrumentStatus.exchangeID}.${pInstrumentStatus.instrumentID}"] = marketStatus
        }

        /**
         * 行情前置连接时回调。会将 [requestId] 置为 0；发送一条 [BrokerEventType.TD_NET_CONNECTED] 信息；自动请求客户端认证 tdApi.ReqAuthenticate，参见 [OnRspAuthenticate]
         */
        override fun OnFrontConnected() {
            requestId.set(0)
            postBrokerEvent(BrokerEventType.TD_NET_CONNECTED, Unit)
            val authField = CThostFtdcReqAuthenticateField().apply {
                appID = config.appId
                authCode = config.authCode
                userProductInfo = config.userProductInfo
                userID = config.investorId
                brokerID = config.brokerId
            }
            runBlocking {
                runWithResultCheck({ tdApi.ReqAuthenticate(authField, nextRequestId()) }, {}, { code, info ->
                    resumeRequestsWithException("connect", "请求客户端认证失败：$info, $code")
                })
            }
        }

        /**
         * 交易前置断开连接时回调。会将 [connected] 置为 false；发送一条 [BrokerEventType.TD_NET_DISCONNECTED] 信息；异常完成所有的协程请求
         */
        override fun OnFrontDisconnected(nReason: Int) {
            connected = false
            postBrokerEvent(BrokerEventType.TD_NET_DISCONNECTED, "${getDisconnectReason(nReason)} ($nReason)")
            val e = Exception("网络连接断开：${getDisconnectReason(nReason)} ($nReason)")
            requestMap.values.forEach {
                it.continuation.resumeWithException(e)
            }
            requestMap.clear()
        }

        /**
         * 客户端认证请求响应，在该回调中自动请求用户登录 tdApi.ReqUserLogin，参见 [OnRspUserLogin]
         */
        override fun OnRspAuthenticate(
            pRspAuthenticateField: CThostFtdcRspAuthenticateField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            checkRspInfo(pRspInfo, {
                val loginField = CThostFtdcReqUserLoginField().apply {
                    userID = config.investorId
                    password = config.password
                    brokerID = config.brokerId
                    userProductInfo = config.userProductInfo
                }
                runBlocking {
                    runWithResultCheck({ tdApi.ReqUserLogin(loginField, nextRequestId()) }, {}, { code, info ->
                        resumeRequestsWithException("connect", "请求用户登录失败：$info ($code)")
                    })
                }
            }, { errorCode, errorMsg ->
                resumeRequestsWithException("connect", "请求客户端认证失败：$errorMsg ($errorCode)")
            })
        }

        /**
         * 用户登录请求响应，在该回调中自动请求结算单确认 tdApi.ReqSettlementInfoConfirm (否则因交易所规定无法报撤单)，参见 [OnRspSettlementInfoConfirm]
         */
        override fun OnRspUserLogin(
            pRspUserLogin: CThostFtdcRspUserLoginField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            checkRspInfo(pRspInfo, {
                if (pRspUserLogin == null) {
                    resumeRequestsWithException("connect", "请求用户登录失败：pRspUserLogin 为 null")
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
                } else { // 如果交易日变动，则清空订单缓存，并将 orderRef 重置为 10000
                    orderRef.set(10000)
                    orders.clear()
                }
                val confirmField = CThostFtdcSettlementInfoConfirmField().apply {
                    investorID = config.investorId
                    brokerID = config.brokerId
                }
                runBlocking {
                    runWithResultCheck({ tdApi.ReqSettlementInfoConfirm(confirmField, nextRequestId()) }, {}, { code, info ->
                        resumeRequestsWithException("connect", "请求结算单确认失败：$info, $code")
                    })
                }
            }, { errorCode, errorMsg ->
                resumeRequestsWithException("connect", "请求用户登录失败：$errorMsg ($errorCode)")
            })
        }

        /**
         * 用户登出结果回调。暂时无用（目前没有主动请求 tdApi.ReqUserLogout 的情况）
         */
        override fun OnRspUserLogout(
            pUserLogout: CThostFtdcUserLogoutField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            checkRspInfo(pRspInfo, {
                connected = false
                postBrokerEvent(BrokerEventType.TD_USER_LOGGED_OUT, Unit)
            }, { errorCode, errorMsg ->
                postBrokerEvent(BrokerEventType.TD_ERROR, "请求用户登出失败：$errorMsg ($errorCode)")
            })
        }

        /**
         * 结算单确认请求响应，在该回调中自动查询全市场合约 (用于更新 [instruments], [codePrefixMap], mdApi.codeMap)
         */
        override fun OnRspSettlementInfoConfirm(
            pSettlementInfoConfirm: CThostFtdcSettlementInfoConfirmField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            checkRspInfo(pRspInfo, {
                scope.launch {
                    try {
                        val allInstruments = queryAllInstruments(false, null)
                        val delimiter = "[0-9]".toRegex()
                        allInstruments.forEach {
                            instruments[it.code] = it
                            codePrefixMap[it.code] = it.code.split(delimiter, limit = 2)[0]
                            mdApi.codeMap[it.code.split('.', limit = 2)[1]] = it.code
                        }
                        connected = true
                        postBrokerEvent(BrokerEventType.TD_USER_LOGGED_IN, Unit)
                        resumeRequests("connect", Unit)
                    } catch (e: Exception) {
                        resumeRequestsWithException("connect", "查询全市场合约信息失败：$e")
                    }
                }
            }, { errorCode, errorMsg ->
                resumeRequestsWithException("connect", "请求结算单确认失败：$errorMsg ($errorCode)")
            })
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
                val cachedOrder = orders[pInputOrder.orderRef] ?: return
                cachedOrder.status = OrderStatus.ERROR
                val orderId = "${frontId}_${sessionId}_${pInputOrder.orderRef}_${pInputOrder.instrumentID}"
                postBrokerEvent(BrokerEventType.TD_ORDER_STATUS, OrderStatusUpdate(
                    orderId = orderId,
                    newStatus = OrderStatus.ERROR,
                    statusMsg = "$errorMsg ($errorCode)",
                ))
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
                val cachedOrder = orders[pInputOrderAction.orderRef] ?: return
                cachedOrder.status = OrderStatus.ERROR
                val orderId = "${pInputOrderAction.frontID}_${pInputOrderAction.sessionID}_${pInputOrderAction.orderRef}_${pInputOrderAction.instrumentID}"
                postBrokerEvent(BrokerEventType.TD_ORDER_STATUS, OrderStatusUpdate(
                    orderId = orderId,
                    newStatus = OrderStatus.ERROR,
                    statusMsg = "$errorMsg ($errorCode)",
                ))
            })
        }

        /**
         * 订单状态更新回调
         */
        override fun OnRtnOrder(pOrder: CThostFtdcOrderField) {
            val orderId = "${pOrder.frontID}_${pOrder.sessionID}_${pOrder.orderRef}_${pOrder.instrumentID}"
            val cachedOrder = orders[pOrder.orderRef]
            if (cachedOrder?.orderId != orderId) return
            if (pOrder.orderSysID.isNotEmpty()) cachedOrder.orderSysId = "${pOrder.exchangeID}_${pOrder.orderSysID}"
            val newOrderStatus = when (pOrder.orderSubmitStatus) {
                THOST_FTDC_OSS_InsertRejected,
                THOST_FTDC_OSS_CancelRejected,
                THOST_FTDC_OSS_ModifyRejected -> OrderStatus.ERROR
                else -> when (pOrder.orderStatus) {
                    THOST_FTDC_OST_Unknown -> OrderStatus.SUBMITTING
                    THOST_FTDC_OST_NoTradeQueueing -> OrderStatus.ACCEPTED
                    THOST_FTDC_OST_PartTradedQueueing -> OrderStatus.PARTIALLY_FILLED
                    THOST_FTDC_OST_AllTraded -> OrderStatus.FILLED
                    THOST_FTDC_OST_Canceled -> OrderStatus.CANCELED
                    else -> OrderStatus.ERROR
                }
            }
            if (newOrderStatus == OrderStatus.ERROR) {
                cachedOrder.status = OrderStatus.ERROR
                postBrokerEvent(BrokerEventType.TD_ORDER_STATUS, OrderStatusUpdate(orderId, newOrderStatus, pOrder.statusMsg))
            } else {
                if (newOrderStatus != cachedOrder.status) {
                    cachedOrder.status = newOrderStatus
                    val updateTime = try {
                        if (newOrderStatus == OrderStatus.CANCELED) {
                            LocalTime.parse(pOrder.cancelTime).atDate(LocalDate.now())
                        } else {
                            val date = pOrder.insertDate
                            LocalDateTime.parse("${date.slice(0..3)}-${date.slice(4..5)}-${date.slice(6..7)}T${pOrder.insertTime}")
                        }
                    } catch (e: Exception) {
                        postBrokerEvent(BrokerEventType.TD_ERROR, "OnRtnOrder updateTime 解析失败：${cachedOrder.orderId}, ${pOrder.insertDate}_${pOrder.insertTime}_${pOrder.cancelTime}, $e")
                        LocalDateTime.now()
                    }
                    postBrokerEvent(BrokerEventType.TD_ORDER_STATUS, OrderStatusUpdate(orderId, newOrderStatus, pOrder.statusMsg, updateTime))
                }
            }
        }

        /**
         * 成交回报回调
         */
        override fun OnRtnTrade(pTrade: CThostFtdcTradeField) {
            val cachedOrder = orders[pTrade.orderRef]
            if (cachedOrder == null || cachedOrder.orderSysId != "${pTrade.exchangeID}_${pTrade.orderSysID}") return
            val tradeTime = try {
                val date = pTrade.tradeDate
                val updateTimeStr = "${date.slice(0..3)}-${date.slice(4..5)}-${date.slice(6..7)}T${pTrade.tradeTime}"
                LocalDateTime.parse(updateTimeStr)
            } catch (e: Exception) {
                postBrokerEvent(BrokerEventType.TD_ERROR, "OnRtnTrade tradeTime 解析失败：${pTrade.tradeID}, ${pTrade.exchangeID}.${pTrade.instrumentID}, ${pTrade.tradeDate}T${pTrade.tradeTime}, $e")
                LocalDateTime.now()
            }
            postBrokerEvent(BrokerEventType.TD_ORDER_FILLED, Trade(
                tradeId = "${pTrade.tradeID}_${pTrade.orderRef}",
                orderId = cachedOrder.orderId,
                code = "${pTrade.exchangeID}.${pTrade.instrumentID}",
                price = pTrade.price,
                volume = pTrade.volume,
                direction = Translator.directionC2A(pTrade.direction),
                offset = Translator.offsetL2C(pTrade.offsetFlag.toString()),
                commission = 0.0,
                time = tradeTime
            ))
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
                    request.continuation.resumeWithException(Exception("未找到 $reqCode 对应的标的"))
                    requestMap.remove(nRequestID)
                    return
                }
                val code = "${pDepthMarketData.exchangeID}.${pDepthMarketData.instrumentID}"
                if (code == reqCode) {
                    val tick = Translator.tickC2A(code, pDepthMarketData, volumeMultiple = instruments[code]?.volumeMultiple, marketStatus = getInstrumentStatus(code)) { e ->
                        postBrokerEvent(BrokerEventType.TD_ERROR, "OnRspQryDepthMarketData updateTime 解析失败：${request.data}, ${pDepthMarketData.updateTime}.${pDepthMarketData.updateMillisec}, $e")
                    }
                    (request.continuation as Continuation<Tick>).resume(tick)
                    requestMap.remove(nRequestID)
                } else {
                    if (bIsLast) {
                        request.continuation.resumeWithException(Exception("未找到 $reqCode 对应的标的"))
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
                try {
                    val type = when (pInstrument.productClass) {
                        THOST_FTDC_PC_Futures -> InstrumentType.FUTURES
                        THOST_FTDC_PC_Options -> InstrumentType.OPTIONS
                        else -> InstrumentType.UNKNOWN
                    }
                    if (type == InstrumentType.UNKNOWN) null else {
                        Instrument(
                            code = "${pInstrument.exchangeID}.${pInstrument.instrumentID}",
                            type = type,
                            name = pInstrument.instrumentName,
                            priceTick = pInstrument.priceTick,
                            volumeMultiple = pInstrument.volumeMultiple,
                            isTrading = pInstrument.isTrading != 0,
                            openDate = LocalDate.parse(pInstrument.openDate, DateTimeFormatter.BASIC_ISO_DATE),
                            expireDate = LocalDate.parse(pInstrument.expireDate, DateTimeFormatter.BASIC_ISO_DATE),
                            endDeliveryDate = LocalDate.parse(pInstrument.endDelivDate, DateTimeFormatter.BASIC_ISO_DATE),
                            isUseMaxMarginSideAlgorithm = pInstrument.maxMarginSideAlgorithm == THOST_FTDC_MMSA_YES,
                            optionsType = when (pInstrument.optionsType) {
                                THOST_FTDC_CP_CallOptions -> OptionsType.CALL
                                THOST_FTDC_CP_PutOptions -> OptionsType.PUT
                                else -> null
                            }
                        )
                    }
                } catch (e: Exception) {
                    postBrokerEvent(BrokerEventType.TD_ERROR, "OnRspQryInstrument Instrument 解析失败(${pInstrument.exchangeID}.${pInstrument.instrumentID})：$e")
                    null
                }
            }
            checkRspInfo(pRspInfo, {
                // 如果是查询单个合约
                if (reqData is String) {
                    if (instrument == null) {
                        request.continuation.resumeWithException(Exception("未找到 $reqData 对应的标的"))
                        requestMap.remove(nRequestID)
                        return
                    }
                    if (reqData == instrument.code) {
                        (request.continuation as Continuation<Instrument>).resume(instrument)
                        requestMap.remove(nRequestID)
                    } else {
                        if (bIsLast) {
                            request.continuation.resumeWithException(Exception("未找到 $reqData 对应的标的"))
                            requestMap.remove(nRequestID)
                        }
                    }
                } else { // 如果是查询多个合约
                    val insList = request.data as MutableList<Instrument>
                    if (instrument != null) insList.add(instrument)
                    if (bIsLast) {
                        (request.continuation as Continuation<List<Instrument>>).resume(insList)
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
                (request.continuation as Continuation<Assets>).resume(Assets(
                    total = pTradingAccount.balance,
                    available = pTradingAccount.available,
                    positionValue = pTradingAccount.currMargin,
                    frozenByOrder = pTradingAccount.frozenCash,
                    todayCommission = pTradingAccount.commission,
                ))
                requestMap.remove(nRequestID)
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }

        /**
         * 账户持仓查询请求响应
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
                if (pInvestorPosition != null && pInvestorPosition.position > 0) {
                    val direction = Translator.directionC2A(pInvestorPosition.posiDirection)
                    val frozenVolume =  when (direction) {
                        Direction.LONG -> pInvestorPosition.shortFrozen
                        Direction.SHORT -> pInvestorPosition.longFrozen
                        else -> 0
                    }
                    posList.add(Position(
                        code = "${pInvestorPosition.exchangeID}.${pInvestorPosition.instrumentID}",
                        direction = direction,
                        yesterdayVolume = pInvestorPosition.ydPosition,
                        volume = pInvestorPosition.position,
                        value = 0.0,
                        todayVolume = pInvestorPosition.todayPosition,
                        frozenVolume = frozenVolume,
                        closeableVolume = pInvestorPosition.position - frozenVolume,
                        todayOpenVolume = pInvestorPosition.openVolume,
                        todayCloseVolume = pInvestorPosition.closeVolume,
                        todayCommission = 0.0,
                        openCost = pInvestorPosition.openCost,
                        avgOpenPrice = 0.0,
                        lastPrice = 0.0,
                        pnl = 0.0,
                    ))
                }
                if (bIsLast) {
                    (request.continuation as Continuation<List<Position>>).resume(posList)
                    requestMap.remove(nRequestID)
                }
            }, { errorCode, errorMsg ->
                request.continuation.resumeWithException(Exception("$errorMsg ($errorCode)"))
                requestMap.remove(nRequestID)
            })
        }
    }
}