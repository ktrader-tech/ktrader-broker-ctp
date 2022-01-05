@file:Suppress("UNCHECKED_CAST", "UNUSED_PARAMETER", "MemberVisibilityCanBePrivate", "CanBeParameter")

package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.rationalityfrontline.jctp.*
import org.rationalityfrontline.ktrader.api.broker.*
import org.rationalityfrontline.ktrader.api.datatype.SecurityInfo
import org.rationalityfrontline.ktrader.api.datatype.SecurityType
import org.rationalityfrontline.ktrader.api.datatype.Tick
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.math.min

internal class CtpMdApi(val api: CtpBrokerApi) {
    val config: CtpConfig = api.config
    private val mdApi: CThostFtdcMdApi
    private val mdSpi: CtpMdSpi
    /**
     * 协程请求列表，每当网络断开（OnFrontDisconnected）时会清空（resumeWithException）
     */
    private val requestMap: ConcurrentHashMap<Int, RequestContinuation> = ConcurrentHashMap()
    /**
     * 自增的请求 id，每当网络连接时（OnFrontConnected）重置为 0
     */
    private val requestId: AtomicInteger = AtomicInteger(0)
    private fun nextRequestId(): Int = requestId.incrementAndGet()
    /**
     * 上次更新的交易日。当 [connected] 处于 false 状态时可能因过期而失效
     */
    private var tradingDay: String = ""
    /**
     * 交易 Api 对象，用于获取合约的乘数、状态
     */
    lateinit var tdApi: CtpTdApi
    /**
     * 是否已调用过 [CThostFtdcMdApi.Init]
     */
    private var inited = false
    /**
     * 行情前置是否已连接
     */
    private var frontConnected: Boolean = false
    /**
     * 是否已完成登录操作（即处于可用状态）
     */
    var connected: Boolean by api::mdConnected
    /**
     * 当前交易日内已订阅的合约代码集合（当交易日发生更替时上一交易日的订阅会自动失效清零）
     */
    val subscriptions: MutableSet<String> = mutableSetOf()
    /**
     * 缓存的合约代码列表，key 为 InstrumentID, value 为 ExchangeID.InstrumentID（因为 OnRtnDepthMarketData 返回的数据中没有 ExchangeID，所以需要在订阅时缓存完整代码，在 CtpTdApi 获取到全合约信息时会被填充）
     */
    val codeMap = mutableMapOf<String, String>()
    /**
     * 缓存的 [Tick] 表，key 为 code，value 为 [Tick]。每当网络断开（OnFrontDisconnected）时会清空以防止出现过期缓存被查询使用的情况。当某个合约退订时，该合约的缓存 Tick 也会清空。
     */
    val lastTicks = mutableMapOf<String, Tick>()

    init {
        val cachePath = config.cachePath.ifBlank { "./data/ctp/" }
        val mdCachePath = "${if (cachePath.endsWith('/')) cachePath else "$cachePath/"}${config.investorId.ifBlank { "unknown" }}/md/"
        File(mdCachePath).mkdirs()
        mdApi = CThostFtdcMdApi.CreateFtdcMdApi(mdCachePath)
        mdSpi = CtpMdSpi()
        mdApi.RegisterSpi(mdSpi)
        config.mdFronts.forEach { mdFront ->
            mdApi.RegisterFront(mdFront)
        }
    }

    /**
     * 依据 [instrumentId] 获取完整的代码（ExchangeID.InstrumentID）
     */
    private fun getCode(instrumentId: String): String {
        return codeMap[instrumentId] ?: instrumentId
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
     * 连接行情前置并自动完成登录。在无法连接至前置的情况下可能会长久阻塞。
     * 该操作不可加超时限制，因为可能在双休日等非交易时间段启动程序。
     */
    suspend fun connect() {
        if (inited) return
        suspendCoroutine<Unit> { continuation ->
            val requestId = Int.MIN_VALUE // 因为 OnFrontConnected 中 requestId 会重置为 0，为防止 requestId 重复，取整数最小值
            requestMap[requestId] = RequestContinuation(requestId, continuation, "connect")
            api.postBrokerLogEvent(LogLevel.INFO, "【行情接口登录】连接前置服务器...")
            mdApi.Init()
            inited = true
        }
    }

    /**
     * 关闭并释放资源
     */
    fun close() {
        if (frontConnected) mdSpi.OnFrontDisconnected(0)
        subscriptions.clear()
        codeMap.clear()
        mdApi.Release()
        mdApi.delete()
    }

    /**
     * 获取当前交易日
     */
    fun getTradingDay(): String {
        return if (connected) tradingDay else mdApi.GetTradingDay()
    }

    /**
     * 查询当前已订阅的合约。[useCache] 及 [extras] 参数暂时无用
     */
    fun querySubscriptions(useCache: Boolean, extras: Map<String, String>?): List<String> = subscriptions.toList()

    /**
     * 订阅行情。合约代码格式为 ExchangeID.InstrumentID。会自动检查合约订阅状态防止重复订阅。[extras.isForce: Boolean = false]【是否强制向交易所发送未更改的订阅请求（默认只发送未/已被订阅的标的的订阅请求）】
     */
    suspend fun subscribeMarketData(codes: Collection<String>, extras: Map<String, String>? = null): List<SecurityInfo> {
        if (codes.isEmpty()) return emptyList()
        val filteredCodes = if (extras?.get("isForce") != "true") codes.filter { it !in subscriptions } else codes
        if (filteredCodes.isEmpty()) return emptyList()
        // CTP 行情订阅目前（2021.07）每34个订阅会丢失一个订阅（OnRspSubMarketData 中会每34个回调返回一个 bIsLast 为 true），所以需要分割
        if (filteredCodes.size >= 34) {
            val fullCodes = filteredCodes.toList()
            var startIndex = 0
            val resultList = mutableListOf<SecurityInfo>()
            while (startIndex < filteredCodes.size) {
                resultList.addAll(subscribeMarketData(fullCodes.subList(startIndex, min(startIndex + 33, filteredCodes.size))))
                startIndex += 33
            }
            return resultList
        } else { // codes 长度小于34，直接订阅
            val rawCodes = filteredCodes.map { code ->
                val instrumentId = parseCode(code).second
                if (codeMap[instrumentId] == null) codeMap[instrumentId] = code
                instrumentId
            }.toTypedArray()
            val requestId = nextRequestId()
            runWithResultCheck({ mdApi.SubscribeMarketData(rawCodes) }, {
                suspendCoroutineWithTimeout<Unit>(config.timeout) { continuation ->
                    // data 为订阅的 instrumentId 可变集合，在 CtpMdSpi.OnRspSubMarketData 中每收到一条合约订阅成功回报，就将该 instrumentId 从该可变集合中移除。当集合为空时，表明请求完成
                    requestMap[requestId] = RequestContinuation(requestId, continuation, "subscribeMarketData", rawCodes.toMutableSet())
                }
            })
            return filteredCodes.mapNotNull {
                tdApi.instruments[it]?.apply {
                    tdApi.ensureFullSecurityInfo(it)
                    // 如果是期权，自动订阅期权标的物的行情，以更新 Tick.optionsUnderlyingPrice 字段
                    if (type == SecurityType.OPTIONS && optionsUnderlyingCode.isNotEmpty()) {
                        subscribeMarketData(listOf(optionsUnderlyingCode))
                    }
                }
            }
        }
    }

    /**
     * 退订行情。合约代码格式为 ExchangeID.InstrumentID。会自动检查合约订阅状态防止重复退订。[extras.isForce: Boolean = false]【是否强制向交易所发送未更改的订阅请求（默认只发送未/已被订阅的标的的订阅请求）】
     */
    suspend fun unsubscribeMarketData(codes: Collection<String>, extras: Map<String, String>? = null) {
        if (codes.isEmpty()) return
        val filteredCodes = if (extras?.get("isForce") != "true") codes.filter { it in subscriptions } else codes
        if (filteredCodes.isEmpty()) return
        val rawCodes = filteredCodes.map { parseCode(it).second }.toTypedArray()
        val requestId = nextRequestId()
        runWithResultCheck({ mdApi.UnSubscribeMarketData(rawCodes) }, {
            suspendCoroutineWithTimeout<Unit>(config.timeout) { continuation ->
                requestMap[requestId] = RequestContinuation(requestId, continuation, "unsubscribeMarketData", rawCodes.toMutableSet())
            }
        })
    }

    /**
     * 订阅全市场合约行情。会自动检查合约订阅状态防止重复订阅。[extras.isForce: Boolean = false]【是否强制向交易所发送未更改的订阅请求（默认只发送未/已被订阅的标的的订阅请求）】
     */
    suspend fun subscribeAllMarketData(extras: Map<String, String>? = null): List<SecurityInfo> {
        val codes = tdApi.instruments.keys
        if (codes.isEmpty()) throw IllegalStateException("交易前置未连接，无法获得全市场合约")
        return subscribeMarketData(codes, extras)
    }

    /**
     * 退订所有已订阅的合约行情。会自动检查合约订阅状态防止重复退订。[extras.isForce: Boolean = false]【是否强制向交易所发送未更改的订阅请求（默认只发送未/已被订阅的标的的订阅请求）】
     */
    suspend fun unsubscribeAllMarketData(extras: Map<String, String>? = null) {
        unsubscribeMarketData(subscriptions.toList(), extras)
    }

    /**
     * Ctp MdApi 的回调类
     */
    private inner class CtpMdSpi : CThostFtdcMdSpi() {

        /**
         * 发生错误时回调。如果没有对应的协程请求，会发送一条 [BrokerEventType.LOG] 信息；有对应的协程请求时，会将其异常完成
         */
        override fun OnRspError(pRspInfo: CThostFtdcRspInfoField, nRequestID: Int, bIsLast: Boolean) {
            val request = requestMap[nRequestID]
            if (request == null) {
                val errorInfo = "${pRspInfo.errorMsg}, requestId=$nRequestID, isLast=$bIsLast"
                val connectRequests = requestMap.values.filter { it.tag == "connect" }
                if (connectRequests.isEmpty()) {
                    api.postBrokerLogEvent(LogLevel.ERROR, "【CtpMdSpi.OnRspError】$errorInfo")
                } else {
                    resumeRequestsWithException("connect", errorInfo)
                }
            } else {
                request.continuation.resumeWithException(Exception(pRspInfo.errorMsg))
                requestMap.remove(nRequestID)
            }
        }

        /**
         * 行情前置连接时回调。会将 [requestId] 置为 0；自动请求用户登录 mdApi.ReqUserLogin（登录成功后 [connected] 才会置为 true），参见 [OnRspUserLogin]
         */
        override fun OnFrontConnected() {
            frontConnected = true
            requestId.set(0)
            api.postBrokerLogEvent(LogLevel.INFO, "【行情接口登录】前置服务器已连接")
            runBlocking {
                runWithResultCheck({ mdApi.ReqUserLogin(CThostFtdcReqUserLoginField(), nextRequestId()) }, {}, { code, info ->
                    resumeRequestsWithException("connect", "【行情接口登录】请求用户登录失败：$info, $code")
                })
            }
        }

        /**
         * 行情前置断开连接时回调。会将 [connected] 置为 false；清空 [lastTicks]；异常完成所有的协程请求
         */
        override fun OnFrontDisconnected(nReason: Int) {
            frontConnected = false
            connected = false
            lastTicks.clear()
            val msg = "【CtpMdSpi.OnFrontDisconnected】前置服务器连接断开：${getDisconnectReason(nReason)} ($nReason)"
            api.postBrokerLogEvent(LogLevel.INFO, msg)
            val e = Exception(msg)
            requestMap.values.forEach {
                it.continuation.resumeWithException(e)
            }
            requestMap.clear()
        }

        /**
         * 用户登录结果回调。登录成功后 [connected] 会置为 true。如果判断是发生了日内断网重连，会自动重新订阅断连前的已订阅合约。如果交易日变更，已订阅列表会清空。
         */
        override fun OnRspUserLogin(
            pRspUserLogin: CThostFtdcRspUserLoginField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            checkRspInfo(pRspInfo, {
                if (pRspUserLogin == null) {
                    resumeRequestsWithException("connect", "【CtpMdSpi.OnRspUserLogin】请求用户登录失败：pRspUserLogin 为 null")
                    return
                }
                connected = true
                // 如果当日已订阅列表不为空，则说明发生了日内断网重连，自动重新订阅
                if (subscriptions.isNotEmpty() && tradingDay == pRspUserLogin.tradingDay) {
                    api.scope.launch {
                        runWithRetry({ subscribeMarketData(subscriptions.toList(), mapOf("isForce" to "true")) }, { e ->
                            api.postBrokerLogEvent(LogLevel.ERROR, "【CtpMdSpi.OnRspUserLogin】重连后自动订阅行情失败：$e")
                        })
                    }
                }
                // 如果交易日变更，则清空当日已订阅列表
                if (tradingDay != pRspUserLogin.tradingDay) {
                    subscriptions.clear()
                    tradingDay = pRspUserLogin.tradingDay
                }
                api.postBrokerLogEvent(LogLevel.INFO, "【行情接口登录】登录成功")
                resumeRequests("connect", Unit)
            }, { errorCode, errorMsg ->
                resumeRequestsWithException("connect", "【CtpMdSpi.OnRspUserLogin】请求用户登录失败：$errorMsg ($errorCode)")
            })
        }

        /**
         * 行情订阅结果回调。
         */
        override fun OnRspSubMarketData(
            pSpecificInstrument: CThostFtdcSpecificInstrumentField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            if (pSpecificInstrument == null) {
                resumeRequestsWithException("subscribeMarketData", "【CtpMdSpi.OnRspSubMarketData】请求订阅行情失败：pSpecificInstrument 为 null")
                return
            }
            val instrumentId = pSpecificInstrument.instrumentID
            val code = getCode(instrumentId)
            checkRspInfo(pRspInfo, {
                subscriptions.add(code)
                resumeRequests("subscribeMarketData", Unit) { req ->
                    val subscribeSet = req.data as MutableSet<String>
                    subscribeSet.remove(instrumentId)
                    subscribeSet.isEmpty()
                }
            }, { errorCode, errorMsg ->
                resumeRequestsWithException("subscribeMarketData", "【CtpMdSpi.OnRspSubMarketData】请求订阅行情失败($code)：$errorMsg ($errorCode)") { req ->
                    (req.data as MutableSet<String>).contains(instrumentId)
                }
            })
        }

        /**
         * 行情退订结果回调。
         */
        override fun OnRspUnSubMarketData(
            pSpecificInstrument: CThostFtdcSpecificInstrumentField?,
            pRspInfo: CThostFtdcRspInfoField?,
            nRequestID: Int,
            bIsLast: Boolean
        ) {
            if (pSpecificInstrument == null) {
                resumeRequestsWithException("unsubscribeMarketData", "【CtpMdSpi.OnRspUnSubMarketData】请求退订行情失败：pSpecificInstrument 为 null")
                return
            }
            val instrumentId = pSpecificInstrument.instrumentID
            val code = getCode(instrumentId)
            checkRspInfo(pRspInfo, {
                subscriptions.remove(code)
                lastTicks.remove(code)
                resumeRequests("unsubscribeMarketData", Unit) { req ->
                    val subscribeSet = req.data as MutableSet<String>
                    subscribeSet.remove(instrumentId)
                    subscribeSet.isEmpty()
                }
            }, { errorCode, errorMsg ->
                resumeRequestsWithException("unsubscribeMarketData", "【CtpMdSpi.OnRspUnSubMarketData】请求退订行情失败($code)：$errorMsg ($errorCode)") { req ->
                    (req.data as MutableSet<String>).contains(instrumentId)
                }
            })
        }

        /**
         * 行情推送回调。行情会以 [BrokerEventType.TICK] 信息发送
         */
        override fun OnRtnDepthMarketData(data: CThostFtdcDepthMarketDataField) {
            val receiveTime: Long? = if (api.isTestingTickToTrade) System.nanoTime() else null
            val code = getCode(data.instrumentID)
            val lastTick = lastTicks[code]
            val info = tdApi.instruments[code]
            val newTick = Converter.tickC2A(code, tdApi.tradingDate, data, lastTick, info, tdApi.getInstrumentStatus(code)) { e ->
                api.postBrokerLogEvent(LogLevel.ERROR, "【CtpMdSpi.OnRtnDepthMarketData】Tick updateTime 解析失败：$code, ${data.updateTime}.${data.updateMillisec}, $e")
            }
            if (api.isTestingTickToTrade) {
                newTick.tttTime = receiveTime!!
            }
            if (info?.type == SecurityType.OPTIONS) {
                newTick.optionsUnderlyingPrice = lastTicks[info.optionsUnderlyingCode]?.price ?: 0.0
            }
            lastTicks[code] = newTick
            // 过滤掉订阅时自动推送的第一笔数据
            if (lastTick != null && api.tdConnected) api.postBrokerEvent(BrokerEventType.TICK, newTick)
        }
    }
}