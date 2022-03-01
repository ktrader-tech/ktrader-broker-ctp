package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.delay
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import org.rationalityfrontline.jctp.CThostFtdcRspInfoField
import org.rationalityfrontline.ktrader.api.datatype.*
import kotlin.coroutines.Continuation

/**
 * 验证 code 是否规范，并解析返回其交易所代码和合约代码（[Pair.first] 为 exchangeId，[Pair.second] 为 instrumentId）
 */
fun parseCode(code: String): Pair<String, String> {
    val splitResult = code.split('.', limit = 2)
    if (splitResult.size != 2) throw IllegalArgumentException("code 需要包含交易所信息，例：SHFE.ru2109")
    return Pair(splitResult[0], splitResult[1])
}

/**
 * 检查 CTP 回调中的 [pRspInfo] 是否表明请求成功，如果成功执行 [onSuccess], 否则执行 [onError]（参数为错误码及错误信息）
 */
internal inline fun checkRspInfo(pRspInfo: CThostFtdcRspInfoField?, onSuccess: () -> Unit, onError: (Int, String) -> Unit) {
    if (pRspInfo == null || pRspInfo.errorID == 0) {
        onSuccess()
    } else {
        onError(pRspInfo.errorID, pRspInfo.errorMsg)
    }
}

/**
 * 获取 CTP 发送请求时与 [errorCode] 相对应的错误信息
 */
internal fun getErrorInfo(errorCode: Int): String {
    return when (errorCode) {
        -1 -> "网络连接失败"
        -2 -> "未处理请求超过许可数"
        -3 -> "每秒发送请求数超过许可数"
        else -> "发生未知错误：$errorCode"
    }
}

/**
 * 获取 CTP 网络断开时与 [reason] 对应的断开原因，[reason] 为 0 时表明是主动请求断开的
 */
internal fun getDisconnectReason(reason: Int): String {
    return when(reason) {
        0 -> "主动断开"
        4097 -> "网络读失败"
        4098 -> "网络写失败"
        8193 -> "接收心跳超时"
        8194 -> "发送心跳失败"
        8195 -> "收到错误报文"
        else -> "未知原因"
    }
}

/**
 * 发送 CTP 请求并检查其返回码，如果请求成功执行 [onSuccess], 否则执行 [onError]（参数为错误码及错误信息，默认实现为抛出异常）
 * @param action 包含 CTP 请求操作的方法，返回请求码
 * @param retry  是否在请求码为 -2 或 -3 时不断自动间隔 10ms 重新请求
 */
internal suspend inline fun <T> runWithResultCheck(action: () -> Int, onSuccess: () -> T, onError: (Int, String) -> T = { code, info -> throw Exception("$info ($code)") }, retry: Boolean = true): T {
    var resultCode = action()
    if (retry) {
        while (resultCode == -2 || resultCode == -3) {
            delay(10)
            resultCode = action()
        }
    }
    return if (resultCode == 0) {
        onSuccess()
    } else {
        onError(resultCode, getErrorInfo(resultCode))
    }
}

/**
 * 发送用 [runWithResultCheck] 包装过后的 CTP 请求，如果遇到 CTP 柜台处流控，则不断自动间隔 10ms 重新请求
 * @param action 用 [runWithResultCheck] 包装过后的 CTP 请求，返回的是请求结果
 */
internal suspend fun <T> runWithRetry(action: suspend () -> T, onError: (Exception) -> T = { e -> throw e }): T {
    return try {
        action()
    } catch (e: Exception) {
        if (e.message == "CTP:查询未就绪，请稍后重试") {
            delay(10)
            runWithRetry(action, onError)
        } else {
            onError(e)
        }
    }
}

/**
 * [withTimeout] 与 [suspendCancellableCoroutine] 的结合简写
 */
internal suspend inline fun <T> suspendCoroutineWithTimeout(timeMills: Long, crossinline block: (CancellableContinuation<T>) -> Unit): T {
    return withTimeout(timeMills) {
        suspendCancellableCoroutine(block)
    }
}

/**
 * 协程请求续体，用于记录请求并在异步回调时恢复请求
 * @param tag 标签，主要用于登录等没有 requestId 的情况
 * @param data 额外数据
 */
internal data class RequestContinuation(
    val requestId: Int,
    val continuation: Continuation<*>,
    val tag: String = "",
    val data: Any = Unit,
)

/**
 * 用于记录成交记录查询请求的请求参数以及保存查询的结果
 */
internal data class QueryTradesData(
    val tradeId: String? = null,
    var code: String? = null,
    var orderSysId: String? = null,
    val results: MutableList<Trade> = mutableListOf()
)

/**
 * 用于记录订单记录查询请求的参数以及保存查询的结果
 */
internal data class QueryOrdersData(
    val orderId: String? = null,
    val code: String? = null,
    val onlyUnfinished: Boolean = false,
    val results: MutableList<Order> =  mutableListOf(),
)

/**
 * 用于记录持仓明细查询请求的参数以及保存查询的结果
 */
internal data class QueryPositionDetailsData(
    val code: String? = null,
    val direction: Direction? = null,
    val results: MutableList<PositionDetails> = mutableListOf()
)

/**
 * Order 的扩展字段，存储于 extras 中。格式为 exchangeId_orderSysId
 */
var Order.orderSysId: String by StringExtrasDelegate.EMPTY

/**
 * Order 的扩展字段，以 String 格式存储于 extras 中。标记该 order 是否计算过挂单费用（仅限中金所）
 */
var Order.insertFeeCalculated: Boolean by BooleanExtrasDelegate.FALSE

/**
 * Order 的扩展字段，以 String 格式存储于 extras 中。标记该 order 是否计算过撤单费用（仅限中金所）
 */
var Order.cancelFeeCalculated: Boolean by BooleanExtrasDelegate.FALSE

/**
 * 按挂单价从低到高的顺序插入 [order]
 */
internal fun MutableList<Order>.insert(order: Order) {
    var i = indexOfFirst { it.price >= order.price }
    i = if (i == -1) size else i
    add(i, order)
}

/**
 * 交易所 ID
 */
@Suppress("unused")
object ExchangeID {
    const val SHFE = "SHFE"
    const val INE = "INE"
    const val CFFEX = "CFFEX"
    const val DCE = "DCE"
    const val CZCE = "CZCE"
}

/**
 * 手续费率
 * @param code 证券代码
 * @param openRatioByMoney 开仓手续费率（按成交额）
 * @param openRatioByVolume 开仓手续费（按手数）
 * @param closeRatioByMoney 平仓手续费率（按成交额）
 * @param closeRatioByVolume 平仓手续费（按手数）
 * @param closeTodayRatioByMoney 平今仓手续费率（按成交额）
 * @param closeTodayRatioByVolume 平今仓手续费（按手数）
 * @param orderInsertFeeByVolume 报单手续费（按手数），目前仅限中金所股指期货，全部为 0.0
 * @param orderInsertFeeByTrade 报单手续费（按订单），目前仅限中金所股指期货，全部为 1.0
 * @param orderCancelFeeByVolume 撤单手续费（按手数），目前仅限中金所股指期货，全部为 0.0
 * @param orderCancelFeeByTrade 撤单手续费（按订单），目前仅限中金所股指期货，全部为 1.0
 * @param optionsStrikeRatioByMoney 期权行权手续费率（按金额）
 * @param optionsStrikeRatioByVolume 期权行权手续费（按手数）
 * */
data class CommissionRate(
    var code: String,
    var openRatioByMoney: Double,
    var openRatioByVolume: Double,
    var closeRatioByMoney: Double,
    var closeRatioByVolume: Double,
    var closeTodayRatioByMoney: Double,
    var closeTodayRatioByVolume: Double,
    var orderInsertFeeByTrade: Double = 0.0,
    var orderInsertFeeByVolume: Double = 0.0,
    var orderCancelFeeByTrade: Double = 0.0,
    var orderCancelFeeByVolume: Double = 0.0,
    var optionsStrikeRatioByMoney: Double = 0.0,
    var optionsStrikeRatioByVolume: Double = 0.0,
    override var extras: MutableMap<String, String>? = null,
) : ExtrasEntity {
    /**
     * 返回一份自身的完全拷贝
     */
    fun deepCopy(): CommissionRate {
        return copy(extras = extras?.toMutableMap())
    }

    /**
     * 将手续费率处理后赋值给 [info] 的对应字段
     */
    fun copyFieldsToSecurityInfo(info: SecurityInfo) {
        info.openCommissionRate = if (openRatioByVolume > 0.01) openRatioByVolume else openRatioByMoney
        info.closeCommissionRate = if (closeRatioByVolume > 0.01) closeRatioByVolume else closeRatioByMoney
        info.closeTodayCommissionRate = if (closeTodayRatioByVolume > 0.01) closeTodayRatioByVolume else closeTodayRatioByMoney
    }
}

/**
 * 期货/期权保证金率
 * @param code 证券代码
 * @param longMarginRatioByMoney 多头保证金率（按金额）。当证券为期权时表示期权卖方固定保证金（[optionsFixedMargin]）
 * @param longMarginRatioByVolume 多头保证金（按手数），目前全部为 0.0。当证券为期权时表示期权卖方交易所固定保证金（[optionsExchangeFixedMargin]）
 * @param shortMarginRatioByMoney 空头保证金率（按金额）。当证券为期权时表示期权卖方最小保证金（[optionsMinMargin]）
 * @param shortMarginRatioByVolume 空头保证金（按手数），目前全部为 0.0。当证券为期权时表示期权卖方交易所最小保证金（[optionsExchangeMinMargin]）
 * @property optionsFixedMargin 期权卖方固定保证金（实际字段为 [longMarginRatioByMoney]）
 * @property optionsMinMargin 期权卖方最小保证金（实际字段为 [shortMarginRatioByMoney]），目前全部为 0.0
 * @property optionsExchangeFixedMargin 期权卖方交易所固定保证金（实际字段为 [longMarginRatioByVolume]）
 * @property optionsExchangeMinMargin 期权卖方交易所最小保证金（实际字段为 [shortMarginRatioByVolume]），目前全部为 0.0
 */
data class MarginRate(
    var code: String,
    var longMarginRatioByMoney: Double,
    var longMarginRatioByVolume: Double,
    var shortMarginRatioByMoney: Double,
    var shortMarginRatioByVolume: Double,
    override var extras: MutableMap<String, String>? = null,
) : ExtrasEntity {
    var optionsFixedMargin: Double by ::longMarginRatioByMoney
    var optionsMinMargin: Double by ::shortMarginRatioByMoney
    var optionsExchangeFixedMargin: Double by ::longMarginRatioByVolume
    var optionsExchangeMinMargin: Double by ::shortMarginRatioByVolume

    /**
     * 返回一份自身的完全拷贝
     */
    fun deepCopy(): MarginRate {
        return copy(extras = extras?.toMutableMap())
    }

    /**
     * 将保证金率处理后赋值给 [info] 的对应字段
     */
    fun copyFieldsToSecurityInfo(info: SecurityInfo) {
        when (info.type) {
            SecurityType.FUTURES -> {
                info.marginRateLong = longMarginRatioByMoney
                info.marginRateShort = shortMarginRatioByMoney
            }
            SecurityType.OPTIONS -> {
                info.optionsFixedMargin = optionsFixedMargin
                info.optionsMinMargin = optionsMinMargin
            }
            else -> Unit
        }
    }
}