package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.delay
import org.rationalityfrontline.jctp.CThostFtdcRspInfoField
import org.rationalityfrontline.ktrader.broker.api.Order
import org.rationalityfrontline.ktrader.broker.api.Position
import org.rationalityfrontline.ktrader.broker.api.Trade
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

internal data class QueryTradesData(
    val tradeId: String? = null,
    var code: String? = null,
    var orderSysId: String? = null,
    val results: MutableList<Trade> = mutableListOf()
)

internal data class QueryOrdersData(
    val orderId: String? = null,
    val code: String? = null,
    val onlyUnfinished: Boolean = false,
    val results: MutableList<Order> =  mutableListOf(),
)

/**
 * Order 的扩展字段，存储于 extras 中。格式为 exchangeId_orderSysId
 */
var Order.orderSysId: String
    get() = extras?.get("orderSysId") as String? ?: ""
    set(value) {
        if (extras == null) {
            extras = mutableMapOf()
        }
        extras!!["orderSysId"] = value
    }

/**
 * Order 的扩展字段，以 String 格式存储于 extras 中。标记该 order 是否计算过挂单费用（仅限中金所）
 */
var Order.insertFeeCalculated: Boolean
    get() = (extras?.get("insertFeeCalculated") as String? ?: "false").toBoolean()
    set(value) {
        if (extras == null) {
            extras = mutableMapOf()
        }
        extras!!["insertFeeCalculated"] = value.toString()
    }

/**
 * Order 的扩展字段，以 String 格式存储于 extras 中。标记该 order 是否计算过撤单费用（仅限中金所）
 */
var Order.cancelFeeCalculated: Boolean
    get() = (extras?.get("cancelFeeCalculated") as String? ?: "false").toBoolean()
    set(value) {
        if (extras == null) {
            extras = mutableMapOf()
        }
        extras!!["cancelFeeCalculated"] = value.toString()
    }

fun Order.deepCopy(): Order {
    return copy(extras = extras?.toMutableMap())
}

/**
 * 按挂单价从低到高的顺序插入 [order]
 */
internal fun MutableList<Order>.insert(order: Order) {
    var i = indexOfFirst { it.price >= order.price }
    i = if (i == -1) size else i
    add(i, order)
}

/**
 * 记录单一合约的双向持仓
 */
internal data class BiPosition(
    var long: Position? = null,
    var short: Position? = null,
)

/**
 * 交易所 ID
 */
object ExchangeID {
    const val SHFE = "SHFE"
    const val INE = "INE"
    const val CFFEX = "CFFEX"
    const val DCE = "DCE"
    const val CZCE = "CZCE"
}

/**
 * 期货保证金/期权权利金价格类型
 */
internal enum class MarginPriceType {
    /**
     * 昨结算价
     */
    PRE_SETTLEMENT_PRICE,
    /**
     * 最新价
     */
    LAST_PRICE,
    /**
     * 今日成交均价
     */
    TODAY_SETTLEMENT_PRICE,
    /**
     * 开仓价
     */
    OPEN_PRICE,
    /**
     * max(昨结算价, 最新价)
     */
    MAX_PRE_SETTLEMENT_PRICE_LAST_PRICE,
}