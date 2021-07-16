package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.delay
import org.rationalityfrontline.jctp.CThostFtdcDepthMarketDataField
import org.rationalityfrontline.jctp.CThostFtdcRspInfoField
import org.rationalityfrontline.jctp.jctpConstants
import org.rationalityfrontline.jctp.jctpConstants.*
import org.rationalityfrontline.ktrader.broker.api.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
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
suspend fun <T> runWithRetry(action: suspend () -> T, onError: (Exception) -> T = { e -> throw e }): T {
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
data class RequestContinuation(
    val requestId: Int,
    val continuation: Continuation<*>,
    val tag: String = "",
    val data: Any = Unit,
)

/**
 * 翻译器，用于将本地的 CTP 信息翻译为标准的 BrokerApi 信息
 */
object Translator {

    private val THOST_FTDC_OF_Open_S = THOST_FTDC_OF_Open.toString()
    private val THOST_FTDC_OF_Close_S = THOST_FTDC_OF_Close.toString()
    private val THOST_FTDC_OF_CloseToday_S = THOST_FTDC_OF_CloseToday.toString()
    private val THOST_FTDC_OF_CloseYesterday_S = THOST_FTDC_OF_CloseYesterday.toString()
    val THOST_FTDC_HF_Speculation = jctpConstants.THOST_FTDC_HF_Speculation.toString()

    fun directionA2C(direction: Direction): Char {
        return when (direction) {
            Direction.LONG -> THOST_FTDC_D_Buy
            Direction.SHORT -> THOST_FTDC_D_Sell
            Direction.UNKNOWN -> throw IllegalArgumentException("不允许输入 UNKNOWN")
        }
    }

    fun directionC2A(direction: Char): Direction {
        return when (direction) {
            THOST_FTDC_D_Buy -> Direction.LONG
            THOST_FTDC_D_Sell -> Direction.SHORT
            THOST_FTDC_PD_Long -> Direction.LONG
            THOST_FTDC_PD_Short -> Direction.SHORT
            else -> Direction.UNKNOWN
        }
    }

    fun offsetA2C(offset: OrderOffset): String {
        return when (offset) {
            OrderOffset.OPEN -> THOST_FTDC_OF_Open_S
            OrderOffset.CLOSE -> THOST_FTDC_OF_Close_S
            OrderOffset.CLOSE_TODAY -> THOST_FTDC_OF_CloseToday_S
            OrderOffset.CLOSE_YESTERDAY -> THOST_FTDC_OF_CloseYesterday_S
            OrderOffset.UNKNOWN -> throw IllegalArgumentException("不允许输入 UNKNOWN")
        }
    }

    fun offsetL2C(offset: String): OrderOffset {
        return when (offset) {
            THOST_FTDC_OF_Open_S -> OrderOffset.OPEN
            THOST_FTDC_OF_Close_S -> OrderOffset.CLOSE
            THOST_FTDC_OF_CloseToday_S -> OrderOffset.CLOSE_TODAY
            THOST_FTDC_OF_CloseYesterday_S -> OrderOffset.CLOSE_YESTERDAY
            else -> OrderOffset.UNKNOWN
        }
    }

    fun tickC2A(code: String, data: CThostFtdcDepthMarketDataField, lastTick: Tick? = null, volumeMultiple: Int? = null, marketStatus: MarketStatus = MarketStatus.UNKNOWN, onTimeParseError: (Exception) -> Unit): Tick {
        val updateTime = try {
            LocalTime.parse("${data.updateTime}.${data.updateMillisec}").atDate(LocalDate.now())
        } catch (e: Exception) {
            onTimeParseError(e)
            LocalDateTime.now()
        }
        val lastPrice = formatDouble(data.lastPrice)
        val bidPrice = arrayOf(formatDouble(data.bidPrice1), formatDouble(data.bidPrice2), formatDouble(data.bidPrice3), formatDouble(data.bidPrice4), formatDouble(data.bidPrice5))
        val askPrice = arrayOf(formatDouble(data.askPrice1), formatDouble(data.askPrice2), formatDouble(data.askPrice3), formatDouble(data.askPrice4), formatDouble(data.askPrice5))
        return Tick(
            code = code,
            time = updateTime,
            lastPrice = lastPrice,
            bidPrice = bidPrice,
            askPrice = askPrice,
            bidVolume = arrayOf(data.bidVolume1, data.bidVolume2, data.bidVolume3, data.bidVolume4, data.bidVolume5),
            askVolume = arrayOf(data.askVolume1, data.askVolume2, data.askVolume3, data.askVolume4, data.askVolume5),
            volume = data.volume - (lastTick?.todayVolume ?: data.volume),
            turnover = data.turnover - (lastTick?.todayTurnover ?: data.turnover),
            openInterest = data.openInterest.toInt() - (lastTick?.todayOpenInterest ?: data.openInterest.toInt()),
            direction = if (lastTick == null) calculateTickDirection(lastPrice, bidPrice[0], askPrice[0]) else calculateTickDirection(lastPrice, lastTick.bidPrice[0], lastTick.askPrice[0]),
            status = marketStatus,
            yesterdayClose = formatDouble(data.preClosePrice),
            yesterdaySettlementPrice = formatDouble(data.preSettlementPrice),
            yesterdayOpenInterest = data.preOpenInterest.toInt(),
            todayOpenPrice = formatDouble(data.openPrice),
            todayClosePrice = formatDouble(data.closePrice),
            todayHighPrice = formatDouble(data.highestPrice),
            todayLowPrice = formatDouble(data.lowestPrice),
            todayHighLimitPrice = formatDouble(data.upperLimitPrice),
            todayLowLimitPrice = formatDouble(data.lowerLimitPrice),
            todayAvgPrice = if (volumeMultiple == null || volumeMultiple == 0 || data.volume == 0) 0.0 else data.turnover / (volumeMultiple * data.volume),
            todayVolume = data.volume,
            todayTurnover = formatDouble(data.turnover),
            todaySettlementPrice = formatDouble(data.settlementPrice),
            todayOpenInterest = data.openInterest.toInt(),
        )
    }

    /**
     * 行情推送的 [Tick] 中很多字段可能是无效值，CTP 内用 [Double.MAX_VALUE] 表示，在此需要统一为 0.0
     */
    private inline fun formatDouble(input: Double): Double {
        return if (input == Double.MAX_VALUE) 0.0 else input
    }

    /**
     * 计算 Tick 方向
     */
    private fun calculateTickDirection(lastPrice: Double, bid1Price: Double, ask1Price: Double): TickDirection {
        return when {
            lastPrice >= ask1Price -> TickDirection.UP
            lastPrice <= bid1Price -> TickDirection.DOWN
            else -> TickDirection.STAY
        }
    }
}

/**
 * Order 的扩展字段，存储于 extras 中。格式为 exchangeId_orderSysId
 */
var Order.orderSysId: String
    get() = extras?.get("orderSysId") as String? ?: ""
    set(value) {
        if (extras != null) {
            extras!!["orderSysId"] = value
        }
    }