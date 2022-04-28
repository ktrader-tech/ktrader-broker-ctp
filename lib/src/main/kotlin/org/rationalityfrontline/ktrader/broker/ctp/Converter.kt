package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.jctp.*
import org.rationalityfrontline.ktrader.api.datatype.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter

/**
 * 翻译器，用于将本地的 CTP 信息翻译为标准的 BrokerApi 信息
 */
internal object Converter {

    private val THOST_FTDC_OF_Open_S = jctpConstants.THOST_FTDC_OF_Open.toString()
    private val THOST_FTDC_OF_Close_S = jctpConstants.THOST_FTDC_OF_Close.toString()
    private val THOST_FTDC_OF_CloseToday_S = jctpConstants.THOST_FTDC_OF_CloseToday.toString()
    private val THOST_FTDC_OF_CloseYesterday_S = jctpConstants.THOST_FTDC_OF_CloseYesterday.toString()
    val THOST_FTDC_HF_Speculation = jctpConstants.THOST_FTDC_HF_Speculation.toString()

    fun dateC2A(date: String): LocalDate {
        return LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE)
    }

    fun timeC2A(time: String, tradingDay: LocalDate, lastTradingDay: LocalDate): LocalDateTime {
        val t = LocalTime.parse(time)
        return t.atDate(if (t.hour > 19) lastTradingDay else tradingDay)
    }

    fun marginPriceTypeC2A(type: Char): MarginPriceType {
        return when (type) {
            jctpConstants.THOST_FTDC_MPT_PreSettlementPrice -> MarginPriceType.PRE_SETTLEMENT_PRICE
            jctpConstants.THOST_FTDC_MPT_SettlementPrice -> MarginPriceType.LAST_PRICE
            jctpConstants.THOST_FTDC_MPT_AveragePrice -> MarginPriceType.TODAY_SETTLEMENT_PRICE
            jctpConstants.THOST_FTDC_MPT_OpenPrice -> MarginPriceType.OPEN_PRICE
            jctpConstants.THOST_FTDC_ORPT_MaxPreSettlementPrice -> MarginPriceType.MAX_PRE_SETTLEMENT_PRICE_LAST_PRICE
            else -> MarginPriceType.PRE_SETTLEMENT_PRICE
        }
    }

    fun directionA2C(direction: Direction): Char {
        return when (direction) {
            Direction.LONG -> jctpConstants.THOST_FTDC_D_Buy
            Direction.SHORT -> jctpConstants.THOST_FTDC_D_Sell
            Direction.UNKNOWN -> throw IllegalArgumentException("不允许输入 UNKNOWN")
        }
    }

    fun directionC2A(direction: Char): Direction {
        return when (direction) {
            jctpConstants.THOST_FTDC_D_Buy -> Direction.LONG
            jctpConstants.THOST_FTDC_D_Sell -> Direction.SHORT
            jctpConstants.THOST_FTDC_PD_Long -> Direction.LONG
            jctpConstants.THOST_FTDC_PD_Short -> Direction.SHORT
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

    @Suppress("MemberVisibilityCanBePrivate")
    fun offsetC2A(offset: String): OrderOffset {
        return when (offset) {
            THOST_FTDC_OF_Open_S -> OrderOffset.OPEN
            THOST_FTDC_OF_Close_S -> OrderOffset.CLOSE
            THOST_FTDC_OF_CloseToday_S -> OrderOffset.CLOSE_TODAY
            THOST_FTDC_OF_CloseYesterday_S -> OrderOffset.CLOSE_YESTERDAY
            else -> OrderOffset.UNKNOWN
        }
    }

    fun tickC2A(
        code: String,
        tradingDay: LocalDate,
        tickField: CThostFtdcDepthMarketDataField,
        lastTick: Tick? = null,
        info: SecurityInfo? = null,
        marketStatus: MarketStatus = MarketStatus.UNKNOWN,
        onTimeParseError: (Exception) -> Unit,
    ): Tick {
        val updateTime = try {
            LocalTime.parse("${tickField.updateTime}.${tickField.updateMillisec}").atDate(LocalDate.now())
        } catch (e: Exception) {
            if ("${tickField.updateTime}.${tickField.updateMillisec}" != ".0") {  // 当 CtpTdApi 在夜市时段查询股指期权时出现的特殊情况
                onTimeParseError(e)
            }
            LocalDateTime.now()
        }
        val lastPrice = formatDouble(tickField.lastPrice)
        val bidPrice = doubleArrayOf(formatDouble(tickField.bidPrice1), formatDouble(tickField.bidPrice2), formatDouble(tickField.bidPrice3), formatDouble(tickField.bidPrice4), formatDouble(tickField.bidPrice5))
        val askPrice = doubleArrayOf(formatDouble(tickField.askPrice1), formatDouble(tickField.askPrice2), formatDouble(tickField.askPrice3), formatDouble(tickField.askPrice4), formatDouble(tickField.askPrice5))
        val volumeMultiple = info?.volumeMultiple ?: 1
        if (code.startsWith("CZCE")) {
            tickField.turnover *= volumeMultiple
        }
        return Tick(
            code = code,
            tradingDay = tradingDay,
            time = updateTime,
            status = marketStatus,
            price = lastPrice,
            prePrice = lastTick?.price ?: lastPrice,
            bidPrice = bidPrice,
            askPrice = askPrice,
            bidVolume = intArrayOf(tickField.bidVolume1, tickField.bidVolume2, tickField.bidVolume3, tickField.bidVolume4, tickField.bidVolume5),
            askVolume = intArrayOf(tickField.askVolume1, tickField.askVolume2, tickField.askVolume3, tickField.askVolume4, tickField.askVolume5),
            volume = tickField.volume - (lastTick?.todayVolume ?: tickField.volume),
            turnover = tickField.turnover - (lastTick?.todayTurnover ?: tickField.turnover),
            openInterestDelta = tickField.openInterest.toInt() - (lastTick?.todayOpenInterest ?: tickField.openInterest.toInt()),
            todayOpenPrice = formatDouble(tickField.openPrice),
            todayHighPrice = formatDouble(tickField.highestPrice),
            todayLowPrice = formatDouble(tickField.lowestPrice),
            todayVolume = tickField.volume,
            todayTurnover = formatDouble(tickField.turnover),
            todayOpenInterest = tickField.openInterest.toInt(),
            info = info,
        ).apply {
            if (info?.type == SecurityType.OPTIONS) {
                optionsDelta = tickField.currDelta
            }
        }
    }

    /**
     * 行情推送的 [Tick] 中很多字段可能是无效值，CTP 内用 [Double.MAX_VALUE] 表示，在此需要统一为 0.0
     */
    @Suppress("NOTHING_TO_INLINE")
    inline fun formatDouble(input: Double): Double {
        return if (input == Double.MAX_VALUE) 0.0 else input
    }

    fun securityC2A(tradingDay: LocalDate, insField: CThostFtdcInstrumentField, onTimeParseError: (Exception) -> Unit): SecurityInfo? {
        return try {
            val type = when (insField.productClass) {
                jctpConstants.THOST_FTDC_PC_Futures -> SecurityType.FUTURES
                jctpConstants.THOST_FTDC_PC_Options,
                jctpConstants.THOST_FTDC_PC_SpotOption, -> SecurityType.OPTIONS
                else -> SecurityType.UNKNOWN
            }
            if (type == SecurityType.UNKNOWN) null else {
                SecurityInfo(
                    code = "${insField.exchangeID}.${insField.instrumentID}",
                    tradingDay = tradingDay,
                    name = insField.instrumentName,
                    type = type,
                    productId = insField.productID,
                    volumeMultiple = insField.volumeMultiple,
                    priceTick = insField.priceTick,
                    isTrading = insField.isTrading != 0,
                    startDate = LocalDate.parse(insField.openDate, DateTimeFormatter.BASIC_ISO_DATE),
                    endDate = LocalDate.parse(insField.expireDate, DateTimeFormatter.BASIC_ISO_DATE),
                    endDeliveryDate = LocalDate.parse(insField.endDelivDate, DateTimeFormatter.BASIC_ISO_DATE),
                    onlyMaxMarginSide = insField.maxMarginSideAlgorithm == jctpConstants.THOST_FTDC_MMSA_YES,
                ).apply {
                    if (type == SecurityType.OPTIONS) {
                        optionsType = when (insField.optionsType) {
                            jctpConstants.THOST_FTDC_CP_CallOptions -> OptionsType.CALL
                            jctpConstants.THOST_FTDC_CP_PutOptions -> OptionsType.PUT
                            else -> OptionsType.UNKNOWN
                        }
                        optionsStrikeMode = OptionsStrikeMode.AMERICAN
                        optionsUnderlyingCode = "${insField.exchangeID}.${insField.underlyingInstrID}"
                        optionsStrikePrice = formatDouble(insField.strikePrice)
                    }
                }
            }
        } catch (e: Exception) {
            onTimeParseError(e)
            null
        }
    }

    fun orderC2A(
        tradingDay: LocalDate,
        lastTradingDay: LocalDate,
        orderField: CThostFtdcOrderField,
        info: SecurityInfo?,
        onTimeParseError: (Exception) -> Unit
    ): Order {
        val orderId = "${orderField.frontID}_${orderField.sessionID}_${orderField.orderRef}"
        val orderType = when (orderField.orderPriceType) {
            jctpConstants.THOST_FTDC_OPT_LimitPrice -> when (orderField.timeCondition) {
                jctpConstants.THOST_FTDC_TC_GFD -> OrderType.LIMIT
                jctpConstants.THOST_FTDC_TC_IOC -> when (orderField.volumeCondition) {
                    jctpConstants.THOST_FTDC_VC_CV -> OrderType.FOK
                    else -> OrderType.FAK
                }
                else -> OrderType.UNKNOWN
            }
            jctpConstants.THOST_FTDC_OPT_AnyPrice -> OrderType.MARKET
            else -> OrderType.UNKNOWN
        }
        val orderStatus = when (orderField.orderSubmitStatus) {
            jctpConstants.THOST_FTDC_OSS_InsertRejected -> OrderStatus.ERROR
            else -> when (orderField.orderStatus) {
                jctpConstants.THOST_FTDC_OST_Unknown -> OrderStatus.SUBMITTING
                jctpConstants.THOST_FTDC_OST_NoTradeQueueing -> OrderStatus.ACCEPTED
                jctpConstants.THOST_FTDC_OST_PartTradedQueueing -> OrderStatus.PARTIALLY_FILLED
                jctpConstants.THOST_FTDC_OST_AllTraded -> OrderStatus.FILLED
                jctpConstants.THOST_FTDC_OST_Canceled -> OrderStatus.CANCELED
                else -> OrderStatus.UNKNOWN
            }
        }
        val createTime = try {
            timeC2A(orderField.insertTime, tradingDay, lastTradingDay)
        } catch (e: Exception) {
            onTimeParseError(e)
            LocalDateTime.now()
        }
        val updateTime = if (orderStatus == OrderStatus.CANCELED) {
            try {
                timeC2A(orderField.cancelTime, tradingDay, lastTradingDay)
            } catch (e: Exception) {
                onTimeParseError(e)
                LocalDateTime.now()
            }
        } else createTime
        val code = "${orderField.exchangeID}.${orderField.instrumentID}"
        return Order(
            orderField.investorID, orderId, tradingDay, code, info?.name ?: code,
            orderField.limitPrice, 0.0,  orderField.volumeTotalOriginal, orderField.minVolume, directionC2A(orderField.direction),
            offsetC2A(orderField.combOffsetFlag), orderType, orderStatus, orderField.statusMsg,
            orderField.volumeTraded, 0.0, 0.0, 0.0, 0.0,
            createTime, updateTime, extras = mutableMapOf()
        ).apply {
            if (orderField.orderSysID.isNotEmpty()) orderSysId = "${orderField.exchangeID}_${orderField.orderSysID}"
        }
    }

    fun tradeC2A(
        tradingDay: LocalDate,
        lastTradingDay: LocalDate,
        tradeField: CThostFtdcTradeField,
        orderId: String,
        closePositionPrice: Double,
        name: String,
        onTimeParseError: (Exception) -> Unit
    ): Trade {
        val tradeTime = try {
            timeC2A(tradeField.tradeTime, tradingDay, lastTradingDay)
        } catch (e: Exception) {
            onTimeParseError(e)
            LocalDateTime.now()
        }
        return Trade(
            accountId = tradeField.investorID,
            tradeId = "${tradeField.tradeID}_${tradeField.orderRef}",
            orderId = orderId,
            tradingDay = tradingDay,
            code = "${tradeField.exchangeID}.${tradeField.instrumentID}",
            name = name,
            price = tradeField.price,
            closePositionPrice = closePositionPrice,
            volume = tradeField.volume,
            turnover = 0.0,
            direction = directionC2A(tradeField.direction),
            offset = offsetC2A(tradeField.offsetFlag.toString()),
            commission = 0.0,
            time = tradeTime
        )
    }

    fun positionC2A(tradingDay: LocalDate, positionField: CThostFtdcInvestorPositionField, info: SecurityInfo? = null): Position {
        val direction = directionC2A(positionField.posiDirection)
        val frozenVolume =  when (direction) {
            Direction.LONG -> positionField.shortFrozen
            Direction.SHORT -> positionField.longFrozen
            else -> 0
        }
        val code = "${positionField.exchangeID}.${positionField.instrumentID}"
        return Position(
            accountId = positionField.investorID,
            tradingDay = tradingDay,
            code = code,
            direction = direction,
            preVolume = positionField.ydPosition,
            volume = positionField.position,
            todayVolume = positionField.todayPosition,
            frozenVolume = frozenVolume,
            todayOpenVolume = positionField.openVolume,
            todayCloseVolume = positionField.closeVolume,
            todayCommission = positionField.commission,
            openCost = positionField.openCost,
            price = info?.preClosePrice ?: 0.0,
            value = positionField.useMargin,
        ).apply {
            info?.copyFieldsToPosition(this)
        }
    }

    fun assetsC2A(
        tradingDay: LocalDate,
        assetsField: CThostFtdcTradingAccountField,
        futuresMarginPriceType: MarginPriceType,
        optionsMarginPriceType: MarginPriceType,
    ): Assets {
        return Assets(
            accountId = assetsField.accountID,
            tradingDay = tradingDay,
            total = assetsField.balance,
            available = assetsField.available,
            positionValue = assetsField.currMargin,
            frozenByOrder = assetsField.frozenCash,
            todayCommission = assetsField.commission,
            yesterdayTotal = assetsField.preBalance,
            futuresMarginPriceType = futuresMarginPriceType,
            optionsMarginPriceType = optionsMarginPriceType,
        )
    }

    fun futuresCommissionRateC2A(crField: CThostFtdcInstrumentCommissionRateField, code: String): CommissionRate {
        return CommissionRate(
            code = code,
            openRatioByMoney = crField.openRatioByMoney,
            openRatioByVolume = crField.openRatioByVolume,
            closeRatioByMoney = crField.closeRatioByMoney,
            closeRatioByVolume = crField.closeRatioByVolume,
            closeTodayRatioByMoney = crField.closeTodayRatioByMoney,
            closeTodayRatioByVolume = crField.closeTodayRatioByVolume,
        )
    }

    fun optionsCommissionRateC2A(crField: CThostFtdcOptionInstrCommRateField): CommissionRate {
        return CommissionRate(
            code = crField.instrumentID,
            openRatioByMoney = crField.openRatioByMoney,
            openRatioByVolume = crField.openRatioByVolume,
            closeRatioByMoney = crField.closeRatioByMoney,
            closeRatioByVolume = crField.closeRatioByVolume,
            closeTodayRatioByMoney = crField.closeTodayRatioByMoney,
            closeTodayRatioByVolume = crField.closeTodayRatioByVolume,
            optionsStrikeRatioByMoney = crField.strikeRatioByMoney,
            optionsStrikeRatioByVolume = crField.strikeRatioByVolume,
        )
    }

    fun futuresMarginRateC2A(mrField: CThostFtdcInstrumentMarginRateField, code: String): MarginRate {
        return MarginRate(
            code = code,
            longMarginRatioByMoney = mrField.longMarginRatioByMoney,
            longMarginRatioByVolume = mrField.longMarginRatioByVolume,
            shortMarginRatioByMoney = mrField.shortMarginRatioByMoney,
            shortMarginRatioByVolume = mrField.shortMarginRatioByVolume,
        )
    }

    fun optionsMarginC2A(mrField: CThostFtdcOptionInstrTradeCostField, code: String): MarginRate {
        return MarginRate(
            code = code,
            longMarginRatioByMoney = mrField.fixedMargin,
            longMarginRatioByVolume = mrField.exchFixedMargin,
            shortMarginRatioByMoney = mrField.miniMargin,
            shortMarginRatioByVolume = mrField.exchMiniMargin,
        )
    }
}