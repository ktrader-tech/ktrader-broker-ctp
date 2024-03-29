package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.jctp.*
import org.rationalityfrontline.ktrader.datatype.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter

/**
 * 翻译器，用于将本地的 CTP 信息翻译为标准的 BrokerApi 信息
 */
@Suppress("MemberVisibilityCanBePrivate")
internal object Converter {

    private val THOST_FTDC_OF_Open_S = jctpConstants.THOST_FTDC_OF_Open.toString()
    private val THOST_FTDC_OF_Close_S = jctpConstants.THOST_FTDC_OF_Close.toString()
    private val THOST_FTDC_OF_CloseToday_S = jctpConstants.THOST_FTDC_OF_CloseToday.toString()
    private val THOST_FTDC_OF_CloseYesterday_S = jctpConstants.THOST_FTDC_OF_CloseYesterday.toString()
    val THOST_FTDC_HF_Speculation = jctpConstants.THOST_FTDC_HF_Speculation.toString()

    fun dateC2A(date: String): LocalDate {
        return LocalDate.parse("${date.slice(0..3)}-${date.slice(4..5)}-${date.slice(6..7)}")
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

    fun offsetC2A(offset: String): OrderOffset {
        return when (offset) {
            THOST_FTDC_OF_Open_S -> OrderOffset.OPEN
            THOST_FTDC_OF_Close_S -> OrderOffset.CLOSE
            THOST_FTDC_OF_CloseToday_S -> OrderOffset.CLOSE_TODAY
            THOST_FTDC_OF_CloseYesterday_S -> OrderOffset.CLOSE_YESTERDAY
            else -> OrderOffset.UNKNOWN
        }
    }

    fun tickC2A(code: String, tickField: CThostFtdcDepthMarketDataField, lastTick: Tick? = null, volumeMultiple: Int? = null, marketStatus: MarketStatus = MarketStatus.UNKNOWN, onTimeParseError: (Exception) -> Unit): Tick {
        val updateTime = try {
            LocalTime.parse("${tickField.updateTime}.${tickField.updateMillisec}").atDate(LocalDate.now())
        } catch (e: Exception) {
            onTimeParseError(e)
            LocalDateTime.now()
        }
        val lastPrice = formatDouble(tickField.lastPrice)
        val bidPrice = arrayOf(formatDouble(tickField.bidPrice1), formatDouble(tickField.bidPrice2), formatDouble(tickField.bidPrice3), formatDouble(tickField.bidPrice4), formatDouble(tickField.bidPrice5))
        val askPrice = arrayOf(formatDouble(tickField.askPrice1), formatDouble(tickField.askPrice2), formatDouble(tickField.askPrice3), formatDouble(tickField.askPrice4), formatDouble(tickField.askPrice5))
        return Tick(
            code = code,
            time = updateTime,
            lastPrice = lastPrice,
            bidPrice = bidPrice,
            askPrice = askPrice,
            bidVolume = arrayOf(tickField.bidVolume1, tickField.bidVolume2, tickField.bidVolume3, tickField.bidVolume4, tickField.bidVolume5),
            askVolume = arrayOf(tickField.askVolume1, tickField.askVolume2, tickField.askVolume3, tickField.askVolume4, tickField.askVolume5),
            volume = tickField.volume - (lastTick?.todayVolume ?: tickField.volume),
            turnover = tickField.turnover - (lastTick?.todayTurnover ?: tickField.turnover),
            openInterestDelta = tickField.openInterest.toInt() - (lastTick?.todayOpenInterest ?: tickField.openInterest.toInt()),
            direction = if (lastTick == null) calculateTickDirection(lastPrice, bidPrice[0], askPrice[0]) else calculateTickDirection(lastPrice, lastTick.bidPrice[0], lastTick.askPrice[0]),
            status = marketStatus,
            preClosePrice = formatDouble(tickField.preClosePrice),
            preSettlementPrice = formatDouble(tickField.preSettlementPrice),
            preOpenInterest = tickField.preOpenInterest.toInt(),
            todayOpenPrice = formatDouble(tickField.openPrice),
            todayClosePrice = formatDouble(tickField.closePrice),
            todayHighPrice = formatDouble(tickField.highestPrice),
            todayLowPrice = formatDouble(tickField.lowestPrice),
            todayHighLimitPrice = formatDouble(tickField.upperLimitPrice),
            todayLowLimitPrice = formatDouble(tickField.lowerLimitPrice),
            todayAvgPrice = if (volumeMultiple == null || volumeMultiple == 0 || tickField.volume == 0) 0.0 else tickField.turnover / (volumeMultiple * tickField.volume),
            todayVolume = tickField.volume,
            todayTurnover = formatDouble(tickField.turnover),
            todaySettlementPrice = formatDouble(tickField.settlementPrice),
            todayOpenInterest = tickField.openInterest.toInt(),
        )
    }

    /**
     * 行情推送的 [Tick] 中很多字段可能是无效值，CTP 内用 [Double.MAX_VALUE] 表示，在此需要统一为 0.0
     */
    @Suppress("NOTHING_TO_INLINE")
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

    fun securityC2A(insField: CThostFtdcInstrumentField, onTimeParseError: (Exception) -> Unit): SecurityInfo? {
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
                    type = type,
                    productId = insField.productID,
                    name = insField.instrumentName,
                    priceTick = insField.priceTick,
                    volumeMultiple = insField.volumeMultiple,
                    isTrading = insField.isTrading != 0,
                    openDate = LocalDate.parse(insField.openDate, DateTimeFormatter.BASIC_ISO_DATE),
                    expireDate = LocalDate.parse(insField.expireDate, DateTimeFormatter.BASIC_ISO_DATE),
                    endDeliveryDate = LocalDate.parse(insField.endDelivDate, DateTimeFormatter.BASIC_ISO_DATE),
                    isUseMaxMarginSideAlgorithm = insField.maxMarginSideAlgorithm == jctpConstants.THOST_FTDC_MMSA_YES,
                    optionsType = when (insField.optionsType) {
                        jctpConstants.THOST_FTDC_CP_CallOptions -> OptionsType.CALL
                        jctpConstants.THOST_FTDC_CP_PutOptions -> OptionsType.PUT
                        else -> OptionsType.UNKNOWN
                    },
                    optionsUnderlyingCode = "${insField.exchangeID}.${insField.underlyingInstrID}",
                    optionsStrikePrice = formatDouble(insField.strikePrice)
                )
            }
        } catch (e: Exception) {
            onTimeParseError(e)
            null
        }
    }

    fun orderC2A(orderField: CThostFtdcOrderField, volumeMultiple: Int, onTimeParseError: (Exception) -> Unit): Order {
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
            val date = orderField.insertDate
            LocalDateTime.parse("${date.slice(0..3)}-${date.slice(4..5)}-${date.slice(6..7)}T${orderField.insertTime}")
        } catch (e: Exception) {
            onTimeParseError(e)
            LocalDateTime.now()
        }
        val updateTime = if (orderStatus == OrderStatus.CANCELED) {
            try {
                LocalTime.parse(orderField.cancelTime).atDate(LocalDate.now())
            } catch (e: Exception) {
                onTimeParseError(e)
                LocalDateTime.now()
            }
        } else createTime
        return Order(
            orderField.investorID,
            orderId, "${orderField.exchangeID}.${orderField.instrumentID}",
            orderField.limitPrice, null,  orderField.volumeTotalOriginal, orderField.minVolume, directionC2A(orderField.direction),
            offsetC2A(orderField.combOffsetFlag), orderType, orderStatus, orderField.statusMsg,
            orderField.volumeTraded, orderField.limitPrice * orderField.volumeTraded * volumeMultiple, orderField.limitPrice, 0.0, 0.0,
            createTime, updateTime, extras = mutableMapOf()
        ).apply {
            if (orderField.orderSysID.isNotEmpty()) orderSysId = "${orderField.exchangeID}_${orderField.orderSysID}"
        }
    }

    fun tradeC2A(tradeField: CThostFtdcTradeField, orderId: String, onTimeParseError: (Exception) -> Unit): Trade {
        val tradeTime = try {
            val date = tradeField.tradeDate
            val updateTimeStr = "${date.slice(0..3)}-${date.slice(4..5)}-${date.slice(6..7)}T${tradeField.tradeTime}"
            LocalDateTime.parse(updateTimeStr)
        } catch (e: Exception) {
            onTimeParseError(e)
            LocalDateTime.now()
        }
        return Trade(
            accountId = tradeField.investorID,
            tradeId = "${tradeField.tradeID}_${tradeField.orderRef}",
            orderId = orderId,
            code = "${tradeField.exchangeID}.${tradeField.instrumentID}",
            price = tradeField.price,
            volume = tradeField.volume,
            turnover = 0.0,
            direction = directionC2A(tradeField.direction),
            offset = offsetC2A(tradeField.offsetFlag.toString()),
            commission = 0.0,
            time = tradeTime
        )
    }

    fun positionC2A(tradingDay: LocalDate, positionField: CThostFtdcInvestorPositionField): Position {
        val direction = directionC2A(positionField.posiDirection)
        val frozenVolume =  when (direction) {
            Direction.LONG -> positionField.shortFrozen
            Direction.SHORT -> positionField.longFrozen
            else -> 0
        }
        return Position(
            accountId = positionField.investorID,
            tradingDay = tradingDay,
            code = "${positionField.exchangeID}.${positionField.instrumentID}",
            direction = direction,
            preVolume = positionField.ydPosition,
            volume = positionField.position,
            value = positionField.useMargin,
            todayVolume = positionField.todayPosition,
            frozenVolume = frozenVolume,
            frozenTodayVolume = 0,
            todayOpenVolume = positionField.openVolume,
            todayCloseVolume = positionField.closeVolume,
            todayCommission = positionField.commission,
            openCost = positionField.openCost,
            avgOpenPrice = 0.0,
            lastPrice = 0.0,
            pnl = 0.0,
        )
    }

    fun assetsC2A(tradingDay: LocalDate, assetsField: CThostFtdcTradingAccountField): Assets {
        return Assets(
            accountId = assetsField.accountID,
            tradingDay = tradingDay,
            total = assetsField.balance,
            available = assetsField.available,
            positionValue = assetsField.currMargin,
            frozenByOrder = assetsField.frozenCash,
            todayCommission = assetsField.commission,
            initialCash = 0.0,
            totalClosePnl = 0.0,
            totalCommission = 0.0,
            positionPnl = 0.0,
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