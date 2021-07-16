package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.jctp.CThostFtdcTraderApi

object CtpBrokerInfo {
    val name: String = "CTP"
    val version: String = CThostFtdcTraderApi.GetApiVersion()
    val configKeys: List<Pair<String, String>> = listOf(
        Pair("mdFronts", "List<String> 行情前置"),
        Pair("tdFronts", "List<String> 交易前置"),
        Pair("investorId", "String 投资者资金账号"),
        Pair("password", "String 投资者资金账号的密码"),
        Pair("brokerId", "String 经纪商ID"),
        Pair("appId", "String 交易终端软件的标识码"),
        Pair("authCode", "String 交易终端软件的授权码"),
        Pair("userProductInfo", "String 交易终端软件的产品信息"),
        Pair("cachePath", "String 存贮订阅信息文件等临时文件的目录"),
        Pair("flowSubscribeType", "String 交易API的私有流及公有流的订阅模式，共3种：RESTART、RESUME、QUICK，默认为 RESUME")
    )
    val methodExtras: List<Pair<String, String>> = listOf(
        Pair("subscribeMarketData/unsubscribeMarketData/subscribeAllMarketData/unsubscribeAllMarketData", "[isForce: Boolean = false]【是否强制向交易所发送未更改的订阅请求（默认只发送未/已被订阅的标的的订阅请求）】"),
        Pair("insertOrder", "[minVolume: Int]【最小成交量。仅当下单类型为 OrderType.FAK 时生效】")
    )

    fun parseConfig(config: Map<String, Any>): CtpConfig {
        return CtpConfig(
            mdFronts = config["mdFronts"] as List<String>? ?: listOf(),
            tdFronts = config["tdFronts"] as List<String>? ?: listOf(),
            investorId = config["investorId"] as String? ?: "",
            password = config["password"] as String? ?: "",
            brokerId = config["brokerId"] as String? ?: "",
            appId = config["appId"] as String? ?: "",
            authCode = config["authCode"] as String? ?: "",
            userProductInfo = config["userProductInfo"] as String? ?: "",
            cachePath = config["cachePath"] as String? ?: "",
            flowSubscribeType = config["flowSubscribeType"] as String? ?: "",
        )
    }
}

data class CtpConfig(
    val mdFronts: List<String>,
    val tdFronts: List<String>,
    val investorId: String,
    val password: String,
    val brokerId: String,
    val appId: String,
    val authCode: String,
    val userProductInfo: String,
    val cachePath: String,
    val flowSubscribeType: String,
)