package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.jctp.CThostFtdcTraderApi

/**
 * 记录了 CtpBroker 的相关信息（初始化参数、额外参数等）
 */
object CtpBrokerInfo {

    /**
     * 交易接口名称
     */
    const val name: String = "CTP"

    /**
     * 交易接口版本
     */
    val version: String = CThostFtdcTraderApi.GetApiVersion()

    /**
     * 实例化 CtpBrokerApi 时所需的参数说明。Pair.first 为参数名，Pair.second 为参数说明。 例：Pair("password", "String 投资者资金账号的密码")
     */
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
        Pair("disableAutoSubscribe", "Boolean 是否禁止自动订阅持仓合约的行情（用于计算合约今仓保证金以及查询持仓时返回最新价及盈亏）"),
        Pair("disableFeeCalculation", "Boolean 是否禁止计算保证金及手续费（首次计算某个合约的费用时，可能会查询该合约的最新 Tick、保证金率、手续费率，造成额外开销，后续再次计算时则会使用上次查询的结果）"),
    )

    /**
     * CtpBrokerApi 成员方法的额外参数（extras: Map<String, Any>?）说明。Pair.first 为方法名，Pair.second 为额外参数说明。
     */
    val methodExtras: List<Pair<String, String>> = listOf(
        Pair("subscribeMarketData/unsubscribeMarketData/subscribeAllMarketData/unsubscribeAllMarketData", "[isForce: Boolean = false]【是否强制向交易所发送未更改的订阅请求（默认只发送未/已被订阅的标的的订阅请求）】"),
        Pair("querySecurity", "[queryFee: Boolean = false]【是否查询保证金率及手续费率，如果之前没查过，可能会耗时。当 useCache 为 false 时无效】"),
    )
}