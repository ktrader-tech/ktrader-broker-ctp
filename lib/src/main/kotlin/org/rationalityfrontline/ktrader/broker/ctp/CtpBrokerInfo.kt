package org.rationalityfrontline.ktrader.broker.ctp

import org.rationalityfrontline.jctp.CThostFtdcTraderApi
import org.rationalityfrontline.ktrader.api.ApiInfo

/**
 * CtpBroker 的描述信息
 */
object CtpBrokerInfo: ApiInfo {

    override val name: String = "CTP"

    override val version: String = CThostFtdcTraderApi.GetApiVersion()

    override val author: String = "RationalityFrontline"

    override val description: String = """
        KTrader-API 中 Broker 接口的 CTP 实现。
        详情参见 https://github.com/ktrader-tech/ktrader-broker-ctp
        
        功能特性：
        * 利用 Kotlin 协程 将 CTP 的异步接口封装为同步调用方式，降低心智负担，提升开发效率
        * 内置自成交风控，存在自成交风险的下单请求会本地拒单
        * 内置撤单数量风控，单合约日内撤单数达到 499 次后会本地拒绝该合约的后续撤单请求
        * 内置 CTP 流控处理，调用层无需关注任何 CTP 流控信息
        * 内置维护本地持仓、订单、成交、Tick 缓存，让查询请求快速返回，不受流控阻塞
        * 支持期货及期权的交易（目前尚不支持期权行权及自对冲，仅支持期权交易）
        * 自动查询账户真实的手续费率（包括中金所申报手续费）与保证金率，并计算持仓、订单、成交相关的手续费、保证金、冻结资金（期权也支持）
        * 封装提供了一些 CTP 原生不支持的功能，如查询当前已订阅行情，Tick 中带有合约交易状态及 Tick 内成交量成交额等
        * 网络断开重连时会自动订阅原先已订阅的行情，不用手动重新订阅
        * 支持 7x24 小时不间断运行
        
        支持的额外参数：
        querySecurity/querySecurities/queryAllSecurities/queryOptions：[ensureFullInfo: Boolean = false]【是否确保信息完整（保证金费率、手续费率、当日价格信息（涨跌停价、昨收昨结昨仓）），如果之前没查过，会比较耗时。当 useCache 为 false 时无效】
    """.trimIndent()

    /**
     * 实例化 CtpBrokerApi 时所需的参数说明。Pair.first 为参数名，Pair.second 为参数说明。 例：Pair("password", "String 投资者资金账号的密码")
     */
    val configKeys: List<Pair<String, String>> = mutableListOf(
        Pair("InvestorID", "String 投资者资金账号"),
        Pair("Password", "String 投资者资金账号的密码"),
        Pair("MdFronts", "List<String> 行情前置"),
        Pair("TdFronts", "List<String> 交易前置"),
        Pair("BrokerID", "String 经纪商ID"),
        Pair("AppID", "String 交易终端软件的标识码"),
        Pair("AuthCode", "String 交易终端软件的授权码"),
        Pair("UserProductInfo", "String 交易终端软件的产品信息。默认为空"),
        Pair("Timeout", "Long 接口调用超时时间（单位：毫秒）。默认为 6000"),
    ).run {
        if (!BuildInfo.IS_PLUGIN) {
            add(Pair("CachePath", "String 存贮订阅信息文件等临时文件的目录。默认为 ./data/ctp/"))
        }
        toList()
    }
}