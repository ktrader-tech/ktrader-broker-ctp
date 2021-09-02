package org.rationalityfrontline.ktrader.broker.ctp

/**
 * CtpBrokerApi 的实例化参数
 *
 * @param mdFronts 行情前置
 * @param tdFronts 交易前置
 * @param investorId 投资者资金账号
 * @param password 投资者资金账号的密码
 * @param brokerId 经纪商ID
 * @param appId 交易终端软件的标识码
 * @param authCode 交易终端软件的授权码
 * @param userProductInfo 交易终端软件的产品信息
 * @param cachePath 存贮订阅信息文件等临时文件的目录
 * @param disableAutoSubscribe 是否禁止自动订阅持仓合约的行情（用于计算合约今仓保证金以及查询持仓时返回最新价及盈亏）
 * @param disableFeeCalculation 是否禁止计算保证金及手续费（首次计算某个合约的费用时，可能会查询该合约的最新 Tick、保证金率、手续费率，造成额外开销，后续再次计算时则会使用上次查询的结果）
 */
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
    val disableAutoSubscribe: Boolean = false,
    val disableFeeCalculation: Boolean = false,
) {
    companion object {
        /**
         * 将标准的 Map<String, String> 格式的 config 转换为 CtpConfig
         */
        fun fromMap(config: Map<String, String>): CtpConfig {
            return CtpConfig(
                mdFronts = config["mdFronts"]?.run { subSequence(1, length - 1).split(", ") } ?: listOf(),
                tdFronts = config["tdFronts"]?.run { subSequence(1, length - 1).split(", ") } ?: listOf(),
                investorId = config["investorId"] ?: "",
                password = config["password"] ?: "",
                brokerId = config["brokerId"] ?: "",
                appId = config["appId"] ?: "",
                authCode = config["authCode"] ?: "",
                userProductInfo = config["userProductInfo"] ?: "",
                cachePath = config["cachePath"] ?: "",
                disableAutoSubscribe = config["disableAutoSubscribe"] == "true",
                disableFeeCalculation = config["disableFeeCalculation"] == "true",
            )
        }
    }
}