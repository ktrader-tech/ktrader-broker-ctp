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
 * @param cachePath 存贮订阅信息文件等临时文件的目录。默认为 ./data/ctp/
 * @param timeout 接口调用超时时间（单位：毫秒）。默认为 10000
 * @param statusTickDelay 补发暂停交易（AUCTION_MATCHED/STOP_TRADING/CLOSED）的纯状态 Tick 的延迟时间（单位：毫秒）。默认为 2800
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
    val timeout: Long = 10000L,
    val statusTickDelay: Long = 2800L,
) {
    companion object {
        /**
         * 将标准的 Map<String, String> 格式的 config 转换为 CtpConfig
         */
        fun fromMap(config: Map<String, String>): CtpConfig {
            fun parseAsStringList(input: String?): List<String> {
                return input?.run {
                    when {
                        startsWith('[') && endsWith(']') -> subSequence(1, length - 1).split(", ", ",", "， ", "，")
                        contains(',') || contains('，') -> split(", ", ",", "， ", "，")
                        isNotEmpty() -> listOf(this)
                        else -> listOf()
                    }
                } ?: listOf()
            }
            return CtpConfig(
                mdFronts = parseAsStringList(config["MdFronts"]),
                tdFronts = parseAsStringList(config["TdFronts"]),
                investorId = config["InvestorID"] ?: "",
                password = config["Password"] ?: "",
                brokerId = config["BrokerID"] ?: "",
                appId = config["AppID"] ?: "",
                authCode = config["AuthCode"] ?: "",
                userProductInfo = config["UserProductInfo"] ?: "",
                cachePath = config["CachePath"] ?: "",
                timeout = config["Timeout"]?.toLongOrNull() ?: 10000L,
                statusTickDelay = config["StatusTickDelay"]?.toLongOrNull() ?: 2800L
            )
        }
    }
}