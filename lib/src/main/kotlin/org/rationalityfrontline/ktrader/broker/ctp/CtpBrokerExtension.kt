package org.rationalityfrontline.ktrader.broker.ctp

import io.github.oshai.kotlinlogging.KLogger
import org.pf4j.Extension
import org.rationalityfrontline.ktrader.api.ApiInfo
import org.rationalityfrontline.ktrader.api.broker.BrokerApi
import org.rationalityfrontline.ktrader.api.broker.BrokerExtension
import java.io.File

@Extension
class CtpBrokerExtension : BrokerExtension(), ApiInfo by CtpBrokerInfo {

    override val configKeys: List<Pair<String, String>> = CtpBrokerInfo.configKeys

    override fun createApi(dataDir: File, logger: KLogger, version: String, id: String, config: Map<String, String>): BrokerApi {
        val c = if ("CachePath" !in config) {
            config.toMutableMap().apply { put("CachePath", "${dataDir.canonicalPath}/cache/") }
        } else config
        return CtpBrokerApi(dataDir, logger, CtpConfig.fromMap(c))
    }
}