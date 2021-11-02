package org.rationalityfrontline.ktrader.broker.ctp

import org.pf4j.Extension
import org.rationalityfrontline.ktrader.api.ApiInfo
import org.rationalityfrontline.ktrader.api.KTraderExtensionType
import org.rationalityfrontline.ktrader.api.broker.BrokerApi
import org.rationalityfrontline.ktrader.api.broker.BrokerExtension

@Extension
class CtpBrokerExtension : BrokerExtension(), ApiInfo by CtpBrokerInfo {

    override val type: KTraderExtensionType = KTraderExtensionType.BROKER
    override val configKeys: List<Pair<String, String>> = CtpBrokerInfo.configKeys

    override fun createApi(config: Map<String, String>): BrokerApi {
        return CtpBrokerApi(CtpConfig.fromMap(config))
    }
}