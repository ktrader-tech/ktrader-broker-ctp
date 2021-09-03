package org.rationalityfrontline.ktrader.broker.ctp

import org.pf4j.Extension
import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.api.KTraderExtensionType
import org.rationalityfrontline.ktrader.api.broker.BrokerApi
import org.rationalityfrontline.ktrader.api.broker.BrokerExtension

@Extension
class CtpBrokerExtension : BrokerExtension() {
    override val name: String = CtpBrokerInfo.name
    override val version: String = CtpBrokerInfo.version
    override val author: String = CtpBrokerInfo.author
    override val description: String = CtpBrokerInfo.description
    override val type: KTraderExtensionType = KTraderExtensionType.BROKER
    override val configKeys: List<Pair<String, String>> = CtpBrokerInfo.configKeys

    override fun createApi(config: Map<String, String>, kEvent: KEvent): BrokerApi {
        return CtpBrokerApi(CtpConfig.fromMap(config), kEvent)
    }
}