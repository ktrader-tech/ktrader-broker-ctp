package org.rationalityfrontline.ktrader.broker.ctp

import org.pf4j.Extension
import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.broker.api.BrokerApi
import org.rationalityfrontline.ktrader.broker.api.BrokerExtension

@Extension
class CtpBrokerExtension : BrokerExtension() {
    override val name: String = CtpBrokerInfo.name
    override val version: String = CtpBrokerInfo.version
    override val configKeys: List<Pair<String, String>> = CtpBrokerInfo.configKeys
    override val methodExtras: List<Pair<String, String>> = CtpBrokerInfo.methodExtras

    override fun createApi(config: Map<String, String>, kEvent: KEvent): BrokerApi {
        return CtpBrokerApi(CtpConfig.fromMap(config), kEvent)
    }
}