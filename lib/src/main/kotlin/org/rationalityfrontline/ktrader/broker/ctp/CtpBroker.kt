package org.rationalityfrontline.ktrader.broker.ctp

import org.pf4j.Extension
import org.rationalityfrontline.kevent.KEvent
import org.rationalityfrontline.ktrader.broker.api.Broker
import org.rationalityfrontline.ktrader.broker.api.BrokerApi

@Extension
class CtpBroker : Broker() {
    override val name: String = CtpBrokerInfo.name
    override val version: String = CtpBrokerInfo.version
    override val configKeys: List<Pair<String, String>> = CtpBrokerInfo.configKeys
    override val methodExtras: List<Pair<String, String>> = CtpBrokerInfo.methodExtras

    override fun createApi(config: Map<String, Any>, kEvent: KEvent): BrokerApi {
        return CtpBrokerApi(config, kEvent)
    }
}