package org.rationalityfrontline.ktrader.broker.ctp

import org.pf4j.Extension
import org.rationalityfrontline.jctp.CThostFtdcTraderApi
import org.rationalityfrontline.ktrader.broker.api.BrokerApi

@Extension
class CtpBroker : BrokerApi {
    override fun getVersion(): String {
        return CThostFtdcTraderApi.GetApiVersion()
    }
}