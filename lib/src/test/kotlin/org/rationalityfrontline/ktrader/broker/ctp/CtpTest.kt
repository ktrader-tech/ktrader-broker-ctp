package org.rationalityfrontline.ktrader.broker.ctp

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.rationalityfrontline.kevent.KEVENT
import org.rationalityfrontline.ktrader.broker.api.BrokerEvent
import org.rationalityfrontline.ktrader.broker.api.BrokerEventType
import org.slf4j.LoggerFactory

class CtpTest {

    private val logger = LoggerFactory.getLogger("TEST")

    @Test
    fun test() {
        KEVENT.subscribeMultiple<BrokerEvent>(BrokerEventType.values().asList()) {
            logger.info(it.data.toString())
        }
        val ctpConfig = CtpAccounts.huaTaiP
        val ctpApi = CtpBrokerApi(ctpConfig, KEVENT)
        runBlocking {
            ctpApi.connect()
            logger.info("CONNECTED")
            logger.info(ctpApi.getTradingDay().toString())
            println(ctpApi.queryAssets())
            println(ctpApi.queryPositions())
            ctpApi.subscribeMarketData(listOf("DCE.m2109"))
            delay(50000000000)
            ctpApi.close()
            logger.info("CLOSED")
        }
    }
}