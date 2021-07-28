package org.rationalityfrontline.ktrader.broker.ctp

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
            when (it.data.type) {
                BrokerEventType.MD_TICK -> Unit
                else -> {
                    logger.info(it.data.toString())
                }
            }
        }
        val ctpConfig = mutableMapOf<String, Any>().apply {
            putAll(CtpAccounts.huaTaiP)
            put("cachePath", "./build/flow/")
            put("disableAutoSubscribe", false)
            put("disableFeeCalculation", false)
        }
        val ctpApi = CtpBrokerApi(ctpConfig, KEVENT)
        runBlocking {
            ctpApi.connect()
            logger.info("CONNECTED")
            logger.info(ctpApi.getTradingDay().toString())
            println(ctpApi.queryAssets())
            println(ctpApi.queryPositions().joinToString(",\n"))
            println(ctpApi.queryOrders(onlyUnfinished = false).joinToString(",\n"))
            println(ctpApi.queryTrades().joinToString(",\n"))
            ctpApi.close()
            logger.info("CLOSED")
        }
    }
}