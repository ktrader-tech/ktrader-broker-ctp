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
        val ctpConfig = mutableMapOf(
            "mdFronts" to listOf(
                "tcp://0.0.0.0:0"
            ),
            "tdFronts" to listOf(
                "tcp://0.0.0.0:0"
            ),
            "investorId" to "123456",
            "password" to "123456",
            "brokerId" to "1234",
            "appId" to "rf_ktrader_1.0.0",
            "authCode" to "ASDFGHJKL",
            "cachePath" to "./build/flow/",
            "disableAutoSubscribe" to false,
            "disableFeeCalculation" to false,
        )
//        ctpConfig.putAll(CtpAccounts.huaTaiP)
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