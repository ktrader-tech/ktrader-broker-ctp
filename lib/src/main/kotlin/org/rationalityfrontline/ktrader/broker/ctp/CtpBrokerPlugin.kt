package org.rationalityfrontline.ktrader.broker.ctp;

import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

class CtpBrokerPlugin(wrapper: PluginWrapper) : Plugin(wrapper) {
    override fun start() {
        println("CtpBrokerPlugin start")
    }

    override fun stop() {
        println("CtpBrokerPlugin stop")
    }

    override fun delete() {
        println("CtpBrokerPlugin delete")
    }
}
