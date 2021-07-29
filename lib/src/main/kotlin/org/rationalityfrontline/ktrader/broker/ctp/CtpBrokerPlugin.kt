package org.rationalityfrontline.ktrader.broker.ctp

import org.pf4j.Plugin
import org.pf4j.PluginWrapper
import org.rationalityfrontline.jctp.jctpJNI

class CtpBrokerPlugin(wrapper: PluginWrapper) : Plugin(wrapper) {
    override fun delete() {
        jctpJNI.release()
    }
}
