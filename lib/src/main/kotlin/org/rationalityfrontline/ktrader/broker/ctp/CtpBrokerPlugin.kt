package org.rationalityfrontline.ktrader.broker.ctp

import org.pf4j.Plugin
import org.pf4j.PluginWrapper
import org.rationalityfrontline.jctp.jctpJNI

@Suppress("unused")
class CtpBrokerPlugin(wrapper: PluginWrapper) : Plugin(wrapper) {
    override fun start() {
        // 这是为了防止插件在未被使用即被 delete 时 jctpJNI.release() 报错
        jctpJNI.libraryLoaded()
    }

    override fun delete() {
        // 释放 native gc root
        jctpJNI.release()
    }
}
