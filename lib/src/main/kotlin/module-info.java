@SuppressWarnings("requires-transitive-automatic")
module ktrader.broker.ctp {
    requires transitive kotlin.stdlib;
    requires transitive kotlinx.coroutines.core.jvm;
    requires transitive ktrader.broker.api;
    requires transitive kevent;
    requires jctp;
    requires static org.pf4j;

    exports org.rationalityfrontline.ktrader.broker.ctp;
}