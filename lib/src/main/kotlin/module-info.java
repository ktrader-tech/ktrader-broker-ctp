module ktrader.broker.ctp {
    requires kotlin.stdlib;
    requires kotlinx.coroutines.core.jvm;
    requires transitive ktrader.broker.api;
    requires static org.pf4j;
    requires jctp;

    exports org.rationalityfrontline.ktrader.broker.ctp;
}