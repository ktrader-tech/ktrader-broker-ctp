@SuppressWarnings({"requires-transitive-automatic", "JavaRequiresAutoModule"})
module ktrader.broker.ctp {
    requires transitive kotlin.stdlib;
    requires transitive kotlinx.coroutines.core.jvm;
    requires transitive kevent;
    requires transitive ktrader.api;
    requires ktrader.utils;
    requires jctp;
    requires static org.pf4j;

    exports org.rationalityfrontline.ktrader.broker.ctp;
}