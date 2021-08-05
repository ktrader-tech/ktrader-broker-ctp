@SuppressWarnings("requires-transitive-automatic")
module ktrader.broker.ctp {
    requires transitive ktrader.broker.api;
    requires jctp;
    requires static org.pf4j;

    exports org.rationalityfrontline.ktrader.broker.ctp;
}