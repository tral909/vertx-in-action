package ru.indorm1992.vertxinaction;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class GatewayVerticle extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(GatewayVerticle.class);

    private final HashMap<String, Object> lastData = new HashMap<>();
    private WebClient webClient;
    private JsonObject cachedLastMinutes;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        webClient = WebClient.create(vertx);

        vertx.eventBus().consumer("temperature.updates", this::storeUpdate);

        Router router = Router.router(vertx);
        router.get("/latest").handler(this::latestData);
        router.get("/five-minutes").handler(this::fiveMinutes);

        vertx.createHttpServer()
            .requestHandler(router)
            .listen(8082)
            .onSuccess(ok -> {
                logger.info("HTTP server running on http://127.0.0.1:{}", 8082);
                startPromise.complete();
            })
            .onFailure(startPromise::fail);
    }

    private void fiveMinutes(RoutingContext context) {

    }

    private void latestData(RoutingContext context) {

    }

    private void storeUpdate(Message<JsonObject> msg) {

    }

    public static void main(String[] args) {

    }
}
