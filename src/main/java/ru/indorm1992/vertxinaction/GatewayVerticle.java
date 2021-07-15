package ru.indorm1992.vertxinaction;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class GatewayVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(GatewayVerticle.class);

    private static final int storePort = 8080;
    private static final String storeHost = "127.0.0.1";
    private static final int httpPort = 8082;

    private final HashMap<String, Object> lastData = new HashMap<>();
    private WebClient webClient;
    private CircuitBreaker breaker;
    private JsonObject cachedLast5Minutes;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        webClient = WebClient.create(vertx);
        breaker = CircuitBreaker.create("store", vertx);

        breaker.openHandler(v -> logger.info("Circuit breaker open"));
        breaker.halfOpenHandler(v -> logger.info("Circuit breaker half-open"));
        breaker.closeHandler(v -> logger.info("Circuit breaker close"));

        vertx.eventBus().consumer("temperature.updates", this::storeUpdate);

        Router router = Router.router(vertx);
        router.get("/latest").handler(this::latestData);
        router.get("/five-minutes").handler(this::fiveMinutes);

        vertx.createHttpServer()
            .requestHandler(router)
            .listen(httpPort)
            .onSuccess(ok -> {
                logger.info("HTTP server running on http://127.0.0.1:{}", httpPort);
                startPromise.complete();
            })
            .onFailure(startPromise::fail);
    }

    private void fiveMinutes(RoutingContext context) {
        Future<JsonObject> future = breaker.execute(promise -> {
            webClient
                .get(storePort, storeHost, "/last-5-minutes")
                .expect(ResponsePredicate.SC_OK)
                .timeout(3000)
                .as(BodyCodec.jsonObject())
                .send()
                .map(HttpResponse::body)
                .onSuccess(promise::complete)
                .onFailure(promise::fail);
        });

        future.onSuccess(json -> {
            logger.info("Last 5 minutes data requested from {}", context.request().remoteAddress());
            cachedLast5Minutes = json;
            context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(json.encode());
        })
        .onFailure(failure -> {
            logger.info("Last 5 minutes data requested from {} and served from cache",
                context.request().remoteAddress());
            if (cachedLast5Minutes != null) {
                context.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(cachedLast5Minutes.encode());
            } else {
                logger.error("Request failed", failure);
                context.fail(500);
            }
        });
    }

    private void latestData(RoutingContext context) {

    }

    private void storeUpdate(Message<JsonObject> msg) {

    }

    public static void main(String[] args) {
        Vertx.clusteredVertx(new VertxOptions())
            .onSuccess(vertx -> vertx.deployVerticle(new GatewayVerticle()));
    }
}
