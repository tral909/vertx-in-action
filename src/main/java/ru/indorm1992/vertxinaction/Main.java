package ru.indorm1992.vertxinaction;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // same process vertex
//        Vertx vertx = Vertx.vertx();
//        vertx.deployVerticle("ru.indorm1992.vertxinaction.SensorVerticle", new DeploymentOptions().setInstances(1));
//
//        vertx.eventBus()
//            .<JsonObject>consumer("temperature.updates", msg -> {
//                logger.info(">>> {}", msg.body().encodePrettily());
//            });

        // distributed vertex (can run on different nodes)
        Vertx.clusteredVertx(new VertxOptions())
            .onSuccess(vertx -> {
                vertx.deployVerticle(new SensorVerticle());
            })
            .onFailure(failure -> {
                logger.error("Woops", failure);
            });
    }
}
