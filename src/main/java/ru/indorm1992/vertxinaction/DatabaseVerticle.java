package ru.indorm1992.vertxinaction;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

public class DatabaseVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseVerticle.class);
    private static final int httpPort = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));

    private PgPool pgPool;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        pgPool = PgPool.pool(vertx, new PgConnectOptions()
            .setHost("127.0.0.1")
            .setUser("postgres")
            .setDatabase("postgres")
            .setPassword("12345"), new PoolOptions());

        vertx.eventBus().consumer("temperature.updates", this::recordTemperature);

        Router router = Router.router(vertx);
        router.get("/data").handler(this::getAllData);
        router.get("/for/:uuid").handler(this::getData);
        router.get("/last-5-minutes").handler(this::getLastFiveMinutes);

        vertx.createHttpServer()
            .requestHandler(router)
            .listen(httpPort)
            .onSuccess(ok -> {
                logger.info("http server running: http://127.0.0.1:{}", httpPort);
                startPromise.complete();
            })
            .onFailure(startPromise::fail);
    }

    private void getLastFiveMinutes(RoutingContext context) {
        //TODO
    }

    private void getData(RoutingContext context) {
        String uuid = context.request().getParam("uuid");
        logger.info("Requesting the data for {} from {}", uuid, context.request().remoteAddress());
        String query = "select tstamp, value from temperature_records where uuid = $1";
        pgPool.preparedQuery(query)
            .execute(Tuple.of(uuid))
            .onSuccess(rows -> {
                JsonArray array = new JsonArray();
                for (Row row : rows) {
                    array.add(new JsonObject()
                        .put("timestamp", row.getValue("tstamp").toString())
                        .put("value", row.getValue("value")));
                }
                context.response()
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject()
                        .put("uuid", uuid)
                        .put("data", array).encode());
            })
            .onFailure(failure -> {
                logger.error("Query failed", failure);
                context.fail(500);
            });
    }

    private void getAllData(RoutingContext context) {
        logger.info("Requesting all data from {}", context.request().remoteAddress());
        String query = "select * from temperature_records";
        pgPool.preparedQuery(query)
            .execute()
            .onSuccess(rows -> {
                JsonArray array = new JsonArray();
                for (Row row : rows) {
                    array.add(new JsonObject()
                        .put("uuid", row.getString("uuid"))
                        .put("temperature", row.getValue("value").toString())
                        .put("timestamp", row.getValue("tstamp").toString()));
                }
                context.response()
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject().put("data", array).encode());
            })
            .onFailure(failure -> {
                logger.error("Query failed", failure);
                context.fail(500);
            });
    }

    private void recordTemperature(Message<JsonObject> msg) {
        String uuid = msg.body().getString("uuid");
        OffsetDateTime timestamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(msg.body().getLong("timestamp")), ZoneId.systemDefault());
        Double temperature = msg.body().getDouble("temperature");

        String query = "insert into temperature_records(uuid, tstamp, value) values ($1, $2, $3)";
        Tuple tuple = Tuple.of(uuid, timestamp, temperature);
        pgPool.preparedQuery(query)
            .execute(tuple)
            .onSuccess(row -> logger.info("Recorder {}", tuple.deepToString()))
            .onFailure(failure -> logger.error("Recording failed", failure));
    }

    public static void main(String[] args) {
        // distributed vertex (can run on different nodes)
        Vertx.clusteredVertx(new VertxOptions())
            .onSuccess(vertx -> {
                vertx.deployVerticle(new DatabaseVerticle());
            })
            .onFailure(failure -> {
                logger.error("Woops", failure);
            });
    }
}
