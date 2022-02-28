package org.acme.kafka;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.providers.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.providers.connectors.InMemorySink;
import org.acme.kafka.config.KafkaTestResourceLifecycleManager;
import org.acme.kafka.quarkus.Movie;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import javax.enterprise.inject.Any;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.restassured.RestAssured.given;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

@QuarkusTest
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
@TestHTTPEndpoint(MovieResource.class)
public class MovieResourceTest {

    @Inject
    @Any
    InMemoryConnector connector;


    @Test
    public void testMovieProducerResource() throws InterruptedException {
        //given
        InMemorySink<Movie> movies = connector.sink("movies");

        // in a separate thread, feed the `MovieResource`
        ExecutorService movieSender = startSendingMovies();

        // check if, after at most 5 seconds, we have at least 2 items collected, and they are what we expect
        await().<List<? extends Message<Movie>>>until(movies::received, t -> t.size() == 2);

        Movie receivedMovie_1 = movies.received().get(0).getPayload();
        assertThat(receivedMovie_1.getTitle(), is("The Shawshank Redemption"));
        assertThat(receivedMovie_1.getYear(), is(1994));

        Movie receivedMovie_2 = movies.received().get(1).getPayload();
        assertThat(receivedMovie_2.getTitle(), is("12 Angry Men"));
        assertThat(receivedMovie_2.getYear(), is(1957));

        // shutdown the executor that is feeding the `MovieResource`
        movieSender.shutdownNow();
        movieSender.awaitTermination(5, SECONDS);
    }

    private ExecutorService startSendingMovies() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            while (true) {
                given()
                        .contentType(ContentType.JSON)
                        .body("{\"title\":\"The Shawshank Redemption\",\"year\":1994}")
                .when()
                        .post()
                .then()
                        .statusCode(202);

                given()
                        .contentType(ContentType.JSON)
                        .body("{\"title\":\"12 Angry Men\",\"year\":1957}")
                .when()
                        .post()
                .then()
                        .statusCode(202);

                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        return executorService;
    }

}
