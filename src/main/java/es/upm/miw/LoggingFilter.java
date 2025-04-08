package es.upm.miw;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;


@Order(Ordered.HIGHEST_PRECEDENCE)
@Component
@Profile("dev")
public class LoggingFilter implements WebFilter {

    private static final Logger log = LogManager.getLogger(LoggingFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {

        ServerHttpRequest request = exchange.getRequest();
        ServerHttpResponse response = exchange.getResponse();

        log.debug("-------------------------------------------------------------------------------------------------");
        log.debug("Request: {} {}", request.getMethod(), request.getURI());

        log.debug("Headers:");
        request.getHeaders().forEach((name, values) -> {
            log.debug("  {}: {}", name, values);
        });

        log.debug("Parameters:");
        request.getQueryParams().forEach((name, values) -> {
            log.debug("  {}: {}", name, values);
        });

        ServerHttpRequestDecorator decoratedRequest = new ServerHttpRequestDecorator(request) {
            @Override
            public Flux<DataBuffer> getBody() {
                // Se intercepta el flujo de datos para leer el cuerpo
                return super.getBody().doOnNext(dataBuffer -> {
                    byte[] content = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(content);
                    String bodyString = new String(content, StandardCharsets.UTF_8);
                    if (!bodyString.isEmpty()) {
                        log.debug("Request body (JSON): {}", bodyString);
                    }
                });
            }
        };

        DataBufferFactory bufferFactory = response.bufferFactory();
        ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(response) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                if (body instanceof Flux<? extends DataBuffer> fluxBody) {
                    Flux<DataBuffer> bufferFlux = fluxBody.map(dataBuffer -> {
                        byte[] content = new byte[dataBuffer.readableByteCount()];
                        dataBuffer.read(content);
                        String bodyString = new String(content, StandardCharsets.UTF_8);
                        if (!bodyString.isEmpty()) {
                            log.debug("Response body: {}", bodyString);
                        }
                        return bufferFactory.wrap(content);
                    });
                    return super.writeWith(bufferFlux);
                }
                return super.writeWith(body);
            }
        };

        ServerWebExchange mutatedExchange = exchange.mutate()
                .request(decoratedRequest)
                .response(decoratedResponse)
                .build();

        return chain.filter(mutatedExchange);
    }
}
