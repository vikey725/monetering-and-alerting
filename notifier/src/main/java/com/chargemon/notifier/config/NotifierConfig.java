package com.chargemon.notifier.config;

import com.chargemon.common.json.JsonMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
@EnableConfigurationProperties(NotifierProperties.class)
public class NotifierConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return JsonMapperFactory.standard();
    }

    @Bean
    public RestClient restClient(NotifierProperties props) {
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        Duration t = props.getWebhook().getTimeout();
        f.setConnectTimeout(t);
        f.setReadTimeout(t);
        return RestClient.builder().requestFactory(f).build();
    }
}
