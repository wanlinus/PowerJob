package tech.powerjob.server.config;

import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableApolloConfig(value = {"application"})
public class ApolloConfig {

}
