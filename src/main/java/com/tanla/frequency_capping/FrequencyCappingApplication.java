package com.tanla.frequency_capping;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import redis.clients.jedis.JedisPool;

@SpringBootApplication
public class FrequencyCappingApplication {

	public static void main(String[] args) {
		SpringApplication.run(FrequencyCappingApplication.class, args);
	}

	@Value("${spring.data.redis.url}")
	String connectionString;

	@Bean
	JedisPool jedisPool() {
		JedisPool jedisPool = new JedisPool(this.connectionString);
		return jedisPool;
	}
}
