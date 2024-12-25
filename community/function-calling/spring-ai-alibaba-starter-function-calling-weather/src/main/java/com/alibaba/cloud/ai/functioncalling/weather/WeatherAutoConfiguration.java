/*
 * Copyright 2024-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.functioncalling.weather;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Description;

/**
 * @author 北极星
 */
@ConditionalOnClass(WeatherService.class)
@EnableConfigurationProperties(WeatherProperties.class)
@ConditionalOnProperty(prefix = "spring.ai.alibaba.functioncalling.weather", name = "enabled", havingValue = "true")
public class WeatherAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	@Description("Use api.weather to get weather information.")
	public WeatherService getWeatherServiceFunction(WeatherProperties properties) {
		return new WeatherService(properties);
	}

}