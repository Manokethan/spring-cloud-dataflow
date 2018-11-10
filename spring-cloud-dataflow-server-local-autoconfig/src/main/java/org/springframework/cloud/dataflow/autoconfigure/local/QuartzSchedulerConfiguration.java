/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.autoconfigure.local;

import java.util.Properties;

import javax.sql.DataSource;

import org.quartz.spi.JobFactory;

import org.springframework.boot.autoconfigure.AbstractDependsOnBeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.dataflow.server.config.features.SchedulerConfiguration;
import org.springframework.cloud.scheduler.spi.core.Scheduler;
import org.springframework.cloud.scheduler.spi.quartz.QuartzDataSourceInitializer;
import org.springframework.cloud.scheduler.spi.quartz.QuartzScheduler;
import org.springframework.cloud.scheduler.spi.quartz.QuartzSchedulerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

/**
 * @author Manokethan Parameswaran
 */
@Configuration
@Conditional({ SchedulerConfiguration.SchedulerConfigurationPropertyChecker.class })
public class QuartzSchedulerConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public Scheduler scheduler(org.quartz.Scheduler quartzScheduler,
			QuartzSchedulerProperties quartzSchedulerProperties) {
		return new QuartzScheduler(quartzScheduler, quartzSchedulerProperties);
	}

	@Bean
	@ConditionalOnMissingBean
	public SchedulerFactoryBean quartzScheduler(ApplicationContext applicationContext, DataSource dataSource,
			QuartzSchedulerProperties quartzSchedulerProperties) {
		SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
		JobFactory jobFactory = new AutowireCapableBeanJobFactory(applicationContext.getAutowireCapableBeanFactory());
		schedulerFactoryBean.setJobFactory(jobFactory);
		if (quartzSchedulerProperties.getSchedulerName() != null) {
			schedulerFactoryBean.setSchedulerName(quartzSchedulerProperties.getSchedulerName());
		}
		schedulerFactoryBean.setAutoStartup(quartzSchedulerProperties.isAutoStartup());
		schedulerFactoryBean.setDataSource(dataSource);
		schedulerFactoryBean.setStartupDelay(quartzSchedulerProperties.getStartupDelay());
		schedulerFactoryBean.setWaitForJobsToCompleteOnShutdown(
				quartzSchedulerProperties.isWaitForJobsToCompleteOnShutdown());
		schedulerFactoryBean.setOverwriteExistingJobs(
				quartzSchedulerProperties.isOverwriteExistingJobs());
		if (!quartzSchedulerProperties.getProperties().isEmpty()) {
			Properties properties = new Properties();
			properties.putAll(quartzSchedulerProperties.getProperties());
			schedulerFactoryBean.setQuartzProperties(properties);
		}
		return schedulerFactoryBean;
	}

	@Bean
	@ConditionalOnMissingBean
	public QuartzSchedulerProperties quartzSchedulerProperties() {
		return new QuartzSchedulerProperties();
	}

	@Bean
	@ConditionalOnProperty(name = "spring.cloud.dataflow.rdbms.initialize.enable", havingValue = "true", matchIfMissing = true)
	@ConditionalOnMissingBean
	public QuartzDataSourceInitializer quartzDataSourceInitializer(DataSource dataSource,
			ResourceLoader resourceLoader, QuartzSchedulerProperties properties) {
		return new QuartzDataSourceInitializer(dataSource, resourceLoader, properties);
	}

	@Bean
	@ConditionalOnProperty(name = "spring.cloud.dataflow.rdbms.initialize.enable", havingValue = "true", matchIfMissing = true)
	public static DataSourceInitializerSchedulerDependencyPostProcessor dataSourceInitializerSchedulerDependencyPostProcessor() {
		return new DataSourceInitializerSchedulerDependencyPostProcessor();
	}

	private static class DataSourceInitializerSchedulerDependencyPostProcessor
			extends AbstractDependsOnBeanFactoryPostProcessor {

		DataSourceInitializerSchedulerDependencyPostProcessor() {
			super(Scheduler.class, SchedulerFactoryBean.class, "quartzDataSourceInitializer");
		}

	}

}
