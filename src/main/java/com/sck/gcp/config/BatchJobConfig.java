package com.sck.gcp.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sck.gcp.batch.tasks.MyTaskOne;

@Configuration
@EnableBatchProcessing
public class BatchJobConfig {

	@Autowired
	private MyTaskOne myTaskOne;
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.tasklet(myTaskOne).build();
	}

	@Bean
	public Job xmlProcessingJob() {
		return jobBuilderFactory.get("XML_Processor")
				.start(step1())
				// .next(step2())
				.build();
	}
	
	
}