package com.sck.gcp.batch.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.sck.gcp.service.XmlProcessingService;

@Component
public class MyTaskOne implements Tasklet {
	private static final Logger LOGGER = LoggerFactory.getLogger(MyTaskOne.class);
	
	@Autowired
	private XmlProcessingService xmlProcessingService;
	

	public MyTaskOne(XmlProcessingService xmlProcessingService) {
		this.xmlProcessingService = xmlProcessingService;
	}


	@Override
	public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
		LOGGER.info("MyTaskOne Executing .....................");
		xmlProcessingService.convertAndUpload("PRODUCTS");
		return RepeatStatus.FINISHED;
	}

}
