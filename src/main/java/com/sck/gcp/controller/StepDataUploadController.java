package com.sck.gcp.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.cloud.bigquery.Job;
import com.sck.gcp.service.XmlProcessingService;

import io.swagger.annotations.ApiOperation;

@RestController
public class StepDataUploadController {
	private static final Logger LOGGER = LoggerFactory.getLogger(StepDataUploadController.class);
	
	@Autowired
	private XmlProcessingService xmlProcessingService;

	@ApiOperation("Endpoint to convert an XML file to JSON and upload in Cloud Storage")
	@GetMapping("/process/convertxml/store")
	@ResponseBody
	public String convertToJsonAndStore() {
		xmlProcessingService.convertAndStoreFile();
		return "ColudStorageController /convert endpoint";
	}
	
	@ApiOperation("Endpoint to convert product.xml in classpath to JSON and upload in table")
	@GetMapping("/process/convertxml/upload/{tableName}")
	public ResponseEntity<?>  convertToJsonAndUpload(@PathVariable("tableName") String tableName) {
		ListenableFuture<Job> payloadJob;
		try {
			payloadJob = xmlProcessingService.convertAndUpload(tableName);
			return new ResponseEntity<>(payloadJob, new HttpHeaders(), HttpStatus.OK);
		} catch (Exception e) {
			LOGGER.error("Exception", e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
		
	}
}
