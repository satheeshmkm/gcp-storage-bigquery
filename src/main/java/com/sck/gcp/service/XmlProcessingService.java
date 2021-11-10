package com.sck.gcp.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.sck.gcp.processor.FileProcessor;

@Service
public class XmlProcessingService {
	private static final Logger LOGGER = LoggerFactory.getLogger(XmlProcessingService.class);

	@Autowired
	private CloudStorageService cloudStorageService;

	@Autowired
	private BigQueryService bigQueryService;
	
	@Autowired
	private FileProcessor fileProcessor;

	public static int PRETTY_PRINT_INDENT_FACTOR = 4;

	public String convertAndStoreFile() {
		String status = "Failure";
		String xml = fileProcessor.readFile();
		String json = fileProcessor.convertToJSONL(xml);
		uploadToCloudStorage(json);
		return status;
	}

	public ListenableFuture<Job> convertAndUpload(String tableName) {
		String xml = fileProcessor.readFile();
		LOGGER.info("readFile() completed");
		String json = fileProcessor.convertToJSONL(xml);
		LOGGER.info("convertToJSON() completed");
		InputStream dataStream = new ByteArrayInputStream(json.getBytes());
		LOGGER.info("ByteArrayInputStream() completed");
		ListenableFuture<Job> payloadJob = bigQueryService.writeFileToTable(tableName, dataStream,
				FormatOptions.json());
		LOGGER.info("Upload completed to Table:" + tableName);
		return payloadJob;
	}

	private void uploadToCloudStorage(String json) {
		byte[] jsonBytes = null;
		try {
			jsonBytes = json.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			LOGGER.error("UnsupportedEncodingException:", e);
		}

		Instant instant = Instant.now();
		long timeStampMillis = instant.toEpochMilli();
		StringBuilder cloudStrorageFile = new StringBuilder().append(timeStampMillis).append(".json");
		String cloudStroragePath = cloudStorageService.getUploadFilePath("product", cloudStrorageFile.toString());
		if (null != jsonBytes) {
			cloudStorageService.uploadToCloudStorage(cloudStroragePath, jsonBytes);
		}
	}

	

	

}
