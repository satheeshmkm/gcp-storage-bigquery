package com.sck.gcp.controller;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.sck.gcp.model.TableData;
import com.sck.gcp.service.BigDataService;
import com.sck.gcp.service.BigQueryService;

import io.swagger.annotations.ApiOperation;

@RestController
public class BigDataContoller {
	private static final Logger LOGGER = LoggerFactory.getLogger(BigDataContoller.class);

	@Autowired
	private BigDataService bigDataService;

	@Autowired
	BigQueryService bigQueryService;

	@ApiOperation("Endpoint to check whether BigDataContoller /big-data endpoints is running")
	@GetMapping("/big-data")
	@ResponseBody
	public String sayHello() {
		LOGGER.info(" /big-data endpoint called");
		return "BigDataContoller /big-data endpoint";
	}

	@ApiOperation("Endpoint to list BigQuery Data Transfers")
	@GetMapping("/data-transfers")
	@ResponseBody
	public ResponseEntity<?> listTransfers() {
		LOGGER.info(" /data-transfers endpoint called");
		List<String> transfers = null;
		try {
			transfers = bigDataService.listTransfers();
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(transfers, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation("Endpoint to run BigQuery Data Transfers using BigDataContoller endpoint")
	@GetMapping("/data-transfer/{transferName}")
	@ResponseBody
	public ResponseEntity<?> runTransfer(@PathVariable("transferName") String transferName) {
		LOGGER.info(" /data-transfer/{transferName} endpoint called");
		List<String> runs = null;
		try {
			runs = bigDataService.runTransfer(transferName);
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(runs, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation("Endpoint to get the Dataset Name")
	@RequestMapping(value = "/dataset", method = RequestMethod.GET)
	public ResponseEntity<?> getDatasetName() {

		try {
			String payload = bigQueryService.getDatasetName();
			return new ResponseEntity<>(payload, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			LOGGER.error("Exception", e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

	@ApiOperation("Endpoint to list the Tables in a dataset")
	@RequestMapping(value = "/dataset/tables", method = RequestMethod.GET)
	public ResponseEntity<?> listTables() {

		try {
			List<String> tables = bigQueryService.listTables();
			return new ResponseEntity<>(tables, new HttpHeaders(), HttpStatus.OK);
		} catch (Exception e) {
			LOGGER.error("Exception", e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}

	@ApiOperation("Endpoint to list the contents of Table in a dataset")
	@RequestMapping(value = "/dataset/tablesData/{tableName}", method = RequestMethod.GET)
	public ResponseEntity<?> listTableData(@PathVariable("tableName") String tableName) {

		TableData payload;
		try {
			payload = bigQueryService.listTableData(tableName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOGGER.error("Exception", e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(payload, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation("Upload contents of a CSV file in to BigQUery Table with the name specified")
	@RequestMapping(value = "/dataset/upload/csv/{tableName}", consumes = {
			"multipart/form-data" }, method = RequestMethod.POST)
	public ResponseEntity<?> uploadFile(@RequestParam(value = "file", required = true) MultipartFile uploadfile,
			@PathVariable("tableName") String tableName) {
		LOGGER.info(" POST /dataset/upload/csv/{tableName} endpoint called with tableName:" + tableName);

		if (uploadfile.isEmpty()) {
			LOGGER.error("Upload file is Empty");
			return new ResponseEntity<>(false, HttpStatus.OK);
		}

		try {
			ListenableFuture<Job> payloadJob = bigQueryService.writeDataToTable(tableName, uploadfile,
					FormatOptions.csv());
			return new ResponseEntity<>(payloadJob, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			LOGGER.error("Exception", e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

	@ApiOperation("Upload contents of a JSONL file in to BigQUery Table with the name specified")
	@RequestMapping(value = "/dataset/upload/json/{tableName}", consumes = {
			"multipart/form-data" }, method = RequestMethod.POST)
	public ResponseEntity<?> uploadJSONFile(@RequestParam(value = "file", required = true) MultipartFile uploadfile,
			@PathVariable("tableName") String tableName) {
		LOGGER.info(" POST /dataset/upload/{tableName} endpoint called with tableName:" + tableName);

		if (uploadfile.isEmpty()) {
			LOGGER.error("Upload file is Empty");
			return new ResponseEntity<>(false, HttpStatus.OK);
		}

		try {
			ListenableFuture<Job> payloadJob = bigQueryService.writeDataToTable(tableName, uploadfile,
					FormatOptions.json());
			return new ResponseEntity<>(payloadJob, new HttpHeaders(), HttpStatus.OK);

		} catch (Exception e) {
			LOGGER.error("Exception", e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

	}

}
