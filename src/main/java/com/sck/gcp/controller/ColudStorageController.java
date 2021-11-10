package com.sck.gcp.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.sck.gcp.service.CloudStorageService;

import io.swagger.annotations.ApiOperation;

@RestController
public class ColudStorageController {
	private static final Logger LOGGER = LoggerFactory.getLogger(BigDataContoller.class);

	@Autowired
	private CloudStorageService cloudStorageService;

	@ApiOperation("Endpoint to check whether ColudStorageController /cloud-storage endpoint running")
	@GetMapping("/cloud-storage")
	@ResponseBody
	public String sayHello() {
		return "ColudStorageController /cloud-storage endpoint";
	}

	@ApiOperation("Upload the file to GCP Cloud Storage")
	@PostMapping(value = "/upload/{uploadFolder}", consumes = { "multipart/form-data" })
	public ResponseEntity<?> uploadFile(@RequestParam(value = "file", required = true) MultipartFile uploadfile,
			@PathVariable("uploadFolder") String uploadFolder) {
		if (uploadfile.isEmpty()) {
			LOGGER.error("File is not passed");
			return new ResponseEntity<>(false, HttpStatus.OK);
		}

		try {
			LOGGER.info("Uploading file to cloud");
			String msg = cloudStorageService.writeFile(uploadFolder, uploadfile);
			return new ResponseEntity<>(msg, new HttpHeaders(), HttpStatus.OK);
		} catch (Exception e) {
			LOGGER.error("Exception happened", e);
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
	}

}
