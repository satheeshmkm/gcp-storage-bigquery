package com.sck.gcp.service;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

@Service
public class CloudStorageService {
	private static final Logger LOGGER = LoggerFactory.getLogger(CloudStorageService.class);

	@Autowired
	private Storage storage;

	@Value("${com.sck.bucket.name}")
	private String bucketName;

	public String writeFile(String uploadFolder, MultipartFile uploadFile) throws IOException {
		LOGGER.info("Inside com.sck.gcp.service.CloudStorageService.writeFIle(String, MultipartFile)");
		String uploadedFile = getUploadFilePath(uploadFolder, uploadFile.getOriginalFilename());
		byte[] arr = uploadFile.getBytes();
		uploadToCloudStorage(uploadedFile, arr);
		return "File uploaded to bucket " + bucketName + " as " + uploadedFile;
	}

	public void uploadToCloudStorage(String uploadedFile, byte[] arr) {
		BlobId blobId = BlobId.of(bucketName, uploadedFile);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();	
		storage.create(blobInfo, arr);
	}

	public String getUploadFilePath(String uploadFolder, String fileName) {
		StringBuilder uploadFilePath = new StringBuilder();
		if (StringUtils.isNotBlank(uploadFolder)) {
			uploadFilePath.append(uploadFolder).append("/");
		}
		uploadFilePath.append(fileName);
		return  uploadFilePath.toString();
	}

}
