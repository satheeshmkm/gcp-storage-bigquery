package com.sck.gcp.service;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient.ListTransferConfigsPagedResponse;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceSettings;
import com.google.cloud.bigquery.datatransfer.v1.StartManualTransferRunsRequest;
import com.google.cloud.bigquery.datatransfer.v1.StartManualTransferRunsResponse;
import com.google.cloud.bigquery.datatransfer.v1.TransferConfig;
import com.google.cloud.bigquery.datatransfer.v1.TransferConfigName;
import com.google.cloud.bigquery.datatransfer.v1.TransferRun;
import com.google.protobuf.Timestamp;

@Service
public class BigDataService {
	private static final Logger LOGGER = LoggerFactory.getLogger(BigDataService.class);

	@Autowired
	CredentialsProvider credentialsProvider;

	@Value("${spring.cloud.gcp.bigquery.project-id}")
	private String projectId;
	
	 

	public List<String> runTransfer(String transferName) throws IOException {
		LOGGER.info("Inside com.sck.gcp.service.BigDataService.runTransfer(String)");
		List<String> runDetails = new ArrayList<>();
		DataTransferServiceSettings dataTransferServiceSettings = getDataTransferServiceSettings();
		try (DataTransferServiceClient dataTransferServiceClient = DataTransferServiceClient
				.create(dataTransferServiceSettings)) {
			Timestamp timestamp = fromMillis(currentTimeMillis());
			StartManualTransferRunsRequest request = StartManualTransferRunsRequest.newBuilder()
					.setParent(TransferConfigName.ofProjectTransferConfigName(projectId, transferName).toString())
					.setRequestedRunTime(timestamp).build();
			StartManualTransferRunsResponse response = dataTransferServiceClient.startManualTransferRuns(request);
			List<TransferRun> runs = response.getRunsList();
			for (TransferRun run : runs) {
				runDetails.add(run.getName());
			}
		}
		return runDetails;
	}

	private DataTransferServiceSettings getDataTransferServiceSettings() throws IOException {
		return DataTransferServiceSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();
	}

	public List<String> listTransfers() throws IOException {
		List<String> transfers = new ArrayList<>();
		DataTransferServiceSettings dataTransferServiceSettings = getDataTransferServiceSettings();
		try (DataTransferServiceClient dataTransferServiceClient = DataTransferServiceClient
				.create(dataTransferServiceSettings)) {
			ListTransferConfigsPagedResponse transferConfigsPagedResponse = dataTransferServiceClient
					.listTransferConfigs("projects/" + projectId);
			for (TransferConfig transferConf : transferConfigsPagedResponse.iterateAll()) {
				transfers.add(transferConf.getDisplayName() + " : " + transferConf.getName());
			}
		}
		return transfers;
	}
}
