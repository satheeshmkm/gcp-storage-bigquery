package com.sck.gcp.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.multipart.MultipartFile;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spring.bigquery.core.BigQueryTemplate;
import com.sck.gcp.model.TableData;
import com.sck.gcp.model.TableRaw;

@Service
public class BigQueryService {
	private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryService.class);

	/*
	 * @Autowired BigQueryFileGateway bigQueryFileGateway;
	 */

	@Autowired
	BigQueryTemplate bigQueryTemplate;

	@Autowired
	BigQuery bigquery;

	@Value("${spring.cloud.gcp.bigquery.datasetName}")
	private String datasetName;

	@Value("${spring.cloud.gcp.bigquery.project-id}")
	private String projectId;

	public String getDatasetName() throws IOException {
		return this.bigQueryTemplate.getDatasetName();
	}

	public List<String> listTables() {
		List<String> tablesList = new ArrayList<>();
		try {
			DatasetId datasetId = DatasetId.of(projectId, datasetName);
			Page<Table> tables = bigquery.listTables(datasetId, TableListOption.pageSize(100));
			tables.iterateAll().forEach(table -> tablesList.add(table.getTableId().getTable()));
			LOGGER.info("Tables listed successfully.");
		} catch (BigQueryException e) {
			LOGGER.error("Tables were not listed. Error occurred: ", e);
		}
		return tablesList;
	}

	public TableData listTableData(String tableName) {
		TableId tableId = TableId.of(datasetName, tableName);
		TableResult result = bigquery.listTableData(tableId, TableDataListOption.pageSize(5));
		TableData table = new TableData();
		List<FieldValueList> rows = new ArrayList<>();
		result.iterateAll().forEach(rows::add);

		result.iterateAll().forEach(row -> {
			TableRaw tableRaw = new TableRaw();
			rows.forEach(fvl -> tableRaw.addColumn(fvl.toString()));
			table.addRaw(tableRaw);
		});

		LOGGER.info("Query ran successfully");
		return table;
	}

	public ListenableFuture<Job> writeDataToTable(String tableName, MultipartFile file, FormatOptions fileFormat)
			throws IOException {
		return writeFileToTable(tableName, file.getInputStream(), fileFormat);
	}

	public ListenableFuture<Job> writeFileToTable(String tableName, InputStream dataStream, FormatOptions fileFormat) {
		LOGGER.info("Table Name: " + tableName);
		LOGGER.info("Dataset Name: " + this.bigQueryTemplate.getDatasetName());
		LOGGER.info("FileFormat: " + fileFormat);
		return this.bigQueryTemplate.writeDataToTable(tableName, dataStream, fileFormat);
	}
}
