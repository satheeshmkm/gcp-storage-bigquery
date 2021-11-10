package com.sck.gcp.model;

import java.util.ArrayList;
import java.util.List;

public class TableData {
	private List<TableRaw> rows = new ArrayList<>();

	public List<TableRaw> getRows() {
		return rows;
	}

	public void setRows(List<TableRaw> rows) {
		this.rows = rows;
	}
	
	public void addRaw(TableRaw tableRaw) {
		this.rows.add(tableRaw);
	}

}
