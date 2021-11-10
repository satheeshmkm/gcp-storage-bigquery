package com.sck.gcp.model;

import java.util.ArrayList;
import java.util.List;

public class TableRaw {
	List<String> column = new ArrayList<>();

	public List<String> getColumn() {
		return column;
	}

	public void setColumn(List<String> column) {
		this.column = column;
	}
	
	public void addColumn(String col) {
		this.column.add(col);		
	}

}
