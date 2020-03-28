package io.javanuts.kafka.serializer;

import java.util.Date;

public class Supplier {

	private int supplierId;
    private String supplierName;
    private Date supplierStartDate;
    
    
    
	public Supplier() {
		super();
	}
	
	public Supplier(int supplierId, String supplierName, Date supplierStartDate) {
		super();
		this.supplierId = supplierId;
		this.supplierName = supplierName;
		this.supplierStartDate = supplierStartDate;
	}
	public int getSupplierId() {
		return supplierId;
	}
	public void setSupplierId(int supplierId) {
		this.supplierId = supplierId;
	}
	public String getSupplierName() {
		return supplierName;
	}
	public void setSupplierName(String supplierName) {
		this.supplierName = supplierName;
	}
	public Date getSupplierStartDate() {
		return supplierStartDate;
	}
	public void setSupplierStartDate(Date supplierStartDate) {
		this.supplierStartDate = supplierStartDate;
	}

    
}
