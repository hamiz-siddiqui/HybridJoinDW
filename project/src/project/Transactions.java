package project;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.stream.Collectors;

public class Transactions{
	public int cid, pid, oid, quant, sid, stid, tid;
	public String cname, gen;
	Date od;
	public double sale;
	int inserts;
	public Transactions(int cid, int pid, int oid, int quant, String cname, String gen, Date od) {
		this.cid = cid;
		this.cname = cname;
		this.pid = pid;
		this.oid = oid;
		this.quant = quant;
		this.gen = gen;
		this.gen = gen.substring(0, 1);
		this.od = od;
		
	}
	public Transactions (int f) {
		cid = f;
	}
	public Transactions () {
		cid = pid = oid = quant = sid = stid = tid = inserts = 0;
		cname = gen = "";
		od = null;
		sale = 0;
	}
	public void addtodw(String u, String p, String dwurl) {
		Connection dwconnect = null;
		try {
		    dwconnect = DriverManager.getConnection(dwurl, u, p);
		}
		catch (SQLException e) {
		     throw new IllegalStateException("Cannot connect the database!", e);
		}
		try {
			Statement insert = dwconnect.createStatement();
			ResultSet check = insert.executeQuery("select cid from customers where cid = " + cid + ";");
			check.next();
			try {
				if (check.wasNull()) {
					insert.executeUpdate("insert into customers (cid, cname, gender) value (" + cid + ", \'" + cname + "\', \'" + gen + "\');");
				}
			}
			catch (NullPointerException n){
				try {
					insert.executeUpdate("insert into customers (cid, cname, gender) value (" + cid + ", \'" + cname + "\', \'" + gen + "\');");
				}
				catch (SQLIntegrityConstraintViolationException s) {}
			}
			dwconnect.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void join(int sid, int stid, double p) {
		this.sid = sid;
		this.stid = stid;
		this.sale = p * quant;
	}
	public void addfacts(String u, String p, String dwurl) {
		Connection dwconnect = null;
		try {
		    dwconnect = DriverManager.getConnection(dwurl, u, p);
		}
		catch (SQLException e) {
		     throw new IllegalStateException("Cannot connect the database!", e);
		}
		try {
			Statement insert = dwconnect.createStatement();
			ResultSet r = insert.executeQuery("select tid from time where tid = " + tid + ";");
			r.next();
			try{
				insert.executeUpdate("insert into facts (cid, pid, sid, stid, tid, sale, quantity) value (" + cid + ", " + pid + ", " + sid + ", " + stid + ", " + tid + ", " + sale + ", " + quant + ");");
			}
			catch (NullPointerException e) {
				insert.executeUpdate("insert into facts (cid, pid, sid, stid, tid, sale, quantity) value (" + cid + ", " + pid + ", " + sid + ", " + stid + ", " + tid + ", " + sale + ", " + quant + ");");
			}
			dwconnect.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void copy(Transactions t) {
		t.cid = cid;
		t.cname = cname;
		t.gen = gen;
		t.oid = oid;
		t.od = od;
		t.pid = pid;
		t.sale = sale;
		t.quant = quant;
		t.stid = stid;
		t.tid = tid;
		t.sid = sid;
	}
	public void display() {
		System.out.println(oid + " " + cid + " " + cname + " " + gen + " " + pid + " " + sale + " " + od + " " + tid + " " + sid + " " + stid);
	}
	public boolean equals(Transactions t){
		if (t.oid == oid && pid == t.pid) {
			return true;
		}
		return false;
	}
}
