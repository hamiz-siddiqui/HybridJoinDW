package project;
import java.sql.*;

public class MasterData {
	int pid, stid, sid;
	String pname, sname, stname;
	double price;
	
	public MasterData(int pid, String pname, double price, int sid, String sname, int stid, String stname) {
		this.pid = pid;
		this.stid = stid;
		this.sid = sid;
		this.pname = pname;
		this.sname = sname;
		this.stname = stname;
		this.price = price;
	}
	public void insertintodw(String dwurl, String u, String p) {
		Connection dwconnect = null;
		try {
		    dwconnect = DriverManager.getConnection(dwurl, u, p);
		}
		catch (SQLException e) {
		     throw new IllegalStateException("Cannot connect the database!", e);
		}
		try {
			Statement insert = dwconnect.createStatement();
			ResultSet check = insert.executeQuery("select pid from products where pid = " + pid + ";");
			check.next();
			try {
				if (check.wasNull()) {
					insert.executeUpdate("Insert into Products (pid, pname, price) value (" + pid + ", \'" + pname + "\', " + price + ");");
				}
				else {
					return;
				}
			}
			catch (NullPointerException n) {
				try {
					insert.executeUpdate("Insert into Products (pid, pname, price) value (" + pid + ", \'" + pname + "\', " + price + ");");
				}
				catch (SQLIntegrityConstraintViolationException s) {
					return;
				}
			}
			ResultSet check1 = insert.executeQuery("select sid from suppliers where sid = " + sid);
			check1.next();
			try {
				if (check1.wasNull()) {
					insert.executeUpdate("Insert into suppliers (sid, sname) value (" + sid + ", \'" + sname + "\');");
				}
			}
			catch (NullPointerException n) {
				try {
					insert.executeUpdate("Insert into suppliers (sid, sname) value (" + sid + ", \'" + sname + "\');");
				}
				catch (SQLIntegrityConstraintViolationException s) {
					return;
				}
			}
			ResultSet check2 = insert.executeQuery("select stid from stores where stid = " + stid);
			check2.next();
			try {
				if (check2.wasNull()) {
					insert.executeUpdate("Insert into stores (stid, stname) value (" + stid + ", \'" + stname + "\');");
				}
			}
			catch (NullPointerException n) {
				try {
					insert.executeUpdate("Insert into stores (stid, stname) value (" + stid + ", \'" + stname + "\');");
				}
				catch (SQLIntegrityConstraintViolationException s) {
					return;
				}
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		try
		{
			dwconnect.close();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}
