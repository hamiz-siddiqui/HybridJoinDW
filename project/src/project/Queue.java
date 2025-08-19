package project;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.sql.*;
import java.time.temporal.IsoFields;

class Node{
	Transactions data;
	Node next;
	Node prev;
	public Node(Transactions v) {
		data = v;
		next = prev = null;
	}
	
}

public class Queue{
	Node head;
	Node tail;
	int size, inserts;
	ArrayList<LocalDate> dates;
	public Queue(){
		head = tail = null;
		size = inserts = 0;
		dates = new ArrayList<LocalDate>();
		LocalDate sd = LocalDate.of(2019, 1, 1);
		LocalDate ed = LocalDate.of(2020, 1, 1);
		dates = (ArrayList<LocalDate>) sd.datesUntil(ed).collect(Collectors.toList());
	}
	public void enqueue(Transactions data) {
		size++;
		Node curr = new Node(data);
		curr.data.tid = size;
		if (tail == null) {
			head = tail = curr;
		}
		else {
			tail.next = curr;
			curr.prev = tail;
			tail = curr;
		}
	}
	public Transactions dequeue() {
		if (head == null) {
			return new Transactions(-1);
		}
		if (head == tail) {
			Transactions data = head.data;
			head = tail = null;
			return data;
		}
		Node curr = head;
		head = head.next;
		Transactions data = curr.data;
		head.prev = null;
		curr.next = null;
		size -= 1;
		return data;
	}
	public int deleteall(int pid, int sid, int stid, double p, String u, String pwd, String dwurl) {
		Node c = head;
		Transactions r, temp = new Transactions();
		int deleted = 0;
		while (c != null) {
			if (c.data.pid == pid) {
				r = c.data;
				if (temp.cid == 0) {
					temp = r;
				}
				else {
					if (temp.equals(r)) {
						try {
							c.prev.next = c.next;
						}
						catch (NullPointerException e) {
							
						}
						try {
							c.next.prev = c.prev;
						}
						catch(NullPointerException e) {
							
						}
						Node tc = c;
						c = c.next;
						tc = null;
						continue;
					}
					else {
						temp = r;
					}
				}
				if (dates.contains(r.od.toLocalDate())) {
					r.tid = dates.indexOf(r.od.toLocalDate()) + 1;
				}
				r.join(sid, stid, p);
				deleted++;
				r.display();
//				r.addtodw(u, pwd, dwurl);
//				r.addfacts(u, pwd, dwurl);
				if (c == head) {
					head = head.next;
					try {
						head.prev = null;
					}
					catch(NullPointerException e){
						head = null;
						break;
					}
				}
				else if (c == tail) {
					tail = tail.prev;
					tail.next = null;
				}
				else {
					try {
						c.prev.next = c.next;
					}
					catch (NullPointerException e) {}
					try{
						c.next.prev = c.prev;
					}
					catch (NullPointerException e) {}
				}
				size--;
			}
			c = c.next;
			if (c == null)
				break;
		}
		return deleted;
	}
	public boolean search(int pid) {
		Node c = head;
		if (c == null) {
			return false;
		}
		while (c != null || c.next != null) {
			if (c.data.pid == pid) {
				return true;
			}
			c = c.next;
			if (c == null)
				break;
		}
		return false;
	}
	public Transactions peek() {
		if (head == null) {
			return new Transactions(-1);
		}
		return head.data;
	}
	public boolean contains(Transactions t) {
		Node c = head;
		if (c == null)
			return false;
		while (c != null) {
			if (c.data.equals(t)) {
				return true;
			}
			c = c.next;
			if (c == null)
				return false;
		}
		return false;
	}
	public int getsize() {
		return size;
	}
	public boolean isempty() {
		if (size != 0) {
			return false;
		}
		return true;
	}
	public void display() {
		Node temp = head;
		while (temp != null) {
			temp.data.display();
			temp = temp.next;
		}
	}
	public void addtodw(String u, String p, String dwurl) {
		Node curr = head;
		int temp = 0;
		while (curr != null) {
			curr.data.tid = temp + 1;
			curr.data.addtodw(u, p, dwurl);
			temp++;
			curr = curr.next;
		}
	}
	public void adddates(String u, String p, String url) {
		Iterator<LocalDate> i = dates.iterator();
		while(i.hasNext()) {
			Date temp = Date.valueOf(i.next());
			int tid = dates.indexOf(temp.toLocalDate())+1;
			Connection dwconnect = null;
			try {
			    dwconnect = DriverManager.getConnection(url, u, p);
			}
			catch (SQLException e) {
			     throw new IllegalStateException("Cannot connect the database!", e);
			}
			try {
				Statement insert = dwconnect.createStatement();
				ResultSet check = insert.executeQuery("select tid from time where tid = " + tid + ";");
				check.next();
				try {
					if (check.wasNull()) {
						insert.executeUpdate("insert into time (tid, day, month, quarter, year, timestamp) value (" + tid + ", " + temp.toLocalDate().getDayOfMonth() + ", " + temp.toLocalDate().getMonthValue() + ", " + temp.toLocalDate().get(IsoFields.QUARTER_OF_YEAR) + ", " + temp.toLocalDate().getYear() + ", " + temp + ");");
					}
				}
				catch (NullPointerException n){
					try {
						int day = temp.toLocalDate().getDayOfMonth();
						int month = temp.toLocalDate().getMonthValue();
						int quarter = temp.toLocalDate().get(IsoFields.QUARTER_OF_YEAR);
						int year = temp.toLocalDate().getYear();
						String dt = temp.toString();
						insert.executeUpdate("insert into time (tid, day, month, quarter, year, timestamp) value (" + tid + ", " + day + ", " + month + ", " + quarter + ", " + year + ", str_to_date(\'" + dt +  "\', '%Y-%m-%d'));");
					}
					catch (SQLIntegrityConstraintViolationException s) {}
				}
				dwconnect.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
