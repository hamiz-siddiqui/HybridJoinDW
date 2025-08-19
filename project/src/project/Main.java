package project;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Iterator;
import java.util.Scanner;
import java.sql.*;
import java.time.LocalDate;
import org.apache.commons.collections4.*;
import org.apache.commons.collections4.multimap.*;
import java.util.Collection;
import java.util.Iterator;

class Stream implements Runnable{
	public int fetch;
	ArrayList <Transactions> LT;
	LinkedBlockingQueue<Transactions> buf;
	boolean cbuf;
	public Stream(String u, String p, String url, String name, LinkedBlockingQueue<Transactions> t, int f, boolean cb){
		fetch = f;
		cbuf = cb;
		buf = t;
		url = url + name;
		LT = new ArrayList<Transactions>();
		Connection connect = null;
		try{
				connect = DriverManager.getConnection(url, u, p);
			    System.out.println("Database connected!");
			    try {
			    	System.out.println("Adding transactions");
					Statement statement = connect.createStatement();
					ResultSet res = statement.executeQuery("select * from transactions;");
					while(res.next()) {
						int cid = Integer.parseInt(res.getString("CustomerID"));
						int pid = Integer.parseInt(res.getString("ProductID"));
						int oid = Integer.parseInt(res.getString("Order ID"));
						int quant = Integer.parseInt(res.getString("Quantity Ordered"));
						Date od = res.getDate("Order Date");
						String cname = res.getString("CustomerName");
						String gen = res.getString("Gender");
						Transactions tt = new Transactions(cid, pid, oid, quant, cname, gen, od);
						LT.add(tt);
					}
			    connect.close();
			    }
			    catch (SQLException e) {
			    	e.printStackTrace();
			    }
		}
		catch (SQLException e) {
			throw new IllegalStateException("Cannot connect the database!", e);
		}
	}
	@Override
	public void run() {
		System.out.println("In thread");
		while(!LT.isEmpty()) {
			Transactions t = LT.remove(0);
			try {
				buf.put(t);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(fetch);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		cbuf = true;
		System.out.println("Thread complete");
		return;
		//Here an issue in Thread.class of Java does not allow the system to close, the last line executed before the code hangs is its closing bracket of run
	}
}

class Join implements Runnable{
	LinkedBlockingQueue<Transactions> buf;
	Queue q;
	HashMap ht;
	int sbuf, del, lastdel;
	String u, p, dburl, dwurl;
	boolean finished;
	public Join(String u, String p, String url, String mdname, String dwname, LinkedBlockingQueue<Transactions> b, int s, boolean sb) {
		buf = b;
		finished = sb;
		ht = new HashMap();
		q = new Queue();
		sbuf = s;
		this.u = u;
		this.p = p;
		this.dburl = url + mdname;
		this.dwurl = url + dwname;
		finished = false;
		del = lastdel = 0;
	}
	
	@Override
	public void run() {
		System.out.println("Inside HJ Thread");
		while (!finished) {
			Transactions temp = new Transactions();
			temp = buf.poll();
			while (temp == null)
				temp = buf.poll();
			q.enqueue(temp);
			ht.add(temp);
			if (finished)
				break;
		}
//			System.out.println(q.size);
	}
	public void join() {
		Connection connect = null;
		try{
			Transactions jn = q.peek();
			try {
				if (jn.pid == 0)
				return;
			}
			catch (NullPointerException n) {
				return;
			}
			connect = DriverManager.getConnection(dburl, u, p);
		    try {
				Statement statement = connect.createStatement();
				statement.setFetchSize(10);
				ResultSet res = statement.executeQuery("select * from master_data where productid >= " + jn.pid + " limit 10;");
				while(res.next()) {
					int pid = Integer.parseInt(res.getString("productID"));
					String pname = res.getString("productName");
					String sprice = res.getString("productPrice");
					double price = Double.parseDouble(sprice.replace("$", "\0"));
					int stid = Integer.parseInt(res.getString("storeID"));
					String stname = res.getString("storeName");
					String sname = res.getString("supplierName");
					Statement stmt = connect.createStatement();
					ResultSet check = stmt.executeQuery("select supplierid, count(distinct(suppliername)) as 'Count' from master_data where supplierid in (select distinct supplierid from master_data where suppliername = \'" + sname + "\') group by supplierid having count(distinct(suppliername)) = 1;");
					check.next();
					int sid;
					try {
						sid = Integer.parseInt(check.getString("supplierID"));
					}
					catch (SQLException e) {
						sid = res.getInt("supplierID");
					}
					MasterData md = new MasterData(pid, pname, price, sid, sname, stid, stname);
					md.insertintodw(dwurl, u, p);
					if (finished)
						return;
					del += q.deleteall(md.pid, md.sid, md.stid, md.price, u, p, dwurl);
					ht.drop(jn);
					double nspeed = (double)q.size / (double)1000;
					if (nspeed >= 0.8) {
						sbuf = 75;
					}
					else if (nspeed >= 0.5) {
						sbuf = 50;
					}
					else if (nspeed >= 0.2) {
						sbuf = 20;
					}
					else {
						sbuf = 10;
					}
				}
				System.out.println("Deletions: " + del + ". Last Time Deletions: " + lastdel +  ". Queue Size: " + q.size);
				if (finished == true) {
					return;
				}
			}
		    catch (SQLException e) {
		    	e.printStackTrace();
		    }
			connect.close();
		}
		catch (SQLException e) {
			throw new IllegalStateException("Cannot connect the database!", e);
		}
		if (lastdel == del && lastdel != 0) {
			finished = true;
			return;
		}
		else {
			lastdel = del;
		}
	}	
}



public class Main{	
	public static void transform(String user, String pwd) {
		String url = "jdbc:mysql://localhost:3306/master_data";
		Connection connect = null;
		Connection dwconnect = null;
		String dwurl = "jdbc:mysql://localhost:3306/electronica-dw";
		try {
			connect = DriverManager.getConnection(url, user, pwd);
		    System.out.println("Database connected!");
		    dwconnect = DriverManager.getConnection(dwurl, user, pwd);
		    System.out.println("Warehouse connected!");
		    
		 }
		catch (SQLException e) {
		     throw new IllegalStateException("Cannot connect the database!", e);
		 }
		try {
			Statement statement = connect.createStatement();
			ResultSet res = statement.executeQuery("select * from master_data;");
			while(res.next()) {
				int pid = Integer.parseInt(res.getString("productID"));
				String pname = res.getString("productName");
				String sprice = res.getString("productPrice");
				double price = Double.parseDouble(sprice.replace("$", "\0"));
				int stid = Integer.parseInt(res.getString("storeID"));
				String stname = res.getString("storeName");
				int sid = Integer.parseInt(res.getString("supplierID"));
				String sname = res.getString("supplierName");
				MasterData md = new MasterData(pid, pname, price, sid, sname, stid, stname);
				md.insertintodw(dwurl, user, pwd);	
			}
			
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		try
		{
			connect.close();
			dwconnect.close();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		System.out.println("Enter your username: ");
		Scanner s = new Scanner(System.in);
		String user = s.nextLine();
		System.out.println("Enter your password: ");
		String pwd = s.nextLine();
		System.out.println("Enter your host: ");
		String host = s.nextLine();
		System.out.println("Enter your port: ");
		String port = s.nextLine();
		System.out.println("Enter your transaction db name: ");
		String tname = s.nextLine();
		System.out.println("Enter your master data db name: ");
		String mdname = s.nextLine();
		System.out.println("Enter your warehouse name: ");
		String dwname = s.nextLine();
		s.close();
		String url = "jdbc:mysql://" + host + ":" + port + "/";
		LinkedBlockingQueue<Transactions> bufq = new LinkedBlockingQueue<Transactions>();
		int speed = 10;
		boolean fin = false;
		Thread stream = new Thread(new Stream(user, pwd, url, tname, bufq, speed, fin));
		if (stream.isDaemon())
			stream.setDaemon(false);
		stream.start();
		Join hj = new Join(user, pwd, url, mdname, dwname, bufq, speed, fin);
		hj.q.adddates(user, pwd, url+dwname);
		Thread stest = new Thread(hj);
		if (stest.isDaemon())
			stest.setDaemon(false);
		stest.start();
		while (hj.q.size < 1000) {/*System.out.println(hj.q.size);*/}
		System.out.println("Starting Join");
		while(!fin) {
			hj.join();
			try {
				Thread.sleep(speed);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (fin)
				break;
		}
		stream.interrupt();
		stest.interrupt();
		System.out.println("Join completed!");
	}
}
