use `electronica-dw`;
#Query 1
select s.sname, sum(f.quantity) as 'Times Sold', sum(f.sale) as 'Sales', t.month, t.quarter from facts f join suppliers s on f.sid = s.sid join time t on t.tid = f.tid group by s.sid, month, quarter order by s.sid, month;
#Query 2
select p.pname, sum(f.quantity) as 'Times Sold', sum(f.sale) as 'Sales', t.month from facts f join products p on p.pid = f.pid join time t on t.tid = f.tid join suppliers s on s.sid = f.sid where s.sname = 'DJI' group by month, p.pid order by month, p.pid;
#Query 3
select p.pname, sum(quantity) as 'All Items Sold' from facts f join products p on p.pid = f.pid join time t on t.tid = f.tid where dayname(t.timestamp) = 'Saturday' or dayname(t.timestamp) = 'Sunday' group by p.pname order by sum(quantity) desc limit 5;
#Query 4
select p.pname, (select sum(f.sale) from facts f join time t on t.tid = f.tid where quarter = 2) as 'Quarter 2 Sales', (select sum(f.sale) from facts f join time t on t.tid = f.tid where quarter = 3) as 'Quarter 3 Sales', (select sum(f.sale) from facts f join time t on t.tid = f.tid where year = 2019) as 'Yearly Sale' from facts f join products p on p.pid = f.pid group by p.pid order by p.pname;
#Query 5
select p.pname from suppliers s join facts f on f.sid = s.sid join products p on p.pid = f.pid where s.sname = 'Pakistan' group by p.pname;
#Query 6
drop view if exists `storeanalysis_mv`;
create view `storeanalysis_mv` as select st.stname, p.pname, sum(f.sale) as 'Sales', sum(quantity) as 'Items Sold' from stores st join facts f on f.stid = st.stid join products p on p.pid = f.pid group by st.stid, st.stname, p.pid, p.pname;
select * from `storeanalysis_mv`;
#Query 7
select st.stname, p.pname, t.month, sum(f.quantity) as 'Items Sold', sum(f.sale) as 'Sales In Month' from facts f join time t on t.tid = f.tid join products p on p.pid = f.pid join stores st on st.stid = f.stid where st.stname = 'Tech Haven' group by pname, month, st.stid;
#Query 8
drop view if exists `supplier_performance_mv`;
create view `supplier_performance_mv` as select s.sid, s.sname, t.month, sum(f.quantity) as 'Items Sold', sum(f.sale) as 'Monthly Sale' from facts f join suppliers s on s.sid = f.sid join time t on t.tid = f.tid group by s.sid, s.sname, t.month order by s.sid, t.month;
select * from `supplier_performance_mv`;
#Query 9
select c.cname, sum(f.sale) as 'Yearly Sale', count(f.pid) as 'Products Purchased' from facts f join customers c on c.cid = f.cid group by c.cname, f.pid order by sum(f.sale) desc limit 5;
#Query 10
drop view if exists `customer_store_sales_mv`;
create view `customer_store_sales_mv` as select st.stname, c.cname, sum(f.quantity) as 'Products Sold', sum(f.sale) as 'Sales per Customer' from facts f join stores st on st.stid = f.stid join customers c on c.cid = f.cid group by c.cname, st.stname order by st.stname, c.cname;
select * from `customer_store_sales_mv`;