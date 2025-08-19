drop database if exists `Electronica-DW`;
create database `Electronica-DW`;
use `Electronica-DW`;

create table Products(
pid integer primary key not null,
pname varchar(60) not null,
price double not null
);

create table Customers(
cid integer primary key not null,
cname varchar(100) not null,
gender varchar(1) not null,
constraint check (LENGTH(Gender) < 2)
);

create table Suppliers(
sid integer primary key not null,
sname varchar(80) not null
);

create table Stores(
stid integer primary key not null,
stname varchar(80) not null
);

create table Time(
tid integer primary key not null auto_increment,
day integer not null,
month integer not null,
quarter integer not null,
year integer not null,
timestamp date not null
);

create table facts(
fid integer primary key not null auto_increment,
pid integer not null,
tid integer not null,
stid integer not null,
sid integer not null,
cid integer not null,
sale double not null,
quantity integer not null,
foreign key (pid) references Products(pid),
foreign key (tid) references Time(tid),
foreign key (stid) references Stores(stid),
foreign key (sid) references Suppliers(sid),
foreign key (cid) references Customers(cid)
);

select * from facts limit 30247;