/*--Reference: https://www.a2hosting.com/kb/developer-corner/mysql/managing-mysql-databases-and-users-from-the-command-line*/

GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' IDENTIFIED BY 'root';
create database root;

ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root';

use blockchain;

CREATE TABLE miner (ip_addr varchar(100) not null, stake_amt int not null, role_id int not null, public_key varchar(2048), private_key varchar(2048), 
port int not null, isblocked boolean, primary key (ip_addr, role_id) );

CREATE TABLE role (role_id int not null auto_increment, role_desc varchar(30), constraint pk_role primary key (role_id) );

CREATE TABLE transaction (tx_id int not null, credit_acc_id int, debit_acc_id int, amount double, processed_flg boolean default 0, approved_flg boolean default 0, constraint pk_transaction primary key (tx_id) );

CREATE TABLE account (acc_id int, balance double, constraint pk_account primary key (acc_id) );

CREATE TABLE usertx (tx_id int, debit_acc_id int, constraint pk_usertx primary key (tx_id));

--Insert records miner
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-17-224.us-west-2.compute.internal', 1000, 1, null, null, 12345, false);
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-47-76.us-west-2.compute.internal', 100, 2, null, null, 12345, false);
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-47-76.us-west-2.compute.internal', 10, 3, null, null, 12347, false);
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-41-55.us-west-2.compute.internal', 1, 4, null, null, 12345, false);
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-45-223.us-west-2.compute.internal', 1, 4, null, null, 12345, false);
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-39-61.us-west-2.compute.internal', 1, 4, null, null, 12345, false);

insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-21-66.us-west-2.compute.internal', 1, 4, null, null, 12345, false);
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-18-179.us-west-2.compute.internal', 1, 4, null, null, 12345, false);
insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-29-49.us-west-2.compute.internal', 1, 4, null, null, 12345, false);

insert into miner (ip_addr, stake_amt, role_id, public_key, private_key, port, isblocked) values ('ip-172-31-41-55.us-west-2.compute.internal', 1, 4, null, null, 12345, false);

--Insert records role

insert into role (role_desc) values("core leader");
insert into role (role_desc) values("core follower");
insert into role (role_desc) values("non-core leader");
insert into role (role_desc) values("non-core follower");
insert into role (role_desc) values("No role assigned");


insert into account values (1,1000);
insert into account values (2,100);
insert into account values (3,0);
insert into account values (4,0);

#Transaction
insert into transaction (tx_id, credit_acc_id, debit_acc_id, amount) values (1, 3, 1, 10);
insert into transaction (tx_id, credit_acc_id, debit_acc_id, amount) values (2, 4, 1, 100);
insert into transaction (tx_id, credit_acc_id, debit_acc_id, amount) values (3, 2, 4, 1000);

#Load file in table
SHOW VARIABLES LIKE "secure_file_priv";
load data infile '/var/lib/mysql-files/miner.csv' into table miner FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
load data infile '/var/lib/mysql-files/account.csv' into table account FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
load data infile '/var/lib/mysql-files/roles.csv' into table role FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
load data infile '/var/lib/mysql-files/transaction.csv' into table transaction FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

#Update queries
#Master
update miner set ip_addr='ip-172-31-47-76.us-west-2.compute.internal' where role_id in (2,3);
update miner set ip_addr='ip-172-31-41-55.us-west-2.compute.internal' where role_id=4;

#Manager
update miner set ip_addr='ip-172-31-17-224.us-west-2.compute.internal' where role_id=1;
update miner set ip_addr='ip-172-31-41-55.us-west-2.compute.internal' where role_id=4;

#Worker
update miner set ip_addr='ip-172-31-17-224.us-west-2.compute.internal' where role_id=1;
update miner set ip_addr='ip-172-31-47-76.us-west-2.compute.internal' where role_id in (2,3);


#FOR CMT SIZE scaling
create table transaction_bkp like transaction;
insert into transaction_bkp select * from transaction;
truncate transaction;
insert into transaction select * from transaction_bkp where credit_acc_id<20500;
select count(*) from transaction;


