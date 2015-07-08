create table test.test (line_id bigint, date date, count bigint, primary key (line_id, date));
insert into test.test (line_id, date, count) values (2000, 20150703, 10) on duplicate key update count = count + 10;
select * from test.test;