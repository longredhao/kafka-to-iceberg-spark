

drop table tbl_test;
create table tbl_test(id int, c1 string, c2 bigint);
desc tbl_test;
alter table tbl_test add columns ( c3 string );
desc tbl_test;
alter table tbl_test change c3 c3 string after id;
desc tbl_test;
alter table tbl_test change c3 c3 string after c1;
desc tbl_test;







table_name change c_time c_time string after column_1 ;  -- 移动到指定位置,column_1字段的后面


after c2;

a
alter table tbl_test add columns(c3 string) after c2;


