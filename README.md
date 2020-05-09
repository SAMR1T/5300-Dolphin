# 5300-Dolphin
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

## Completed Sprint Verano:
**Author:**

*Ruoyang Qiu*

*Samrit Dhesi* 
### Milestone1
In Milestone 1, we've create an SQL interpreter that takes in SQL statments and returns an appropriatley formatted SQL statement back to the user. 
The interpreter supports SELECT, CREATE, and INSERT statments. 

The following is all keywords it interprets:
```
CREATE TABLE | IF NOT EXISTS | INSERT INTO | SELECT
FROM | WHERE | JOIN | LEFT JOIN | RIGHT JOIN
NOT | VALUES | AND | OR | AS | ON | DOUBLE | INT | TEXT
```
### Milestone2
For Milstone 2, we've added a heap storage engine via SlottedPage, HeapFile, and HeapTable classes. They contain most functionality required but there's basic functionality within the HeapTable methods. The functionality
of the classes has been verified using the Professor Lundeens provided test_slotted_page.cpp as well as his test_heap_storage.cpp. View "Storage Engine testing" on information on how to preform the tests.

**Sample SQL statements to test with:**
```
create table students (fname text, lname text, age integer)

select * from student

select a, b, g.c from foo as f, goo as g

insert into tablename values ('a', 123, 'c')
```

**Storage Engine testing:**

Within the heap_storage.cpp there's pre-made tests that can be run to determine if the classes work correctly.
They can be run after the code is compiled using "test". View the "Hot to Test" section on more on syntax for testing.

How to Test:

1. Compile the code using "make" since we've provided a Makefile
2. Run the program using "./sql5300 ../data"
3. To verify code using pre-made tests run "test"
4. To test the SQL interpreter simply type in any supported SQL
   statement or you can opt to use the sample ones provided.
5. To exit, use "quit"

### OUTPUT
Here output of the sample of the code being run:

```
>
> make
> ./sql5300 ../data
(sql5300: running with database environment at ../data)
SQL> test
test_heap_storage: create ok
drop ok
create_if_not_exsts ok
try insert
insert ok
select ok 1
project ok
Test value stored in the db
Test slotted page
ok
SQL> create table students (fname text, lname text, age integer)
CREATE TABLE students (fname TEXT, lname TEXT, age INT)
SQL> select * from student
SELECT * FROM student
SQL> select a, b, g.c from foo as f, goo as g
SELECT a, b, g.c FROM goo AS g, foo AS f
SQL> insert into tablename values ('a', 123, 'c')
INSERT INTO tablename VALUES (a, 123, c) 
SQL>
SQL> create abc
invalid SQL: create abc
SQL> quit
>
```
