# Interpreting SQL Logic Test Files

SQL Logic Test (.slt) files validate the logic of a database system using SQL statements and queries.
Here’s how to interpret them:

### Test Structure

A SQL logic test file contain multiple blocks, blocks are categorized as:

1. **`statement` Lines**
   - These lines execute SQL commands, like creating tables, or inserting data.
   - Example:
     ```sql
     statement ok
     CREATE TABLE t1(a INT, b TEXT);
     ```
     - `statement ok`: The command should execute successfully and we don't care about the result.
     - `statement error xyz`: The command should fail with an error that matches regex `xyz`.
     - In this case, we don't expect a result.

2. **`query` Lines**
   - These lines test SQL queries and compare their output against the expected result.
   - The first line describes the datatypes of the columns in the expected result. Second line is the query itself.
     The third line is the result separator, and everything after that is the expected output values.
   - Example:
     ```sql
     # An I (Integer) column and a S (String) column
     query IS
     SELECT a, b FROM t1;
     ----
     1 foo
     2 bar
     3 baz
     ```
     - **All Format Code**:
       - `I`: Signed Integer result.
       - `S`: String/TEXT result.
       - `F`: Float result.
       - `U`: Unsigned Integer result.
       - `B`: Boolean result.

Note: Comments in a SQL logic test file are prefixed with `#`.

### Example

```sql
# Create a table
statement ok
CREATE TABLE t1(a INT, b TEXT);

# Insert some data, expect this to succeed
statement ok
INSERT INTO t1 VALUES(1, 'foo'), (2, 'bar');

# Query the table, we expect a single I (Integer) column
# with values 1 and 2
query I
SELECT a FROM t1;
----
1
2

# This should fail, we can match the error message using regex
# (this fails because the table expects two values).
statement error
INSERT INTO t1(a) VALUES('invalid input');
```
