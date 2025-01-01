pub const EXAMPLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS users (\n\tid UINT UNIQUE NOT NULL,\n\tname TEXT,\n\temail TEXT,\n\tpassword TEXT\n);",
    "INSERT INTO users VALUES\n(1, 'John Doe', 'example@email.com', 'password'),\n(2, 'Jane Doe', 'example2@email.com', 'password');",
    "SELECT name, id FROM users;",
    "SELECT * FROM users\nWHERE id < 2;",
    "SELECT * FROM users\nWHERE id = 1\n\tOR name = 'Jane Doe';",
    "-- Prewhere forces an index search\n-- Can do ranges as well\nSELECT * FROM users PREWHERE (id BETWEEN 1 AND 2);",
    "DELETE FROM users\nWHERE id = 1;",
    "TRUNCATE TABLE users;",
    "DROP TABLE IF EXISTS users;",
];
