pub const EXAMPLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS users (\n\tid UINT UNIQUE NOT NULL,\n\tname TEXT,\n\temail TEXT,\n\tpassword TEXT\n);",
    "INSERT INTO users VALUES\n(1, 'John Doe', 'example@email.com', 'password'),\n(2, 'Jane Doe', 'example2@email.com', 'password');",
    "SELECT name, id FROM users;",
    "SELECT * FROM users\nWHERE id = 1;",
    "SELECT * FROM users\nWHERE id = 1\n\tOR name = 'Jane Doe';",
];
