import sqlite3
import bcrypt
import datetime

def get_db_connection():
    conn = sqlite3.connect('books.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    print("Initializing database...")
    conn = get_db_connection()
    c = conn.cursor()

    # Create books table with user_id
    c.execute('''CREATE TABLE IF NOT EXISTS books
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  titel TEXT,
                  auteur_voornaam TEXT,
                  auteur_achternaam TEXT,
                  genre TEXT,
                  prijs REAL,
                  paginas INTEGER,
                  bindwijze TEXT,
                  edition TEXT,
                  isbn TEXT,
                  reeks_nr TEXT,
                  uitgeverij TEXT,
                  serie TEXT,
                  staat TEXT,
                  taal TEXT,
                  gesigneerd TEXT,
                  gelezen TEXT,
                  added_date TEXT,
                  land TEXT,
                  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE)''')

    # Create geocache table
    c.execute('''CREATE TABLE IF NOT EXISTS geocache (
            location TEXT PRIMARY KEY,
            lat REAL,
            lon REAL
        )''')

    # Create users table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 username TEXT UNIQUE NOT NULL,
                 password TEXT NOT NULL,
                 role TEXT NOT NULL,
                 color TEXT DEFAULT '#e31c73',
                 dark_mode INTEGER DEFAULT 1,
                 bio TEXT DEFAULT '',
                 profile_pic TEXT DEFAULT 'default.jpg',
                 email TEXT DEFAULT '',
                 created_at TEXT DEFAULT TEXT
             )''')


    # Check and add user_id column to books if not exists
    c.execute("PRAGMA table_info(books)")
    columns = [col['name'] for col in c.fetchall()]
    if 'user_id' not in columns:
        print("Adding 'user_id' column to books table...")
        c.execute("ALTER TABLE books ADD COLUMN user_id INTEGER")
        # Optionally, set a default user_id for existing books (e.g., admin user or null)
        c.execute("UPDATE books SET user_id = NULL WHERE user_id IS NULL")
        # If you want to assign existing books to a default user, e.g., admin (id=1):
        # c.execute("UPDATE books SET user_id = 1 WHERE user_id IS NULL")

    # Add land column to books if not exists
    if 'land' not in columns:
        print("Adding 'land' column to books table...")
        c.execute("ALTER TABLE books ADD COLUMN land TEXT DEFAULT ''")

    # Add color and dark_mode columns to users if not exists
    c.execute("PRAGMA table_info(users)")
    columns = [col['name'] for col in c.fetchall()]
    if 'color' not in columns:
        print("Adding 'color' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN color TEXT DEFAULT '#e31c73'")
    if 'dark_mode' not in columns:
        print("Adding 'dark_mode' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN dark_mode INTEGER DEFAULT 1")
        
    # Add extra profile fields if not exists
    c.execute("PRAGMA table_info(users)")
    columns = [col['name'] for col in c.fetchall()]

    if 'bio' not in columns:
        print("Adding 'bio' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN bio TEXT DEFAULT ''")

    if 'profile_pic' not in columns:
        print("Adding 'profile_pic' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN profile_pic TEXT DEFAULT 'default.jpg'")

    if 'email' not in columns:
        print("Adding 'email' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN email TEXT DEFAULT ''")

    if 'created_at' not in columns:
        print("Adding 'created_at' column to users table...")
        c.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
        now = datetime.date
        c.execute("UPDATE users SET created_at = ? WHERE created_at IS NULL", (now,))
     

    c.execute('SELECT * FROM users WHERE username = ?', ('admin',))
    if not c.fetchone():
        hashed_password = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        c.execute('''INSERT INTO users 
                     (username, password, role, color, dark_mode, bio, profile_pic, email) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                  ('admin', hashed_password, 'admin', '#2563eb', 1, 
                   'Ik ben de admin van deze boeken-app.', 'default.jpg', 'admin@example.com'))



    # Drop settings table if it exists
    c.execute('DROP TABLE IF EXISTS settings')

    # Drop user_likes table if it exists
    c.execute('DROP TABLE IF EXISTS user_likes')

    # Insert default admin user if not exists
    c.execute('SELECT * FROM users WHERE username = ?', ('admin',))
    if not c.fetchone():
        hashed_password = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        c.execute('INSERT INTO users (username, password, role, color, dark_mode) VALUES (?, ?, ?, ?, ?)', 
                  ('admin', hashed_password, 'admin', '#2563eb', 1))

    conn.commit()
    conn.close()
    print("Database initialized successfully.")