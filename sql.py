import mariadb

def connect():
    con = mariadb.connect(
        host="localhost",
        port=3306,
        user="scrapper",
        password="password",
        database="hal"
    )
    con.autocommit = False
    con.auto_reconnect = True
    return con

def setup():
    con = connect()
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS author
    (
    id INTEGER PRIMARY KEY,
    label varchar(255) NOT NULL
    );"""
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS article
    (
    id INTEGER PRIMARY KEY
    );"""
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS collaboration
    (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    auth1 int NOT NULL,
    auth2 int NOT NULL,
    count int DEFAULT 1,
    FOREIGN KEY(auth1) REFERENCES author(id),
    FOREIGN KEY(auth2) REFERENCES author(id),
    CONSTRAINT unique_authors UNIQUE (auth1, auth2)
    );"""
    )

    con.commit()
    return con


def insert_author(id, lastname, firstname, con=None):
    to_close = False
    if con == None:
        con = connect()
        to_close = True
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM author WHERE id=?", (id,))
    res = cur.fetchall()
    if res[0][0] != 0:
        return

    lastname = lastname.replace('"', "'")
    firstname = firstname.replace('"', "'")
    
    try:
        cur.execute(
            'INSERT INTO author (id, label) VALUES (?,?)', (id, firstname + " " + lastname)
        )
    except mariadb.OperationalError:
        print("Error inserting author")
        print(lastname, firstname)
        exit(1)

    if to_close:
        con.commit()
        con.close()

def insert_collaboration(auth1, auth2, con=None):
    to_close = False
    if con is None:
        con = connect()
        to_close = True
    cur = con.cursor()

    if auth1 > auth2:
        auth1, auth2 = auth2, auth1

    cur.execute(
        'INSERT INTO collaboration (auth1, auth2, count) VALUES (?, ?, 1) ON DUPLICATE KEY UPDATE count = count + 1',
        (auth1, auth2),
    )

    if to_close:
        con.commit()
        con.close()


def insert_document(id, con=None):
    to_close = False
    if con is None:
        con = connect()
        to_close = True
    cur = con.cursor()

    cur.execute("INSERT INTO article (id) VALUES (" + str(id) + ")")
    con.commit()

    if to_close:
        con.close()


def append_document(article_id, authors, con=None):
    to_close = False
    if con is None:
        con = connect()
        to_close = True
    try:
        insert_document(article_id, con)
    except mariadb.IntegrityError:
        return

    for id, lastname, firstname in authors:
        insert_author(id, lastname, firstname, con)

    if len(authors) > 1:
        query = "INSERT INTO collaboration (auth1, auth2, count) VALUES (?, ?, 1) ON DUPLICATE KEY UPDATE count = count + 1"
        data = []
        for author_index in range(len(authors)):
            for collaboration_index in range(author_index + 1, len(authors)):
                auth1 = authors[author_index][0]
                auth2 = authors[collaboration_index][0]
                if auth1 > auth2:
                    auth1, auth2 = auth2, auth1
                data.append((auth1, auth2))

        cur = con.cursor()
        cur.executemany(query, data)

    con.commit()

    if to_close:
        con.close()

def document_count(con=None):
    to_close = False
    if con is None:
        con = connect()
        to_close = True
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM article")
    res = cur.fetchall()

    if to_close:
        con.close()

    return res[0][0]
