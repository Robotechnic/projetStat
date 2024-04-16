import sqlite3
import os

def remove_db():
    try:
        os.remove("links.db")
    except OSError:
        pass

def setup():
    if os.path.exists("links.db"):
        return sqlite3.connect("links.db")
    con = sqlite3.connect("links.db")
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute(
        """CREATE TABLE author
    (
    id INTEGER PRIMARY KEY,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255) NOT NULL
    );"""
    )

    cur.execute(
        """CREATE TABLE article
    (
    id INTEGER PRIMARY KEY
    );"""
    )

    cur.execute(
        """CREATE TABLE collaboration
    (
    id INTEGER PRIMARY KEY,
    article_id int NOT NULL,
    auth1 int NOT NULL,
    auth2 int NOT NULL,
    );"""
    )
    # FOREIGN KEY(article_id) REFERENCES article(id),
    # FOREIGN KEY(auth1) REFERENCES author(id),
    # FOREIGN KEY(auth2) REFERENCES author(id)

    con.commit()
    return con


def insert_author(id, lastname, firstname, con=None):
    to_close = False
    if con == None:
        con = sqlite3.connect("links.db")
        to_close = True
    cur = con.cursor()

    res = cur.execute("SELECT COUNT(*) FROM author WHERE id=" + str(id))
    if res.fetchone()[0] != 0:
        return

    cur.execute(
        'INSERT INTO author (LastName, FirstName) VALUES ("'
        + lastname
        + '", "'
        + firstname
        + '")'
    )

    if to_close:
        con.commit()
        con.close()

def insert_collaboration(auth1, auth2, articleId, con=None):
    to_close = False
    if con is None:
        con = sqlite3.connect("links.db")
        to_close = True
    cur = con.cursor()

    cur.execute(
        'INSERT INTO collaboration (auth1, auth2, article_id) VALUES ("'
        + str(auth1)
        + '","'
        + str(auth2)
        + '","'
        + str(articleId)
        + '")'
    )

    if to_close:
        con.commit()
        con.close()


def insert_document(id, con=None):
    to_close = False
    if con is None:
        con = sqlite3.connect("links.db")
        to_close = True
    cur = con.cursor()

    cur.execute("INSERT INTO article (id) VALUES (" + str(id) + ")")
    con.commit()

    if to_close:
        con.close()


def append_document(article_id, authors, con=None):
    to_close = False
    if con is None:
        con = sqlite3.connect("links.db")
        to_close = True
    try:
        insert_document(article_id, con)
    except sqlite3.IntegrityError:
        return
     
    for id, lastname, firstname in authors:
        insert_author(id, lastname, firstname, con)
    
    for author_index in range(len(authors)):
        for collaboration_index in range(author_index + 1, len(authors)):
            insert_collaboration(
                authors[author_index][0], authors[collaboration_index][0], article_id, con
            )

    con.commit()

    if to_close:
        con.close()
