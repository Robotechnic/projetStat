import dataloader
import sql
import time
import mariadb

def insert_data(data):
    print("Starting insert")
    con = sql.connect()
    for row in data:
        try:
            authors = list(zip(
                row["authIdForm_i"], row["authLastName_s"], row["authFirstName_s"]
            ))
            sql.append_document(row["docid"], authors, con)
        except KeyError:
            continue

    loader.save_cursor_mark()
    print(f"Cursor mark: {loader.cursor_mark}")
    con.close()

if __name__ == "__main__":
    BATCH = 10000
    loader = dataloader.DataLoader(
        fields=["authIdForm_i", "docid", "authLastName_s", "authFirstName_s"]
    )
    loader.load_cursor_mark()

    sql.setup()
    count = sql.document_count()

    while not loader.end:
        t = time.time()
        print("Starting a new load")
        data = loader.next(BATCH)
        if data is None:
            print("Error loading data")
            break
        print("Batch loaded")
        loaded = False
        while not loaded:
            try:
                insert_data(data)
                loaded = True
            except mariadb.OperationalError:
                print("Error inserting data, retrying")
                time.sleep(1)

    
        count += BATCH
        print(f"Loaded {count / loader.num_found * 100}% of data in {time.time() - t} seconds")
