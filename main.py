import dataloader
import sql
import time
import mariadb

def insert_data(data):
    print("Starting insert")
    con = sql.connect()
    for i, row in enumerate(data):
        if i % 50 == 0:
            print(f"{i / len(data) * 100:.2f}%     ", end="\r")
        try:
            authors = list(zip(
                row["authIdForm_i"], row["authLastName_s"], row["authFirstName_s"]
            ))
            sql.append_document(row["docid"], authors, con)
        except KeyError:
            continue
    print("100%     ")
    print("Committing")

    loader.save_cursor_mark()
    print(f"Cursor mark: {loader.cursor_mark}")
    con.close()

if __name__ == "__main__":
    BATCH = 5000
    loader = dataloader.DataLoader(
        fields=["authIdForm_i", "docid", "authLastName_s", "authFirstName_s"],
        doc_type="ART+OR+COMM+OR+THESE",
        years=[2050, 2029, 2028, 2026, 2025, 2024, 2023, 2022, 2020, 2021, 2019, 2018]
    )
    loader.load_cursor_mark()

    sql.setup()
    count = sql.document_count()
    print(f"Loaded {count} documents")

    while not loader.end:
        t = time.time()
        print("Starting a new load")
        data = loader.next(BATCH)
        if data is None:
            print("Error loading data")
            continue
        print("Batch loaded")
        loaded = False
        while not loaded:
            try:
                insert_data(data)
                loaded = True
            except mariadb.OperationalError as e:
                print(f"Error: {e}")
                print("Error inserting data, retrying")
                time.sleep(1)

        count += BATCH
        print(f"Loaded {count / loader.num_found * 100}% of data in {time.time() - t} seconds")
