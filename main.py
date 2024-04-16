import dataloader
import sql

def insert_data(data):
    print("Starting insert")
    for row in data:
        try:
            authors = list(zip(
                row["authIdForm_i"], row["authLastName_s"], row["authFirstName_s"]
            ))
        except KeyError:
            continue
        sql.append_document(row["docid"], authors, conn)

    loader.save_cursor_mark()
    print(f"Cursor mark: {loader.cursor_mark}")


if __name__ == "__main__":
    count = 0
    BATCH = 10000
    loader = dataloader.DataLoader(
        fields=["authIdForm_i", "docid", "authLastName_s", "authFirstName_s"]
    )
    loader.load_cursor_mark()

    conn = sql.setup()

    thread = None
    while not loader.end:
        print("Starting a new load")
        data = loader.next(BATCH)
        if data is None:
            print("Error loading data")
            break
        print("Batch loaded")
        insert_data(data)
        count += BATCH
        print(f"Loaded {count / loader.num_found * 100}% of data")
