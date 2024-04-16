import dataloader
import sql

def insert_data(data):
    print("Starting insert")
    for row in data:
        authors = list(zip(
            row["authIdForm_i"], row["authLastName_s"], row["authFirstName_s"]
        ))
        sql.append_document(row["docid"], authors, conn)

    conn.commit()
    loader.save_cursor_mark()
    print(f"Cursor mark: {loader.cursor_mark}")


if __name__ == "__main__":
    loader = dataloader.DataLoader(
        fields=["authIdForm_i", "docid", "authLastName_s", "authFirstName_s"]
    )
    loader.load_cursor_mark()

    conn = sql.setup()

    thread = None
    while not loader.end:
        print("Starting a new load")
        data = loader.next(100)
        if data is None:
            print("Error loading data")
            break
        print("Batch loaded")
        insert_data(data)
