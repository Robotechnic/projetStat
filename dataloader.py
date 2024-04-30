import requests
import time
import os

class DataLoader:
    def __init__(self, doc_type="ART", fields=None):
        self.doc_type = doc_type
        self.cursor_mark = "*"
        self.end = False
        self.num_found = 0
        if fields is None:
            self.fields = ["docid", "label_s", "uri_s"]
        else:
            self.fields = fields

    def __build_request(self, rows) -> str:
        return f"https://api.archives-ouvertes.fr/search/?q=*"\
               f"&rows={rows}&docType_s={self.doc_type}&wt=json"\
               f"&sort=docid asc&cursorMark={self.cursor_mark}"\
               f"&fl={','.join(self.fields)}"

    def next(self, rows: int) -> dict:
        """
        Get the next rows of data from the API

        Args:
            rows (int): the number of rows to get

        Returns:
            dict: the data from the API
        """
        url = self.__build_request(rows)
        response = requests.get(url)
        if response.status_code != 200:
            self.end = True
            return None
        data = response.json()
        new_cursor_mark = data["nextCursorMark"]
        if new_cursor_mark == self.cursor_mark:
            self.end = True
        self.cursor_mark = new_cursor_mark
        self.num_found = data["response"]["numFound"]
        print(f"Found: {len(data['response']['docs'])}")
        return data["response"]["docs"]

    def save_cursor_mark(self):
        with open("cursor_mark.txt", "w") as f:
            f.write(self.cursor_mark)
   
    def load_cursor_mark(self):
        if not os.path.exists("cursor_mark.txt"):
            return
        with open("cursor_mark.txt", "r") as f:
            self.cursor_mark = f.read()


if __name__ == "__main__":
    loader = DataLoader(fields=["authIdHal_i", "docid", "authLastName_s", "authFirstName_s"])
    while not loader.end:
        start = time.time()
        data = loader.next(10000)
        print(f"Time: {time.time() - start}")
