from dataclasses import dataclass

@dataclass
class Book:
    title: str
    author: str
    year: int
    genre: str
    isbn: int
    pages: int

    def __gt__(self, other):
        if isinstance(other, Book):
            return self.pages > other.pages
        elif isinstance(other, int):
            return self.pages > other
        raise TypeError

    def __lt__(self, other):
        if isinstance(other, Book):
            return self.pages < other.pages
        elif isinstance(other, int):
            return self.pages < other
        raise TypeError

    def __call__(self, *args, **kwargs):
        print(f"Reading book {self.title}")

    def __iter__(self):
        return iter(range(1, self.pages + 1))

if __name__ == "__main__":
    book = Book("a", "b", 2000, "c", 123465435462, 500)