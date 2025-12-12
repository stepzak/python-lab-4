def test_eq_operator(db_library_initial_data):
    res = db_library_initial_data.select("library", genre="Genre 1")
    assert len(res) == 2
    books = db_library_initial_data.select_rows("library", genre="Genre 1")
    assert all(b.genre == "Genre 1" for b in books)

    res = db_library_initial_data.select("library", title="Title 1")
    assert len(res) == 1
    books = db_library_initial_data.select_rows("library", title="Title 1")
    assert books[0].title == "Title 1"


def test_gt_operator(db_library_initial_data):
    res = db_library_initial_data.select("library", year__gt=2010)
    assert len(res) == 1
    books = db_library_initial_data.select_rows("library", year__gt=2010)
    assert all(b.year > 2010 for b in books)


def test_ge_operator(db_library_initial_data):
    res = db_library_initial_data.select("library", year__ge=2010)
    assert len(res) == 2
    books = db_library_initial_data.select_rows("library", year__ge=2010)
    assert all(b.year >= 2010 for b in books)


def test_lt_operator(db_library_initial_data):
    res = db_library_initial_data.select("library", pages__lt=120)
    assert len(res) == 1
    books = db_library_initial_data.select_rows("library", pages__lt=120)
    assert all(b.pages < 120 for b in books)


def test_le_operator(db_library_initial_data):
    res = db_library_initial_data.select("library", pages__le=125)
    assert len(res) == 2
    books = db_library_initial_data.select_rows("library", pages__le=125)
    assert all(b.pages <= 125 for b in books)


def test_in_operator(db_library_initial_data):
    res = db_library_initial_data.select("library", author__in=["Author 1", "Author 2"])
    assert len(res) == 3
    books = db_library_initial_data.select_rows("library", author__in=["Author 1", "Author 2"])
    authors = {b.author for b in books}
    assert authors == {"Author 1", "Author 2"}

    res = db_library_initial_data.select("library", title__in=["Title 1", "Title 3"])
    assert len(res) == 2
    titles = {b.title for b in db_library_initial_data.select_rows("library", title__in=["Title 1", "Title 3"])}
    assert titles == {"Title 1", "Title 3"}


def test_empty_result(db_library_initial_data):
    res = db_library_initial_data.select("library", year__gt=3000)
    assert len(res) == 0

    res = db_library_initial_data.select("library", genre__in=["NonExisting"])
    assert len(res) == 0

    res = db_library_initial_data.select("library", title="NonExisting")
    assert len(res) == 0


def test_combined_filters(db_library_initial_data):
    res = db_library_initial_data.select("library", genre="Genre 1", year__ge=2010)
    assert len(res) == 2
    books = db_library_initial_data.select_rows("library", genre="Genre 1", year__ge=2010)
    assert all(b.genre == "Genre 1" and b.year >= 2010 for b in books)


def test_no_filters(db_library_initial_data):
    res = db_library_initial_data.select("library")
    assert len(res) == 3