def test_delete_single_record(db_library_initial_data):
    initial_count = len(db_library_initial_data.select_rows("library"))
    assert initial_count == 3
    
    db_library_initial_data.delete("library", isbn=1234567890123)
    final_count = len(db_library_initial_data.select_rows("library"))
    assert final_count == 2
    
    res = db_library_initial_data.select("library", isbn=1234567890123)
    assert len(res) == 0


def test_delete_multiple_records(db_library_initial_data):
    db_library_initial_data = db_library_initial_data
    db_library_initial_data.delete("library", genre="Genre 1")
    remaining = db_library_initial_data.select_rows("library")
    assert len(remaining) == 1
    assert remaining[0].genre == "Genre 2"


def test_delete_non_existing_record(db_library_initial_data):
    db_library_initial_data = db_library_initial_data
    initial_count = len(db_library_initial_data.select_rows("library"))
    db_library_initial_data.delete("library", isbn=9999999999999)
    final_count = len(db_library_initial_data.select_rows("library"))
    assert final_count == initial_count


def test_update_non_existing_record(db_library_initial_data):
    db_library_initial_data = db_library_initial_data
    initial_count = len(db_library_initial_data.select_rows("library"))
    db_library_initial_data.update("library", {"title": "No Change"}, isbn=9999999999999)
    final_count = len(db_library_initial_data.select_rows("library"))
    assert final_count == initial_count