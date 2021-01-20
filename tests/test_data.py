from backend.data import get_static_ward_data


def test_get_static_ward_data():
    expected = 'population'
    result = get_static_ward_data()

    assert expected in result
