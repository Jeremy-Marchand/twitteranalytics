from api import word_transformation

def test_num_remove():
    assert word_transformation.num_remove('F12') == 'F'

def test_punct_remove():
    assert word_transformation.punct_remove('F.') == 'F'

def test_stop_remove():
    assert word_transformation.stop_remove('sister and brother') == 'sister brother'
