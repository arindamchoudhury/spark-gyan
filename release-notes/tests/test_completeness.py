from catalog.parser import parse_dump
from catalog.completeness import check_release

DUMP = """<h2>New Feature</h2>
<li>[<a href='x'>SPARK-1</a>] - feat one</li>
<li>[<a href='x'>SPARK-2</a>] - feat two</li>
<h2>Bug</h2>
<li>[<a href='x'>SPARK-3</a>] - bug one</li>
"""

def test_check_release_balances():
    items = parse_dump(DUMP)
    report = check_release(DUMP, items)
    assert report["total_li"] == 3
    assert report["kept"] == 2
    assert report["dropped"] == 1
    assert report["unaccounted"] == 0
    assert report["ok"] is True
