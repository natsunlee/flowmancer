import pytest
from importlib import resources
from flowmancer.executors.local import LocalExecutor
from flowmancer.typedefs.exceptions import DuplicateDependency

def test_read():
    with resources.open_text("tests.resources", "sample.txt") as f:
        x = f.readlines()
    assert x[0] == 'hello world'

#def test_name():
#    ex = Executor("sample")
#    assert ex.name == "sample"
#
#@pytest.mark.parametrize(
#    "val", ["r", "NOT REAL", None, "", 1, 0]
#)
#def test_state_enum_exception(val):
#    with pytest.raises(TypeError):
#        Executor("sample").state = val
#
#def test_duplicate_dependency_exception():
#    a = Executor("a")
#    b = Executor("b")
#    a.add_dependency(b)
#    with pytest.raises(DuplicateDependency):
#        a.add_dependency(b)
#
#@pytest.mark.parametrize(
#    "val", ["NOT REAL", None, "", 1, 0]
#)
#def test_dependency_type_exception(val):
#    a = Executor("a")
#    with pytest.raises(TypeError):
#        a.add_dependency(val)