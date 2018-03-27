import fsumcheck

# Init
all_dfs = fsumcheck.run("tests/1.txt", "tests/2.txt", output=False, delimiter=" ")


class Expected:
    different_checksum = {"file2.diff"}
    only_left = {"file4.onlyleft"}
    only_right = {"file5.onlyright", "file3.nocheck_2"}
    problematic_left = {"file3.nocheck_1", "only_checksum_1"}
    problematic_right = {"file3.nocheck_1", "file3.nocheck_2", "only_checksum_2"}


def test_only_left():
    only_left = all_dfs["only_left"].rdd.keys().collect()
    assert Expected.only_left == set(only_left)


def test_only_right():
    only_right = all_dfs["only_right"].rdd.keys().collect()
    assert Expected.only_right == set(only_right)


def test_different_checksum():
    different_checksum = all_dfs["different_checksum"].rdd.keys().collect()
    assert Expected.different_checksum == set(different_checksum)


def test_problematic_left():
    problematic_left = all_dfs["problematic_left"].rdd.keys().collect()
    assert Expected.problematic_left == set(problematic_left)


def test_problematic_right():
    problematic_right = all_dfs["problematic_right"].rdd.keys().collect()
    assert Expected.problematic_right == set(problematic_right)
