import fscheck

dfs = fscheck.run("tests/1.txt", "tests/2.txt", output=False)

for name, df in dfs.items():
    print(name + " results:")
    df.show()
