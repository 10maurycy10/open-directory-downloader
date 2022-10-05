def log(string):
    print(string)
    open("log", "a").write(f"{string}\n")
