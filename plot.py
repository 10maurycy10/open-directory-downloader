import pandas

data = pandas.read_csv("trace", delimiter="\t")

mapping = list(set(data["host"]))

data["hostid"] = data["host"].rank(method='dense').astype(int)

colors = ["Red", "Blue", "Green", "Black", "Gray", "Magenta", "Pink", "White"]

data["color"] = [colors[x] for x in data["hostid"]]


print(data.head())

plot = data.plot(x="time", y="num", color=data["color"], figsize=(51, 10), s=1, logy=True, kind="scatter")

plot.get_figure().savefig(f'plot.png')
