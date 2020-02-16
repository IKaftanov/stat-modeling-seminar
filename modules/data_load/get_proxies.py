"""
copy list of proxies from https://free-proxy-list.net/

the column delimiter is `\t`
after this cope final list to PROXIES variable

TODO: automate it
"""

s = """144.217.163.138	8080	CA	Canada	anonymous	no	no	1 minute ago"""

result = []

for line in s.split("\n"):
    row = line.split("\t")
    url = f"http://{row[0]}:{row[1]}"
    if row[6] == 'yes':
        result.append({"https": url})
    else:
        result.append({"http:": url})
print(result)