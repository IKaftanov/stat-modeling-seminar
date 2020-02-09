"""
copy list of proxies from https://free-proxy-list.net/

the column delimiter is `\t`
after this cope final list to PROXIES variable

TODO: automate it
"""

s = """72.35.40.34	8080	US	United States	elite proxy	no	yes	7 seconds ago"""

result = []

for line in s.split("\n"):
    row = line.split("\t")
    url = f"http://{row[0]}:{row[1]}"
    if row[6] == 'yes':
        result.append({"https": url})
    else:
        result.append({"http:": url})
print(result)