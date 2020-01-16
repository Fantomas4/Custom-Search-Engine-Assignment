from collections import Counter

test_counter = Counter(a=3, b=4, c=6)
test_counter = Counter({'ext': 25, 'ready': 15, 'value': 11, 'limit': 11, 'Apage': 8, 'mediawiki': 6})

print(test_counter)

for item in test_counter.items():
  print(item)

print(test_counter.get("ext"))

print("================================================")

test_dict = {'url': 'https://en.wikipedia.org/wiki/Apage', 'title': 'Apage - Wikipedia', 'bag': Counter({'ext': 25, 'ready': 15, 'value': 11, 'limit': 11, 'Apage': 8, 'mediawiki': 6})}

print(test_dict.get("url"))
print(test_dict.get("title"))
terms = test_dict.get("bag")

for item in terms.items():
  print(item)