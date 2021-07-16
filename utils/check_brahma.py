import json

try:
    brahma = json.load(open('brahma.json', 'r'))
    success = True
except Exception as e:
    success = False
    print('Brahma is not correctly formatted. Please check the following error: \n {}'.format(e))

if success:
    print('Voila!! Everything is fine with Brahma.')