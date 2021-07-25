import json
import re
import statistics
import logging

f = open('soal-2.json',)
with open('soal-2.json', 'r') as file:
    jsonfile = file.read().replace('\n', '')
data = json.loads(jsonfile)

list_berat = {}
for i in range(len(data)):
    data[i]['berat'] = re.sub(r'(rata-rata)|(masing-masing)|(rata2)', '', data[i]['berat'])
    data[i]['berat'] = re.sub(r'([a-zA-Z])', '', data[i]['berat'])
    data[i]['komoditas'] = re.sub(r'(ikan)', '', data[i]['komoditas'])
    data[i]['berat'] = data[i]['berat'].replace(',', ' ').split()
    data[i]['komoditas'] = data[i]['komoditas'].replace(',', ' ').split()
    berat = data[i]['berat']
    komoditas = data[i]['komoditas']

    if len(berat) < 1:
        next
    for j in range(len(berat)):
        try:
            if '-' in berat[j]:
                berat[j] = map(int, berat[j].split('-'))
                berat[j] = statistics.mean(berat[j])
            elif '/' in berat[j]:
                berat[j] = map(int, berat[j].split('/'))
                berat[j] = statistics.mean(berat[j])
            
            if len(berat) == len(komoditas):
                if komoditas[j] not in list_berat:
                    list_berat[komoditas[j]] = float(berat[j])
                else:
                    list_berat[komoditas[j]] += float(berat[j])
            else:
                if komoditas[j] not in list_berat:
                    list_berat[komoditas[j]] = float(berat[0])
                else:
                    list_berat[komoditas[j]] += float(berat[0])
        except Exception as err:
            logging.info(f'row {i} error:{err}')

list_berat_sorted = sorted(list_berat, key=list_berat.get, reverse=True)

for i in range(len(list_berat_sorted)):
    print(f'{i+1}. {list_berat_sorted[i]} {list_berat[list_berat_sorted[i]]}kg')