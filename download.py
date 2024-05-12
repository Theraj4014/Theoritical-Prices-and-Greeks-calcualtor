import concurrent.futures

import requests, zipfile,os, csv,time
import numpy as np, pandas as pd

fo_tempdata ='C:/Users/My/PycharmProjects/fo'
cash_tempdata = 'C:/Users/My/PycharmProjects/cash'
index_tempdata = 'C:/Users/My/PycharmProjects/indices/'
mto_tempdata = 'C:/Users/My/PycharmProjects/bhavcopy/mto/'

def bulk_data_download(zip_file_url):

    if ('fo' in zip_file_url):
        r = requests.get(zip_file_url)
        if r.ok:
            file_name = zip_file_url[-23:]
            zname = os.path.join(fo_tempdata, file_name)
            zfile = open(zname, 'wb')
            zfile.write(r.content)
            zfile.close()
            print('%s downloaded' %file_name)
            z = zipfile.ZipFile(zname)
            z.extractall('C:/Users/My/PycharmProjects/fo/extracted')
            print('%s extracted' %file_name)

    if ('cm' in zip_file_url):
        r = requests.get(zip_file_url)
        if r.ok:
            file_name = zip_file_url[-23:]
            zname = os.path.join(cash_tempdata, file_name)
            zfile = open(zname, 'wb')
            zfile.write(r.content)
            zfile.close()
            print('%s downloaded' % file_name)
            z = zipfile.ZipFile(zname)
            z.extractall('C:/Users/My/PycharmProjects/cash/extracted')
            print('%s extracted' % file_name)

    if ('mto' in zip_file_url):
        r = requests.get(zip_file_url)
        if r.ok:
            file_name = zip_file_url[-16:]
            zname = os.path.join(mto_tempdata, file_name)
            zfile = open(zname, 'wb')
            zfile.write(r.content)
            zfile.close()
            with open('C:/Users/My/PycharmProjects/bhavcopy/mto/' + file_name) as dat_file, open('C:/Users/My/PycharmProjects/bhavcopy/mto/csv/' + file_name + '.csv', 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                for line in dat_file:
                    row = [field.strip() for field in line.split(',')]
                    if len(row) > 0:
                        print(len(row))
                        csv_writer.writerow(row)

    elif ('ind_close' in zip_file_url):
        # print(zip_file_url)
        r = requests.get(zip_file_url)
        if r.ok:
            file_name = zip_file_url[-12:]
            # print(download)
            decoded_content = r.content.decode('utf-8')
            # print(decoded_content)
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            # print(cr)
            df = pd.DataFrame(cr)
            # print(df)
            file_name = index_tempdata + file_name
            df.to_csv(file_name,index=False,header=False)
            print('%s downloaded' % file_name)

def string_to_date(argument):
    switcher = {
        'JAN': '01',
        'FEB': '02',
        'MAR': '03',
        'APR': '04',
        'MAY': '05',
        'JUN': '06',
        'JUL': '07',
        'AUG': '08',
        'SEP': '09',
        'OCT': '10',
        'NOV': '11',
        'DEC': '12'
    }

    return switcher.get(argument, "nothing")

def bulk_rename_fo(file):

        src = file
        dst = string_to_date(file[4:7]) + file[2:4] + file[7:11] + '_fo.csv'
        os.rename(src,dst)
        print('renamed %s' %file)

def bulk_rename_cash(file):

        src = file
        dst = string_to_date(file[4:7]) + file[2:4] +  file[7:11] + '_cm.csv'
        os.rename(src, dst)
        print('renamed %s' % file)

def bulk_rename_index(file):

        src = file
        dst = file[2:4] + file[0:2]  + file[4:8] + '_ind.csv'
        os.rename(src, dst)
        print('renamed %s' % file)

if __name__ == '__main__':


    date = np.arange(1,31,1)
    month = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    month_num = ['01','02','03','04','05','06','07','08','09','10','11','12']
    Year = [2018,2019]

    t1 = time.perf_counter()
    # FnO data download
    fo_url = []
    for i in range(len(Year)):
        for j in range(len(month)):
            for k in range(0,len(date)):
                if k < 10:
                    query = 'https://www1.nseindia.com/content/historical/DERIVATIVES/' + str(Year[i]) + '/' + month[j] + '/fo0' + str(date[k]) + month[j] + str(Year[i]) + 'bhav.csv.zip'
                    file_name = 'fo0' + str(date[k]) + month[j] + str(Year[i]) + 'bhav.csv.zip'
                else:
                    query = 'https://www1.nseindia.com/content/historical/DERIVATIVES/' + str(Year) + '/' + month[i] + '/fo' + str(date[j]) + month[i] + str(Year) + 'bhav.csv.zip'
                    file_name = 'fo' + str(date[k]) + month[j] + str(Year[i]) + 'bhav.csv.zip'

                fo_url.append(query)

            # bulk_data_download(query,file_name)

    # Cash data download
    cash_url = []
    for i in range(len(Year)):
        for j in range(len(month)):
            for k in range(0,len(date)):
                if k < 10:

                    query = 'https://www1.nseindia.com/content/historical/EQUITIES/' + str(Year[i]) + '/' + month[j] + '/cm0' + str(date[k]) + month[j] + str(Year[i]) + 'bhav.csv.zip'
                    file_name = 'cm0' + str(date[k]) + month[j] + str(Year[i]) + 'bhav.csv.zip'
                else:
                    query = 'https://www1.nseindia.com/content/historical/EQUITIES/' + str(Year[i]) + '/' + month[j] + '/cm' + str(date[k]) + month[j] + str(Year[i]) + 'bhav.csv.zip'
                    file_name = 'cm' + str(date[k]) + month[j] + str(Year[i]) + 'bhav.csv.zip'

                cash_url.append(query)
        #         # bulk_data_download(query,file_name)
    #

    #index data download
    index_url = []
    for i in range(len(Year)):
        for j in range(len(month_num)):
            for k in range(0,len(date)):
                if k < 10:

                    query = 'https://www1.nseindia.com/content/indices/ind_close_all_0' + str(date[k]) + month_num[j] + str(Year[i]) + '.csv'
                    file_name = month_num[j] + '0' + str(date[k]) + str(Year[i]) + '_ind.csv'
                else:
                    query = 'https://www1.nseindia.com/content/indices/ind_close_all_' + str(date[k]) + month_num[j] + str(Year[i]) + '.csv'
                    file_name = month_num[j] + str(date[k]) + str(Year[i]) + '_ind.csv'

                index_url.append(query)
        #         # bulk_data_download(query,file_name)

    # Delivery URL
    # delivery_url = []
    # for i in range(len(month_num)):
    #     for j in range(0,len(date)):
    #         if j < 10:
    #
    #             query = 'https://www1.nseindia.com/archives/equities/mto/MTO_0' + str(date[j]) + month_num[i] + str(Year) + '.DAT'
    #             file_name = month_num[i] + '0' + str(date[j]) + str(Year) + '_del.DAT'
    #         else:
    #             query = 'https://www1.nseindia.com/archives/equities/mto/MTO_' + str(date[j]) + month_num[i] + str(Year) + '.DAT'
    #             file_name = month_num[i] + str(date[j]) + str(Year) + '_del.DAT'
    #
    #         delivery_url.append(query)

    # print(fo_url)
    # print(cash_url)
    # print(index_url)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(bulk_data_download,fo_url)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(bulk_data_download,cash_url)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(bulk_data_download,index_url)

    # with concurrent.futures.ThreadPoolExecutor() as executor:
        # executor.map(bulk_data_download,delivery_url)

    os.chdir('C:/Users/My/PycharmProjects/fo/extracted')
    fo_files = os.listdir()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(bulk_rename_fo,fo_files)

    os.chdir('C:/Users/My/PycharmProjects/cash/extracted')
    cash_files = os.listdir()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(bulk_rename_cash,cash_files)

    os.chdir('C:/Users/My/PycharmProjects/indices')
    index_files = os.listdir()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(bulk_rename_index,index_files)

    t2 = time.perf_counter()
    print(t2-t1)

