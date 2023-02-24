from ClassForm4 import Form4
from multiprocessing import Pool
import time


def create_form4(cik):
    form4 = Form4(cik, start_date='2012-01-01', end_date='2022-12-31')
    form4.add_stock_data()
    return form4


def main(ciks, parallel_exc):
    # create a pool of processes
    with Pool(processes=parallel_exc) as pool:
        for form4 in pool.imap_unordered(create_form4, ciks):
            form4.save_to_csv(f'form4-10yrs/data_{form4.cik}.csv')


if __name__ == '__main__':
    start_time = time.time()
    ciks = ["1318605", "320193", "1045810", "1018724", "789019", "1326801", "1652044",
            "1682852", "1647639", "1535527", "1818874", "1783879", "1633917", "1559720", "2488"]
    main(ciks, 2)
    end_time = time.time()
    print(f"Execution time: {round(end_time - start_time)} seconds.")
