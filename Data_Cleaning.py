import pandas as pd
import sys


class BureauBalance:

    def __init__(self):
        print("***Processing bureau_balance.csv started - please wait......***")
        try:
            self.bureau_balance = pd.read_csv('bureau_balance.csv')
        except FileNotFoundError:
            print("***Please make sure the path name of the data set - balance_bureau.csv***")
            sys.exit(1)
        self.Pr_bureau_balance = self.bur_bal()

    def bur_bal(self):
        try:
            dt = self.bureau_balance
            dt.drop(['MONTHS_BALANCE'], axis=1)
            dt['STATUS'] = dt['STATUS'].astype("str")
            dt['STATUS'] = list(map(lambda x: "D" if x.isdigit() else x, dt['STATUS']))
            dt = dt.groupby(['SK_ID_BUREAU', 'STATUS'], sort=True).size().reset_index(name='counts')
            dt = dt.sort_values('counts').groupby('SK_ID_BUREAU').first()
            dt['STATUS'] = list(map(lambda x: 1 if x == "D" else (2 if x == "C" else 3), dt['STATUS']))
            dt = dt.drop('counts', axis=1)
            dt.to_csv("processed_data_set/Pr_bureau_balance.csv")
            return dt
        except Exception as e:
            print(e)
            sys.exit(1)

    def __str__(self):
        return "***Process Finished***"

if __name__ == '__main__':
    b_bal = BureauBalance()
    Pr_bureau_balance = b_bal.Pr_bureau_balance
    print(b_bal)
