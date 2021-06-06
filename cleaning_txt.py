import pandas as pd


def clean_txt_telecomm(start, end, feature):
    time_range = pd.period_range(start, end, freq='D').astype('string')
    for i in time_range:
        file_df = pd.read_csv(f'raw_txtdata/dataverse_files/sms-call-internet-mi-{i}.txt', header=None, sep='\t',
                              names=['CellID', 'datetime', 'countrycode', 'smsin', 'smsout', 'callin',
                                     'callout', 'internet'])
        # file_df = file_df[(file_df['CellID'] == 4259) | (file_df['CellID'] == 4456) | (file_df['CellID'] == 5060)]
        file_df = file_df[file_df['countrycode'] == 39]
        if feature == 'all':
            pass
        if feature == 'Calls':
            file_df = file_df.interpolate('akima', limit_area='inside')
            file_df['Calls'] = file_df['callin'] + file_df['callout']
            file_df = file_df.drop(['smsin', 'smsout', 'callin', 'callout', 'countrycode',
                                    'internet'], axis=1)
        elif feature == 'SMS':
            file_df = file_df.interpolate('akima', limit_area='inside')
            file_df['SMS'] = file_df['smsin'] + file_df['smsout']
            file_df = file_df.drop(['smsin', 'smsout', 'callin', 'callout', 'countrycode',
                                    'internet'], axis=1)
        elif feature == 'internet':
            file_df = file_df.drop(['smsin', 'smsout', 'callin', 'callout', 'countrycode'], axis=1)
        file_df['datetime'] = pd.to_datetime(file_df['datetime'], unit='ms')
        file_df.to_csv(f'data/{feature}/{feature}-mi-{i}.csv', index=False)
    return time_range


def clean_txt_provinces(start, end):
    time_range = pd.period_range(start, end, freq='D').astype('string')
    for i in time_range:
        file_df = pd.read_csv(f'dataverse_files_mi_to_prov/mi-to-provinces-{i}.txt', header=None,
                              sep='\t',
                              names=['CellID', 'Name', 'datetime', 'From Milan', 'To Milan'])
        file_df = file_df[(file_df['CellID'] == 4259) | (file_df['CellID'] == 4456) | (file_df['CellID'] == 5060)]
        file_df['datetime'] = pd.to_datetime(file_df['datetime'], unit='ms')
        file_df = file_df.drop('Name', axis=1)
        file_df = file_df.sort_values(['CellID', 'datetime']).reset_index(drop=True)
        file_df = file_df.fillna(0)
        file_df = file_df.groupby(['CellID', 'datetime']).sum()
        file_df.reset_index(drop=False, inplace=True)
        file_df.to_csv(f'New data/inter_prov_calls/interp/mi-to-provinces-{i}.csv', index=False)
    return time_range


def bi_dir_strength(start, end):
    time_range = pd.period_range(start, end, freq='D').astype('string')
    for date in time_range:
        df = pd.read_csv(f'MItoMI-{date}.txt', sep='\t', skiprows=1, names=['datetime', 'CellID_1',
                                                                            'CellID_2', 'strength'])
        df = df[(df['CellID_1'] == 4259) | (df['CellID_1'] == 4456) | (df['CellID_1'] == 5060)]
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        df.to_csv(f'raw_csv_data/MItoMI-{date}.csv', index=False)


if __name__ == '__main__':
    _ = clean_txt_telecomm('2013-11-01', '2014-01-01', 'internet')
