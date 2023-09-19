import pandas as pd
from tqdm.notebook import tqdm
from pathlib import Path


def process_data(path, sheet_name):
  df = pd.read_excel(path, header=5, sheet_name=sheet_name, skipfooter=1)

  states = pd.concat(
    [df['Unnamed: 0'].drop(0) for _ in range(20)], ignore_index=True
  )
  states.name = 'STATES'

  columns = [
    'JAN', 'FEV', 'MAR', 'ABR',
    'MAI', 'JUN', 'JUL', 'AGO',
    'SET', 'OUT', 'NOV', 'DEZ'
  ]

  data = df.drop('Unnamed: 0', axis=1)
  all_data = []
  for id, year in zip(range(20), range(2004, 2024)):
    if id > 0:
      new_columns = [column + f'.{id}' for column in columns]
    else:
      new_columns = columns
    year_data = data.filter(new_columns).drop(0)
    year_data.columns = columns
    year_data['YEAR'] = [year] * year_data.shape[0]
    all_data.append(year_data)

  all_data = pd.concat(all_data, ignore_index=True)
  all_data = pd.concat([states, all_data], axis=1)
  sheet_name = sheet_name.split()[1].capitalize()
  all_data['CONSUMER_TYPE'] = [sheet_name] * len(all_data)
  return all_data


def concat_save(file_path, names, path_to_save):
  concatenated_data = []
  for name in tqdm(names, desc='Processing and saving tables', leave=False):
    data = process_data(file_path, name)
    concatenated_data.append(data)
  pd.concat(concatenated_data).to_csv(path_to_save, index=False)

if __name__ == '__main__':
  consumptions = [
    'CONSUMIDORES RESIDENCIAIS POR F',
    'CONSUMIDORES INDUSTRIAIS POR UF',
    'CONSUMIDORES COMERCIAIS POR UF',
    'CONSUMIDORES OUTROS POR UF'
  ]
  consumers = [
    'CONSUMO RESIDENCIAL POR UF',
    'CONSUMO INDUSTRIAL POR UF',
    'CONSUMO COMERCIAL POR UF',
    'CONSUMO OUTROS POR UF',
  ]
  assets = [
    (consumptions, 'consumption.csv'),
    (consumers, 'consumers.csv')
  ]

  file_path = 'data/CONSUMO MENSAL DE ENERGIA ELÉTRICA POR CLASSE.xls'
  for values, save_path in tqdm(assets, desc='Files creation'):
    concat_save(file_path, values, save_path)