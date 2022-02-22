#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Esse script é responsável por juntar todos os
    arquivos .csv de uma estação do INMET para um
    único arquivo .csv. Também é realizado a formatação
    dos dados. A base de dados pode ser encontra aqui:
    https://portal.inmet.gov.br/dadoshistoricos

    author: Marcelo Bittencourt
'''

import pandas as pd
import os


class DataJoinFormat:

    def __init__(self):
        pass

    '''
        Formatação dos dados
        
        df: dataframe
    '''
    def data_format(self, df):

        # Exluindo dados NaN e substituindo por ZERO
        df.fillna(value=0, inplace=True)

        # Novo dataframe
        df2 = pd.DataFrame(columns=['pressao_atm_(mB)', 'radiacao_global_(Kj/m2)',
                                    'temp_ar_bulbo_seco_(C)', 'temp_ponto_de_orvalho_(C)',
                                    'temp_max_(C)', 'temp_min_(C)', 'umidade_rlv_ar_(%)',
                                    'vento_direcao_horaria_(gr)','vento_velocidade_horaria_(m/s)','precipitacao_total_horario_(mm)'],index=df.index)

        df2['pressao_atm_(mB)'] = df['PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)']
        df2['radiacao_global_(Kj/m2)'] = df['RADIACAO GLOBAL (Kj/m²)']
        df2['temp_ar_bulbo_seco_(C)'] = df['TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)']
        df2['temp_ponto_de_orvalho_(C)'] = df['TEMPERATURA DO PONTO DE ORVALHO (°C)']
        df2['temp_max_(C)'] = df['TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)']
        df2['temp_min_(C)'] = df['TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)']
        df2['umidade_rlv_ar_(%)'] = df['UMIDADE RELATIVA DO AR, HORARIA (%)']
        df2['vento_direcao_horaria_(gr)'] = df['VENTO, DIREÇÃO HORARIA (gr) (° (gr))']
        df2['vento_velocidade_horaria_(m/s)'] = df['VENTO, VELOCIDADE HORARIA (m/s)']
        df2['precipitacao_total_horario_(mm)'] = df['PRECIPITAÇÃO TOTAL, HORÁRIO (mm)']

        # Exluindo dados NaN e substituindo por ZERO
        df2.fillna(value=0, inplace=True)

        # Convertendo colunas float em str
        for c in df2:
            df2[c] = df2[c].astype(str)

        # Trocando virgula por ponto em todas as colunas
        for c in df2:
            df2[c] = df2[c].str.replace(',', '.')

        # Convertando todas as colunas para float
        df2 = df2.apply(pd.to_numeric)

        # Reamostrando
        df2 = df2.resample('H').mean().round(2)

        # Ordenando pela data
        df2 = df2.sort_values(by=['Data'])

        # Removendo dados incorretos
        for c in df2:
            index_names = df2[df2[c] < 0].index
            df2.drop(index_names, inplace=True)

        # Removendo linhas que estao sem leitura (todas variaveis zero)
        df2['soma'] = df2.sum(axis=1)
        df2 = df2[df2.soma != 0]
        df2 = df2.drop(['soma'], axis=1)

        return df2

    '''
        Concatenação de todos os arquivos .csv
        de uma estação para apenas um único arquivo.
        
        p_source: diretório de origem dos arquivos
        p_dest: diretório final dos arquivos processados
    '''
    def data_join(self, p_source, p_dest):
        index_list = []
        df_list = []

        # Nesse diretorio deve conter os arquivos .csv baixados base de dados
        dir_path = p_source
        files_list = os.listdir(dir_path)
        # Selecionando arquivos da mesma estacao de anos diferentes
        for f in files_list:
            df_list_aux = []
            name_file = f.split('_')
            code = name_file[3]

            if code in index_list:
                pass
            else:
                print('>> processando:',code)
                index_list.append(code)
                for f2 in files_list:
                    if f2.find(code) != -1:  # Verifica se e da mesma estacao

                        df = pd.read_csv(dir_path + f2, sep=";", encoding="ISO-8859-1", engine='python', skiprows=8)

                        if df.columns[0] != 'Data': # Antes de 2019
                            df.rename(columns={df.columns[0]: 'Data'},inplace=True)
                            df['Data'] = df['Data'] + ' ' + df[df.columns[1]] + ':00'
                        else:
                            df[df.columns[1]] = df[df.columns[1]].replace(" UTC", "", regex=True)
                            df[df.columns[1]] = df[df.columns[1]].str.slice_replace(2, 2, ':')
                            df['Data'] = df['Data'] + ' ' + df[df.columns[1]] + ':00'
                            df['Data'] = df['Data'].replace("/", "-", regex=True)

                        df['Data'] = pd.to_datetime(df['Data'])
                        df.set_index('Data', inplace=True)
                        df_list_aux.append(df)

                df_list.append(df_list_aux)

        # Concatenando arquivos
        for i in range(0, len(df_list)):
            df_result = pd.concat(df_list[i])
            df_result = self.data_format(df_result)
            df_result.to_csv(p_dest + index_list[i] + '.csv')

        return True

djf = DataJoinFormat()
print("> Processamento em andamento")
path_source = "/home/marcelo/Documents/tcc/dados-analise/regiao-sul/sc/"
path_dest = "/home/marcelo/Documents/git/marcelo-preprocessing/data-preprocessing/dados-prepocessados/regiao-sul/sc/"
djf.data_join(path_source, path_dest)
print("> Processamento concluído")

