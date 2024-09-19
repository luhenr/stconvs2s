import xarray as xr
import numpy as np
import glob
import argparse
import os

# Função para processar os arquivos e realizar as operações solicitadas
def process_nc_files(lat_min, lat_max, lon_min, lon_max, start_year, end_year):
    # Passo 1: Listar todos os arquivos .nc no diretório 'chirps_data_p05'
    nc_files = glob.glob(os.path.join('chirps_data_p05', '*.nc'))
    
    # Se foi passado um intervalo de anos, filtrar os arquivos
    if start_year and end_year:
        nc_files = [f for f in nc_files if start_year <= int(os.path.basename(f).split('.')[2]) <= end_year]

    if len(nc_files) == 0:
        print("Nenhum arquivo .nc encontrado no diretório ou no intervalo de anos especificado.")
        return

    # Passo 2: Abrir e concatenar todos os arquivos ao longo da dimensão do tempo
    ds = xr.open_mfdataset(nc_files, combine='by_coords', engine='netcdf4')
    ds = ds.sortby('time')

    # Passo 3: Renomear coordenadas para corresponder ao arquivo exemplo
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    # Passo 4: Selecionar o domínio espacial de interesse, se fornecido
    if lat_min and lat_max and lon_min and lon_max:
        ds = ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))

    # Definir as coordenadas alvo para lat e lon (50 pontos)
    lat_target = np.linspace(lat_min, lat_max, 50) if lat_min and lat_max else ds['lat']
    lon_target = np.linspace(lon_min, lon_max, 50) if lon_min and lon_max else ds['lon']

    # Interpolar o dataset para as coordenadas alvo
    ds_interp = ds.interp(lat=lat_target, lon=lon_target)

    # Passo 5: Definir o comprimento da sequência e o passo
    sequence_length = 5  # Comprimento da sequência
    step_size = 5        # Passo entre as sequências

    # Passo 6: Criar sequências de dados para 'x' (input)
    x_sequences = ds_interp['precip'].rolling(time=sequence_length, center=False).construct('time_window')
    x_sequences = x_sequences.isel(time=slice(0, None, step_size))
    x_sequences = x_sequences.dropna(dim='time', how='any')

    # Passo 7: Criar sequências de dados para 'y' (output)
    y_sequences = ds_interp['precip'].rolling(time=sequence_length, center=False).construct('time_window')
    y_sequences = y_sequences.isel(time=slice(sequence_length, None, step_size))
    y_sequences = y_sequences.dropna(dim='time', how='any')

    # Passo 8: Garantir que 'x' e 'y' tenham o mesmo número de amostras
    min_samples = min(x_sequences.sizes['time'], y_sequences.sizes['time'])
    x_sequences = x_sequences.isel(time=slice(0, min_samples))
    y_sequences = y_sequences.isel(time=slice(0, min_samples))

    # Passo 9: Ajustar dimensões e adicionar dimensão 'channel'
    x_sequences = x_sequences.rename({'time': 'sample', 'time_window': 'time'})
    y_sequences = y_sequences.rename({'time': 'sample', 'time_window': 'time'})

    x_sequences = x_sequences.expand_dims('channel', axis=-1)
    y_sequences = y_sequences.expand_dims('channel', axis=-1)

    x_sequences = x_sequences.transpose('sample', 'time', 'lat', 'lon', 'channel')
    y_sequences = y_sequences.transpose('sample', 'time', 'lat', 'lon', 'channel')

    # Passo 10: Criar um novo Dataset com as variáveis 'x' e 'y'
    ds_out = xr.Dataset({
        'x': x_sequences,
        'y': y_sequences
    })

    # Passo 11: Adicionar atributos e coordenadas necessários
    ds_out.attrs['description'] = 'Dados processados para corresponder ao formato do arquivo exemplo.'
    ds_out = ds_out.assign_coords({
        'lat': ds_interp['lat'],
        'lon': ds_interp['lon']
    })

    # Passo 12: Criar o diretório 'data' se não existir e salvar o Dataset em um arquivo .nc
    #os.makedirs('data', exist_ok=True)  # Garante que o diretório 'data' existe
    output_filename = os.path.join('data', f'dataset-chirps-{start_year}-{end_year or "2024"}-seq5-ystep5.nc')
    ds_out.to_netcdf(output_filename)



    # Passo 13: Verificar as dimensões resultantes
    print(ds_out)

    # Passo 14: Verificar as formas das variáveis 'x' e 'y'
    print(f"[x] shape: {ds_out['x'].shape}")
    print(f"[y] shape: {ds_out['y'].shape}")

# Função principal
if __name__ == "__main__":
    # Argumentos de linha de comando
    parser = argparse.ArgumentParser(description="Processar arquivos CHIRPS NetCDF")
    parser.add_argument('--lat-min', type=float, help="Latitude mínima")
    parser.add_argument('--lat-max', type=float, help="Latitude máxima")
    parser.add_argument('--lon-min', type=float, help="Longitude mínima")
    parser.add_argument('--lon-max', type=float, help="Longitude máxima")
    parser.add_argument('--start-year', type=int, help="Ano de início do período (ex: 1981)")
    parser.add_argument('--end-year', type=int, help="Ano de fim do período (ex: 2020)")
    
    args = parser.parse_args()

    # Executar processamento
    process_nc_files(args.lat_min, args.lat_max, args.lon_min, args.lon_max, args.start_year, args.end_year)
