import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import netCDF4 as nc
import argparse

# URL do diretório HTTP
base_url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/"
output_dir = "chirps_data_p05"
concatenated_file = "chirps_data_concatenated.nc"

# Função para baixar um arquivo com exibição de progresso
def download_file(url, output_dir):
    local_filename = os.path.join(output_dir, url.split("/")[-1])
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        block_size = 1024  # Tamanho do bloco
        t = tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Baixando {local_filename}")
        with open(local_filename, 'wb') as f:
            for data in r.iter_content(block_size):
                t.update(len(data))
                f.write(data)
        t.close()
    return local_filename

# Função para concatenar um arquivo baixado ao arquivo NetCDF concatenado
def concatenate_file_to_netcdf(nc_file, concatenated_file):
    # Abre o arquivo NetCDF baixado
    src = nc.Dataset(nc_file, 'r')
    
    # Se o arquivo de saída ainda não existir, cria-o com as mesmas dimensões e variáveis
    if not os.path.exists(concatenated_file):
        dst = nc.Dataset(concatenated_file, 'w')
        
        # Copia as dimensões
        for dim_name, dim in src.dimensions.items():
            dst.createDimension(dim_name, len(dim) if not dim.isunlimited() else None)
        
        # Copia as variáveis
        for var_name, var in src.variables.items():
            out_var = dst.createVariable(var_name, var.datatype, var.dimensions)
            out_var.setncatts({k: var.getncattr(k) for k in var.ncattrs()})
        
        dst.close()

    # Adiciona os dados do arquivo baixado ao arquivo concatenado
    dst = nc.Dataset(concatenated_file, 'a')
    for var_name, var in src.variables.items():
        dst.variables[var_name][:] = var[:]
    
    dst.close()
    src.close()
    print(f"Arquivo {nc_file} concatenado com sucesso.")

# Função para verificar se o arquivo está no intervalo de anos
def is_in_year_range(filename, start_year, end_year):
    year = int(filename.split('.')[2])
    return start_year <= year <= end_year

# Função para buscar, baixar e concatenar os arquivos dentro do período
def download_and_concatenate_chirps_data(start_year, end_year):
    # Criar o diretório de saída se não existir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Obter a página HTML e fazer parsing
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Para cada arquivo .nc, verificar o intervalo de anos, baixar e concatenar
    for link in soup.find_all('a'):
        href = link.get('href')
        if href.endswith('.nc') and (start_year is None or end_year is None or is_in_year_range(href, start_year, end_year)):
            file_url = base_url + href
            local_file = download_file(file_url, output_dir)
            concatenate_file_to_netcdf(local_file, concatenated_file)

if __name__ == "__main__":
    # Configurar os argumentos de linha de comando
    parser = argparse.ArgumentParser(description="Baixar e concatenar arquivos CHIRPS")
    parser.add_argument('--start-year', type=int, help="Ano de início do período (ex: 1981)")
    parser.add_argument('--end-year', type=int, help="Ano de fim do período (ex: 2020)")
    args = parser.parse_args()

    # Se nenhum ano for especificado, baixar todos os arquivos
    start_year = args.start_year if args.start_year else None
    end_year = args.end_year if args.end_year else None

    # Iniciar o download e a concatenação
    download_and_concatenate_chirps_data(start_year, end_year)
