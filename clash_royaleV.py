import json
import pandas as pd
import pymongo
import datetime

# Configurar a conexão com o MongoDB
client = pymongo.MongoClient("mongodb+srv://luizmiranda29e:qwer4321@clashroyale.rmyvr.mongodb.net/")
db = client['ClashRoyale']  
collection = db['batalhas'] 

# Carregar os dados da coleção batalhas do MongoDB
batalhas = list(collection.find({}))

# Criação das listas para os dados
vencedorDados = []
perdedorDados = []
batalhaDados = []

for batalha in batalhas:
    # Dados do vencedor
    winner = batalha['winner']
    
    dados_vencedor = {
        'tag': winner['tag'],
        'trofeus': winner.get('startingTrophies', 0),
        'torresDerrubadas': winner.get('crowns', 0),
        'is_winner': True,
        'elixir': winner['elixir'].get('average', 0),
        'nivelDeck': winner['totalcard'].get('level', 0),
        'cards': winner['cards'].get('list', []),
        'timestamp': batalha['battleTime']
    }
    
    # Adicionar os dados do vencedor à lista
    vencedorDados.append(dados_vencedor)

    # Dados do perdedor
    loser = batalha['loser']
        
    dados_perdedor = {
        'tag': loser['tag'],
        'trofeus': loser.get('startingTrophies', 0),
        'torresDerrubadas': loser.get('crowns', 0),
        'is_winner': False,
        'elixir': loser['elixir'].get('average', 0),
        'nivelDeck': loser['totalcard'].get('level', 0),
        'cards': loser['cards'].get('list', []),
        'timestamp': batalha['battleTime']
    }
        
    # Adicionar os dados do perdedor à lista
    perdedorDados.append(dados_perdedor)

# Criar um DataFrame com os dados dos vencedores e perdedores
battles_data = vencedorDados + perdedorDados
df_battles = pd.DataFrame(battles_data)

# Converter a coluna de timestamp para datetime
df_battles['timestamp'] = pd.to_datetime(df_battles['timestamp'], utc=True)

# Função 1: Calcular a porcentagem de vitórias e derrotas utilizando a carta X
def calcular_porcentagem_vitorias_derrotas(carta_x, start_time, end_time, battles_df):
    filtered_battles = battles_df[
        (battles_df['timestamp'] >= start_time) &
        (battles_df['timestamp'] <= end_time) &
        (battles_df['cards'].apply(lambda x: carta_x in x))
    ]

    total_battles = filtered_battles.shape[0]
    vitorias = filtered_battles[filtered_battles['is_winner']].shape[0]

    if total_battles > 0:
        porcentagem_vitorias = (vitorias / total_battles) * 100
        porcentagem_derrotas = 100 - porcentagem_vitorias
    else:
        porcentagem_vitorias = 0
        porcentagem_derrotas = 0

    return porcentagem_vitorias, porcentagem_derrotas

# Função 2: Listar decks completos que produziram mais de X% de vitórias
def listar_decks_com_vitorias_x_percent(carta_x, percentual_vitoria, start_time, end_time, battles_df):
    filtered_battles = battles_df[
        (battles_df['timestamp'] >= start_time) &
        (battles_df['timestamp'] <= end_time)
    ]

    # Calcular a porcentagem de vitórias para cada deck
    win_rates = filtered_battles.groupby('tag').agg(
        total_battles=('is_winner', 'count'),
        victories=('is_winner', 'sum')
    )
    win_rates['win_percentage'] = (win_rates['victories'] / win_rates['total_battles']) * 100

    # Filtrar decks que têm mais de X% de vitórias
    winning_decks = win_rates[win_rates['win_percentage'] > percentual_vitoria]

    return winning_decks

# Função 3: Calcular a quantidade de derrotas utilizando um combo de cartas
def calcular_derrotas_por_combo(combo_cartas, start_time, end_time, battles_df):
    filtered_battles = battles_df[
        (battles_df['timestamp'] >= start_time) &
        (battles_df['timestamp'] <= end_time) &
        (battles_df['cards'].apply(lambda x: all(card in x for card in combo_cartas)))
    ]

    # Contar derrotas
    derrotas = filtered_battles[filtered_battles['is_winner'] == False].shape[0]

    return derrotas

# Função 4: Calcular vitórias onde o vencedor tem Z% menos troféus que o perdedor
def calcular_vitorias_com_z_menos_trofeus(carta_x, z_percent, start_time, end_time, battles_df):
    filtered_battles = battles_df[
        (battles_df['timestamp'] >= start_time) &
        (battles_df['timestamp'] <= end_time) &
        (battles_df['cards'].apply(lambda x: carta_x in x))
    ]

    # Calcular troféus
    vitorias = filtered_battles[
        (filtered_battles['is_winner'] == True) &
        ((filtered_battles['trofeus'] - filtered_battles['trofeus']) * (1 - z_percent / 100) > filtered_battles['trofeus'])
    ]

    return vitorias.shape[0]

# Função 5: Listar combos de cartas de tamanho N que têm mais de Y% de vitórias
def listar_combos_de_cartas(combos_tamanho, percentual_vitoria, start_time, end_time, battles_df):
    filtered_battles = battles_df[
        (battles_df['timestamp'] >= start_time) &
        (battles_df['timestamp'] <= end_time)
    ]

    # Obter combos de cartas e calcular porcentagens
    win_rates = filtered_battles.groupby('tag').agg(
        total_battles=('is_winner', 'count'),
        victories=('is_winner', 'sum')
    )
    win_rates['win_percentage'] = (win_rates['victories'] / win_rates['total_battles']) * 100

    # Filtrar decks que têm mais de Y% de vitórias
    winning_decks = win_rates[win_rates['win_percentage'] > percentual_vitoria]
    
    # Filtrar decks com combos de tamanho N
    valid_combos = filtered_battles[filtered_battles['cards'].apply(lambda x: len(x) == combos_tamanho)]

    return valid_combos

# Perguntas

# 1. Calcule a porcentagem de vitórias e derrotas utilizando a carta 26000008 ocorridas entre 00:00 às 12:00 no dia 01/01/2021.
start_time_1 = pd.Timestamp("2021-01-01 00:00:00", tz='UTC')
end_time_1 = pd.Timestamp("2021-01-01 12:00:00", tz='UTC')
porcentagem_vitorias, porcentagem_derrotas = calcular_porcentagem_vitorias_derrotas(26000008, start_time_1, end_time_1, df_battles)
print(f"Porcentagem de vitórias: {porcentagem_vitorias:.2f}%")
print(f"Porcentagem de derrotas: {porcentagem_derrotas:.2f}%")

# 2. Liste os decks completos que produziram mais de 70% de vitórias ocorridas entre 00:00 às 12:00 no dia 01/01/2021.
start_time_2 = pd.Timestamp("2021-01-01 00:00:00", tz='UTC')
end_time_2 = pd.Timestamp("2021-01-01 12:00:00", tz='UTC')
decks_vitoria = listar_decks_com_vitorias_x_percent(26000008, 70, start_time_2, end_time_2, df_battles)
print(decks_vitoria)

# 3. Calcule a quantidade de derrotas utilizando um combo de cartas.
combo_cartas = [26000003, 26000004, 26000006, 26000008, 26000022, 27000004, 28000001, 28000002]
start_time_3 = pd.Timestamp("2021-01-01 00:00:00", tz='UTC')
end_time_3 = pd.Timestamp("2021-01-01 12:00:00", tz='UTC')
quantidade_derrotas = calcular_derrotas_por_combo(combo_cartas, start_time_3, end_time_3, df_battles)
print(f"Quantidade de derrotas: {quantidade_derrotas}")

# 4. Calcule vitórias onde o vencedor tem Z% menos troféus que o perdedor
z_percent = 10  # Exemplo de porcentagem Z
vitorias_z = calcular_vitorias_com_z_menos_trofeus(26000008, z_percent, start_time_2, end_time_2, df_battles)
print(f"Vitórias com Z% menos troféus: {vitorias_z}")

# 5. Liste combos de cartas de tamanho N que têm mais de Y% de vitórias.
combos_tamanho = 6  # Exemplo de tamanho N
percentual_vitoria_y = 75  # Exemplo de percentual Y
combos_vitoriosos = listar_combos_de_cartas(combos_tamanho, percentual_vitoria_y, start_time_2, end_time_2, df_battles)
print(combos_vitoriosos)
