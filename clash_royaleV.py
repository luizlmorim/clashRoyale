import json
import pandas as pd
import pymongo
import datetime

# Configurar a conexão com o MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/clashRoyale")
db = client['clashRoyale']  
collection = db['batalhas'] 

# Carregar os dados da coleção batalhas do MongoDB
batalhas = list(collection.find({}))

battles_data = []

# Extrair os dados das batalhas
for battle in batalhas:
    # Dados do vencedor
    winner = battle['winner']
    winner_data = {
        'player_tag': winner['tag'],
        'trophies_start': winner.get('startingTrophies', 0),
        'trophy_change': winner.get('trophyChange', 0),
        'is_winner': True,
        'king_tower_hp': winner.get('kingTowerHitPoints', 0),
        'princess_tower_hp': winner.get('princessTowersHitPoints', [0])[0],
        'deck_elixir_avg': winner['elixir'].get('average', 0),
        'troop_count': winner['troop'].get('count', 0),
        'spell_count': winner['spell'].get('count', 0),
        'structure_count': winner['structure'].get('count', 0),
        'total_card_level': winner['totalcard'].get('level', 0),
        'cards': winner['cards'].get('list', []),
        'timestamp': battle['battleTime']  # Adicionando o timestamp
    }

    # Dados do perdedor
    loser = battle['loser']
    loser_data = {
        'player_tag': loser['tag'],
        'trophies_start': loser.get('startingTrophies', 0),
        'trophy_change': loser.get('trophyChange', 0),
        'is_winner': False,
        'king_tower_hp': loser.get('kingTowerHitPoints', 0),
        'princess_tower_hp': loser.get('princessTowersHitPoints', [0])[0],
        'deck_elixir_avg': loser['elixir'].get('average', 0),
        'troop_count': loser['troop'].get('count', 0),
        'spell_count': loser['spell'].get('count', 0),
        'structure_count': loser['structure'].get('count', 0),
        'total_card_level': loser['totalcard'].get('level', 0),
        'cards': loser['cards'].get('list', []),
        'timestamp': battle['battleTime']  # Adicionando o timestamp
    }

    battles_data.append(winner_data)
    battles_data.append(loser_data)

# Criar um DataFrame
df_battles = pd.DataFrame(battles_data)

# Converter a coluna de timestamp para datetime
df_battles['timestamp'] = pd.to_datetime(df_battles['timestamp'], utc=True)

# Definir o intervalo de tempo
start_time = datetime.datetime(2020, 1, 31, 22, 0, 0, tzinfo=datetime.timezone.utc)
end_time = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

# 1. Calcular a porcentagem de vitórias e derrotas utilizando a carta 26000008
carta_x = '26000008'
porcentagem_vitorias, porcentagem_derrotas = calcular_porcentagem_vitorias_derrotas(carta_x, start_time, end_time, df_battles)
print(f'Porcentagem de vitórias utilizando a carta {carta_x}: {porcentagem_vitorias:.2f}%')
print(f'Porcentagem de derrotas utilizando a carta {carta_x}: {porcentagem_derrotas:.2f}%')

# 2. Listar decks completos que produziram mais de 70% de vitórias
percentual_vitoria = 70  # Exemplo: 70%
decks_vencedores = listar_decks_com_vitorias_x_percent(carta_x, percentual_vitoria, start_time, end_time, df_battles)
print("Decks com mais de 70% de vitórias:")
print(decks_vencedores)

# 3. Calcular a quantidade de derrotas utilizando um combo de cartas (parâmetro)
combo_cartas = ['26000008', '27000004']  # Exemplo de combo de cartas
quantidade_derrotas = calcular_derrotas_por_combo(combo_cartas, start_time, end_time, df_battles)
print(f'Quantidade de derrotas com o combo de cartas {combo_cartas}: {quantidade_derrotas}')

# 4. Calcular a quantidade de vitórias onde o vencedor tem 15% menos troféus e o perdedor derrubou ao menos duas torres
z_percent = 15
vitorias_com_condicoes = df_battles[
    (df_battles['is_winner'] == True) &
    (df_battles['trophies_start'] < df_battles['trophies_start'] * (1 - z_percent / 100)) &
    (df_battles['princess_tower_hp'] < 3)  # Assumindo que o número de torres derrubadas é representado por princess_tower_hp
]
quantidade_vitorias = vitorias_com_condicoes.shape[0]
print(f'Quantidade de vitórias com {z_percent}% menos troféus e duas torres derrubadas: {quantidade_vitorias}')

# 5. Listar combos de cartas de tamanho 8 que têm mais de 70% de vitórias
combos_tamanho = 8
winning_combos = listar_combos_de_cartas(combos_tamanho, percentual_vitoria, start_time, end_time, df_battles)
print(f"Combos de cartas de tamanho {combos_tamanho} com mais de {percentual_vitoria}% de vitórias:")
print(winning_combos)
