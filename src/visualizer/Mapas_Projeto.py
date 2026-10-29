import folium
import pandas as pd
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calcula a distância em metros entre dois pontos GPS
    """
    # Converter graus para radianos
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Fórmula de haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Raio da Terra em metros
    return c * r

# 1. Carregar o ficheiro stops_smtuc.txt
# Certifica-te que o ficheiro 'stops_smtuc.txt' está na mesma pasta que este script
try:
    df = pd.read_csv("data/stops_smtuc.txt")
except FileNotFoundError:
    print("❌ Erro: Não encontro o ficheiro 'stops_smtuc.txt'. Verifica o nome e a pasta.")
    exit()

# 2. Identificar paragens com outras paragens próximas mas com nomes diferentes
radius = 150  # metros
paragens_com_vizinhas_diferentes = set()

print(f"🔍 A procurar paragens com vizinhas de nomes diferentes num raio de {radius}m...")

for i, row_i in df.iterrows():
    for j, row_j in df.iterrows():
        if i != j:  # Não comparar uma paragem consigo mesma
            # Calcular distância
            dist = haversine(row_i['stop_lon'], row_i['stop_lat'], 
                           row_j['stop_lon'], row_j['stop_lat'])
            
            # Se estiverem a menos de 'radius' metros E tiverem nomes diferentes
            if dist <= radius and row_i['stop_name'] != row_j['stop_name']:
                paragens_com_vizinhas_diferentes.add(i)

print(f"✅ Encontradas {len(paragens_com_vizinhas_diferentes)} paragens com vizinhas de nomes diferentes")

# 3. Criar o Mapa Base centrado em Coimbra
# Coordenadas aproximadas do centro de Coimbra
mapa = folium.Map(location=[40.2033, -8.4103], zoom_start=13, tiles="OpenStreetMap")

# 4. Adicionar as Paragens ao Mapa
print(f"📍 A adicionar {len(df)} paragens ao mapa...")

for index, row in df.iterrows():
    # Verificar se esta paragem tem vizinhas com nomes diferentes
    tem_vizinhas_diferentes = index in paragens_com_vizinhas_diferentes
    
    # Criar o texto que aparece quando clicas na paragem (Popup)
    info_popup = f"""
    <b>ID:</b> {row['stop_id']}<br>
    <b>Nome:</b> {row['stop_name']}<br>
    <b>Desc:</b> {row['stop_desc']}<br>
    <b>Tem vizinhas diferentes:</b> {'✅ Sim' if tem_vizinhas_diferentes else '❌ Não'}
    """
    
    # Escolher a cor do marcador
    cor = "red" if tem_vizinhas_diferentes else "blue"
    
    # Adicionar o marcador
    folium.Marker(
        location=[row['stop_lat'], row['stop_lon']],
        popup=folium.Popup(info_popup, max_width=300),
        tooltip=row['stop_name'], # Texto que aparece só de passar o rato
        icon=folium.Icon(color=cor, icon="bus", prefix="fa")
    ).add_to(mapa)

# 5. Adicionar Círculos de Raio (Opcional - Ajuda a ver a proximidade)
# Desenha um círculo de 150m à volta das paragens com vizinhas diferentes
for index, row in df.iterrows():
    if index in paragens_com_vizinhas_diferentes:
        folium.Circle(
            location=[row['stop_lat'], row['stop_lon']],
            radius=radius,
            color="red",
            weight=2,
            fill=True,
            fill_opacity=0.15
        ).add_to(mapa)

# 6. Adicionar legenda ao mapa
legenda_html = '''
<div style="position: fixed; 
            top: 10px; right: 10px; width: 250px; height: 120px; 
            background-color: white; border:2px solid grey; z-index:9999; 
            font-size:14px; padding: 10px">
<p><b>Legenda:</b></p>
<p><i class="fa fa-map-marker fa-2x" style="color:red"></i> Tem vizinhas com nomes diferentes</p>
<p><i class="fa fa-map-marker fa-2x" style="color:blue"></i> Sem vizinhas de nomes diferentes</p>
</div>
'''
mapa.get_root().html.add_child(folium.Element(legenda_html))

# 7. Guardar o ficheiro
output_file = "mapa_smtuc.html"
mapa.save(output_file)

print(f"✅ Mapa criado com sucesso: {output_file}")
print("Abre este ficheiro no teu browser para veres as paragens!")